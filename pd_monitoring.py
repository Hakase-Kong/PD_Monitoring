import os
import re
import json
import time
import hashlib
import logging
import requests
import datetime as dt
from urllib.parse import urlparse, parse_qs
from typing import Optional, List, Dict, Set

import pytz
import streamlit as st
from apscheduler.schedulers.background import BackgroundScheduler
from threading import Lock

# -------------------------
# 기본 설정 / 전역 상태
# -------------------------
APP_TZ = pytz.timezone("Asia/Seoul")
DEFAULT_CONFIG_PATH = os.getenv("PD_CFG", os.getenv("PE_CFG", "config.json"))
CACHE_FILE = os.getenv("PD_SENT_CACHE", os.getenv("PE_SENT_CACHE", "sent_cache.json"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pd_monitor")

CURRENT_CFG_PATH = DEFAULT_CONFIG_PATH
CURRENT_CFG_DICT: Dict = {}
CURRENT_ENV = {
    "NEWSAPI_KEY": os.getenv("NEWSAPI_KEY", ""),
    "NAVER_CLIENT_ID": os.getenv("NAVER_CLIENT_ID", ""),
    "NAVER_CLIENT_SECRET": os.getenv("NAVER_CLIENT_SECRET", ""),
    "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID", ""),
    "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", ""),
}

# -------------------------
# 공용 유틸
# -------------------------
def load_config(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("config 로드 실패(%s): %s", path, e)
        return {}

def now_kst() -> dt.datetime:
    return dt.datetime.now(APP_TZ)

def clamp(n, lo, hi):
    return max(lo, min(hi, n))

def domain_of(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").replace("www.", "")
    except Exception:
        return ""

def _naver_sid(url: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(url).query).get("sid", [])
        return q[0] if q else None
    except Exception:
        return None

def sha1(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

def is_weekend(kst: dt.datetime) -> bool:
    return kst.weekday() >= 5

def is_holiday(kst: dt.datetime, holidays: List[str]) -> bool:
    ymd = kst.strftime("%Y-%m-%d")
    return ymd in set(holidays or [])

def between_working_hours(kst: dt.datetime, start=6, end=20) -> bool:
    return start <= kst.hour < end

# -------------------------
# Scheduler Helpers
# -------------------------
def _map_days_for_cron(days_ui: list[str]) -> str:
    if not days_ui or "매일" in days_ui:
        return "*"
    m = {"월": "mon", "화": "tue", "수": "wed", "목": "thu", "금": "fri", "토": "sat", "일": "sun"}
    mapped = [m[d] for d in days_ui if d in m]
    return ",".join(mapped) if mapped else "*"

def ensure_cron_job(sched: BackgroundScheduler, cfg: dict):
    job_id = "pd_news_job"
    try:
        sched.remove_job(job_id)
    except Exception:
        pass

    day_of_week = _map_days_for_cron(cfg.get("CRON_DAYS_UI", ["매일"]))
    hour = int(cfg.get("CRON_HOUR", 9))
    minute = int(cfg.get("CRON_MINUTE", 0))
    sched.add_job(
        scheduled_job,
        "cron",
        id=job_id,
        replace_existing=True,
        day_of_week=day_of_week,
        hour=hour,
        minute=minute,
    )

def ensure_interval_job(sched: BackgroundScheduler, minutes: int):
    job_id = "pd_news_job"
    try:
        sched.remove_job(job_id)
    except Exception:
        pass

    sched.add_job(
        scheduled_job,
        "interval",
        id=job_id,
        replace_existing=True,
        minutes=int(minutes),
        next_run_time=now_kst(),
    )

def ensure_scheduled_job(sched: BackgroundScheduler, cfg: dict):
    mode = cfg.get("SCHEDULE_MODE", "주기(분)")
    if mode == "주기(분)":
        ensure_interval_job(sched, int(cfg.get("INTERVAL_MIN", 60)))
    else:
        ensure_cron_job(sched, cfg)

# -------------------------
# 전송 캐시 (v1 호환)
# -------------------------
def load_sent_cache() -> Set[str]:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            arr = json.load(f)
            return set(arr if isinstance(arr, list) else [])
    except Exception:
        return set()

def save_sent_cache(hashes: Set[str]) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(hashes)), f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("전송 캐시 저장 실패: %s", e)

# -------------------------
# Enhanced Cache (v2)
# -------------------------
def story_key(item: dict, cfg: dict | None = None) -> str:
    url = item.get("url", "")
    cid = canonical_url_id(url)
    if cid.startswith("naver:"):
        return cid
    norm_t = normalize_title(item.get("title", ""), cfg)
    return f"title:{sha1(norm_t)}"

def _utcnow_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_iso(s: str) -> dt.datetime:
    try:
        return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except Exception:
        return dt.datetime.now(dt.timezone.utc)

def load_sent_cache_v2(retention_hours: int = 72) -> tuple[dict, dict]:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        raw = {}

    now = dt.datetime.now(dt.timezone.utc)
    limit = now - dt.timedelta(hours=max(6, retention_hours))

    url_map = {}
    story_map = {}

    if isinstance(raw, list):
        for h in raw:
            url_map[h] = _utcnow_iso()
    elif isinstance(raw, dict):
        url_map = dict(raw.get("url", {}))
        story_map = dict(raw.get("story", {}))

    def _prune(d: dict) -> dict:
        out = {}
        for k, ts in d.items():
            try:
                ts_dt = _parse_iso(ts)
            except Exception:
                continue
            if ts_dt >= limit:
                out[k] = ts
        return out

    return _prune(url_map), _prune(story_map)

def save_sent_cache_v2(url_map: dict, story_map: dict) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"url": url_map, "story": story_map}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("전송 캐시 저장 실패(v2): %s", e)

# -------------------------
# 외부 API (Naver / NewsAPI)
# -------------------------
def search_naver_news(
    keyword: str,
    client_id: str,
    client_secret: str,
    recency_hours=72,
    page_size: int = 30,
) -> List[dict]:
    if not client_id or not client_secret or not keyword:
        return []

    base = "https://openapi.naver.com/v1/search/news.json"
    params = {"query": keyword, "display": clamp(int(page_size), 10, 100), "sort": "date"}
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }

    try:
        r = requests.get(base, params=params, headers=headers, timeout=12)
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        res = []
        cutoff = now_kst() - dt.timedelta(hours=recency_hours)

        for it in items:
            link = it.get("link") or it.get("originallink") or ""
            if not link:
                continue

            pubdate = it.get("pubDate")
            try:
                pub_kst = dt.datetime.strptime(pubdate, "%a, %d %b %Y %H:%M:%S %z")
            except Exception:
                pub_kst = now_kst()

            if pub_kst < cutoff:
                continue

            title = re.sub("<.*?>", "", it.get("title") or "")
            description = re.sub("<.*?>", "", it.get("description") or "")

            res.append(
                {
                    "title": title.strip(),
                    "description": description.strip(),
                    "summary": description.strip(),
                    "content": "",
                    "url": link.strip(),
                    "source": domain_of(link),
                    "publishedAt": pub_kst.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "origin_keyword": keyword,
                    "provider": "naver",
                }
            )
        return res
    except Exception as e:
        log.warning("Naver 오류(%s): %s", keyword, e)
        return []

def search_newsapi(query: str, page_size: int, api_key: str, from_hours: int = 72, cfg: dict = None) -> List[dict]:
    if not api_key or not query:
        return []

    base = "https://newsapi.org/v2/everything"
    from_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=from_hours)
    params = {
        "q": (cfg.get("NEWSAPI_QUERY") if (cfg and cfg.get("NEWSAPI_QUERY")) else query),
        "searchIn": "title,description",
        "pageSize": clamp(int(page_size), 10, 100),
        "language": "ko",
        "sortBy": "publishedAt",
        "from": from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "apiKey": api_key,
    }
    if cfg and cfg.get("NEWSAPI_DOMAINS"):
        params["domains"] = ",".join(cfg["NEWSAPI_DOMAINS"])

    try:
        r = requests.get(base, params=params, timeout=12)
        r.raise_for_status()
        data = r.json()
        arts = data.get("articles", [])
        res = []
        for a in arts:
            title = (a.get("title") or "").strip()
            url = (a.get("url") or "").strip()
            description = (a.get("description") or "").strip()
            if not title or not url:
                continue
            res.append(
                {
                    "title": title,
                    "description": description,
                    "summary": description,
                    "content": "",
                    "url": url,
                    "source": domain_of(url) or (a.get("source", {}) or {}).get("name", ""),
                    "publishedAt": (a.get("publishedAt") or "").replace(".000Z", "Z"),
                    "origin_keyword": "_newsapi",
                    "provider": "newsapi",
                }
            )
        return res
    except Exception as e:
        log.warning("NewsAPI 오류: %s", e)
        return []

# -------------------------
# 중복 제거 강화 (URL/제목 정규화 + 근사 중복)
# -------------------------
NAVER_ART_RE = re.compile(r"/article/(\d{3})/(\d{10})")
NOISE_TAGS = {"단독", "속보", "시그널", "fn마켓워치", "투자360", "영상", "포토", "르포", "사설", "칼럼", "분석"}
BRACKET_RE = re.compile(r"[\[\(（](.*?)[\]\)）]")
MULTISPACE_RE = re.compile(r"\s+")

_FALLBACK_SYNONYM_MAP = {
    "private credit": "private debt",
    "private debt": "private debt",
    "사모크레딧": "private debt",
    "사모대출": "직접대출",
    "direct lending": "직접대출",
    "direct lender": "직접대출",
    "mergers & acquisitions": "m&a",
    "merger": "m&a",
    "acquisition": "인수",
    "tender offer": "공개매수",
    "takeover": "인수",
    "sell-down": "지분매각",
    "spin-off": "스핀오프",
    "carve-out": "카브아웃",
    "special situations": "special sits",
    "distressed debt": "부실채권",
    "refinancing": "리파이낸싱",
}

def canonical_url_id(url: str) -> str:
    try:
        u = urlparse(url)
        host = (u.netloc or "").replace("www.", "")
        path = u.path or ""
        if host.endswith("naver.com"):
            m = NAVER_ART_RE.search(path)
            if m:
                oid, aid = m.group(1), m.group(2)
                return f"naver:{oid}:{aid}"
        base = f"{host}{path}".rstrip("/")
        return re.sub(r"/+$", "", base)
    except Exception:
        return url

def normalize_title(t: str, cfg: dict | None = None) -> str:
    if not t:
        return ""

    s = t

    def _strip_noise(m):
        inner = (m.group(1) or "").strip()
        return "" if any(tag in inner.replace(" ", "") for tag in NOISE_TAGS) else inner

    s = BRACKET_RE.sub(_strip_noise, s)

    for tag in NOISE_TAGS:
        s = re.sub(rf"^\s*(?:\[{tag}\]|\({tag}\))\s*", "", s, flags=re.IGNORECASE)

    s = s.replace("…", " ").replace("ㆍ", " ").replace("·", " ").replace("—", " ")

    s_low = s.lower()
    try:
        synonyms = (cfg or {}).get("SYNONYM_MAP") if isinstance(cfg, dict) else None
    except Exception:
        synonyms = None

    if not isinstance(synonyms, dict) or not synonyms:
        synonyms = _FALLBACK_SYNONYM_MAP

    for k, v in (synonyms or {}).items():
        try:
            s_low = s_low.replace(k.lower(), v.lower())
        except Exception:
            pass

    s_low = re.sub(r"\b(\d{1,3}(,\d{3})+|\d+)\b", lambda m: m.group(0).replace(",", ""), s_low)
    s_low = MULTISPACE_RE.sub(" ", s_low).strip()
    return s_low

def _tokens(s: str) -> set:
    return {w for w in re.split(r"[^0-9a-zA-Z가-힣]+", s) if len(w) >= 2}

def _bigrams(s: str) -> set:
    return {s[i:i+2] for i in range(len(s)-1)} if len(s) > 1 else set()

def _sim_norm_title(a: str, b: str) -> float:
    ta, tb = _tokens(a), _tokens(b)
    ja = len(ta & tb) / max(1, len(ta | tb)) if ta and tb else 0.0
    ba, bb = _bigrams(a), _bigrams(b)
    jb = len(ba & bb) / max(1, len(ba | bb)) if ba and bb else 0.0
    return 0.6 * ja + 0.4 * jb

def dedup(items: List[dict], cfg: dict | None = None) -> List[dict]:
    def _ts_kst(it):
        try:
            return dt.datetime.strptime(it["publishedAt"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=dt.timezone.utc
            ).astimezone(APP_TZ)
        except Exception:
            return now_kst()

    cfg = cfg or {}
    xs_th = float(cfg.get("TITLE_SIM_XSRC", 0.58))
    ss_th = float(cfg.get("TITLE_SIM_SAMESRC", 0.58))
    same_src_hours = int(cfg.get("SAME_SOURCE_WINDOW_HOURS", 24))

    work = sorted(items, key=lambda x: x.get("_score", 0.0), reverse=True)
    out, seen = [], []

    for it in work:
        t_norm = normalize_title(it.get("title", ""), cfg)
        src = domain_of(it.get("url", ""))
        ts = _ts_kst(it)
        is_dup = False

        for s in seen:
            sim = _sim_norm_title(t_norm, s["t_norm"])

            if s["src"] == src and abs((ts - s["ts"]).total_seconds()) <= same_src_hours * 3600:
                if sim >= ss_th:
                    is_dup = True
                    break

            if s["src"] != src and sim >= xs_th:
                is_dup = True
                break

        if not is_dup:
            out.append(it)
            seen.append({"t_norm": t_norm, "src": src, "ts": ts})

    return out

# -------------------------
# 규칙 기반 필터/정렬
# -------------------------
def should_drop(item: dict, cfg: dict) -> bool:
    url = item.get("url", "")
    title = (item.get("title") or "").strip()
    description = (item.get("description") or "").strip()

    if not url or not title:
        return True

    src = domain_of(url)
    allow = set(cfg.get("ALLOW_DOMAINS", []) or [])
    block = set(cfg.get("BLOCK_DOMAINS", []) or [])
    allow_strict = bool(cfg.get("ALLOWLIST_STRICT", False))

    if src in block:
        return True
    if allow_strict and allow and (src not in allow):
        return True

    if "naver.com" in src:
        sids = set(cfg.get("NAVER_ALLOW_SIDS", []) or [])
        if sids:
            sid = _naver_sid(url)
            if sid and sid not in sids:
                return True

    include = cfg.get("INCLUDE_TITLE_KEYWORDS", []) or []
    full_text_for_include = f"{title} {description}".lower()
    if include and not any(w.lower() in full_text_for_include for w in include):
        return True

    for w in (cfg.get("EXCLUDE_TITLE_KEYWORDS", []) or []):
        if w and w.lower() in full_text_for_include:
            return True

    for pat in (cfg.get("EXCLUDE_TITLE_REGEX", []) or []):
        try:
            if re.search(pat, f"{title} {description}", flags=re.IGNORECASE):
                return True
        except re.error:
            log.warning("잘못된 EXCLUDE_TITLE_REGEX 패턴 무시: %s", pat)

    context_any = cfg.get("CONTEXT_REQUIRE_ANY", []) or []
    context = f"{title} {description} {item.get('summary', '')} {item.get('content', '')}".lower()
    has_context = any(k.lower() in context for k in context_any)

    trusted = set(cfg.get("TRUSTED_SOURCES_FOR_FI", cfg.get("ALLOW_DOMAINS", [])) or [])
    amb_tokens = set(t.lower() for t in (cfg.get("STRICT_AMBIGUOUS_TOKENS", []) or []))
    has_ambiguous = any(tok in context for tok in amb_tokens)

    if not has_context:
        if not (src in trusted and has_ambiguous):
            return True

    return False

def score_item(item: dict, cfg: dict) -> float:
    src = domain_of(item.get("url", ""))
    score = float((cfg.get("DOMAIN_WEIGHTS", {}) or {}).get(src, 1.0))
    try:
        ts = item.get("publishedAt")
        pub = dt.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except Exception:
        pub = now_kst()

    hours_ago = (now_kst() - pub.astimezone(APP_TZ)).total_seconds() / 3600.0
    score += max(0.0, 6.0 - (hours_ago / 8.0))
    return score

def rank_filtered(items: List[dict], cfg: dict) -> List[dict]:
    arr = [it for it in items if not should_drop(it, cfg)]
    for it in arr:
        it["_score"] = score_item(it, cfg)
    arr.sort(key=lambda x: x["_score"], reverse=True)
    return dedup(arr, cfg)

# -------------------------
# LLM 기반 2차 필터
# -------------------------
def _flatten_aliases(cfg: dict) -> List[str]:
    out = []
    for k, v in (cfg.get("KEYWORD_ALIASES", {}) or {}).items():
        out.append(k)
        out.extend(v or [])
    return sorted(set(out))

def _llm_prompt_for_item(item: dict, cfg: dict) -> str:
    kw = cfg.get("KEYWORDS", []) or []
    aliases = _flatten_aliases(cfg)
    firms = cfg.get("FIRM_WATCHLIST", []) or []
    context_any = cfg.get("CONTEXT_REQUIRE_ANY", []) or []

    return f"""
당신은 '프라이빗데트(PD) / 프라이빗크레딧(Private Credit)' 관련 기사를 분류하는 전문가입니다.
다음 기사가 private debt / private credit / direct lending / acquisition financing /
mezzanine / NAV financing / special situations / distressed debt / NPL / refinancing /
credit fund raising 과 관련이 있는지 판단하세요.

포함 예시:
- 사모대출, 직접대출, 인수금융, 리파이낸싱, 브릿지론
- 선순위/후순위/유닛랜치(unitranche), 메자닌, 구조화 크레딧
- special situations, distressed debt, stressed debt, NPL 투자
- private credit fund 결성/증액/클로징
- 크레딧 운용사가 참여한 대출/차환/구조조정 딜

제외 예시:
- 일반 은행 여신 기사
- 단순 회사채 발행 기사
- 단순 금리/거시 기사
- 순수 PE 지분 인수 기사(M&A 자체만 있고 대출/크레딧 구조가 없는 경우)
- 일반 주식시장 기사

판단 기준:
- 핵심 키워드: {', '.join(kw)}
- 동의어: {', '.join(aliases)}
- 운용사/크레딧 플랫폼 워치리스트: {', '.join(firms)}
- 맥락 키워드: {', '.join(context_any)}

출력은 반드시 JSON 한 줄로 반환:
{{
  "relevant": true|false,
  "confidence": 0.0~1.0,
  "category": "private debt"|"credit fund"|"distressed/special sits"|"general finance"|"irrelevant",
  "matched": ["매칭된 단어들"],
  "reason": "한 줄 근거"
}}

기사:
- 제목: {item.get('title', '')}
- 요약: {item.get('description', '')}
- 출처: {domain_of(item.get('url', ''))}
- 링크: {item.get('url', '')}
"""

def _openai_chat(messages: List[Dict], api_key: str, model: str, max_tokens: int = 300, temperature: float = 0.0) -> str:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=25)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]

def llm_filter_items(items: List[dict], cfg: dict, env: dict) -> List[dict]:
    if not items:
        return items
    if not bool(cfg.get("USE_LLM_FILTER", False)):
        return items

    api_key = env.get("OPENAI_API_KEY", "")
    if not api_key:
        log.warning("LLM 필터 활성화되어 있으나 OPENAI_API_KEY 미설정 → 규칙기반 결과 사용")
        return items

    model = cfg.get("LLM_MODEL", "gpt-4o-mini")
    conf_th = float(cfg.get("LLM_CONF_THRESHOLD", 0.7))
    out = []

    for it in items:
        try:
            user_prompt = _llm_prompt_for_item(it, cfg)
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a professional news classifier for Private Debt / Private Credit in Korea. "
                        "Classify only news where private debt, private credit, direct lending, acquisition financing, "
                        "mezzanine, NAV financing, special situations, distressed debt, NPL investing, structured credit, "
                        "or private credit fund activities are involved or plausibly involved. "
                        "Exclude pure equity M&A, general bank lending news, vanilla bond issuance, stock market news, "
                        "and general macro commentary unless clearly tied to private debt/private credit. "
                        "Refinancing alone is NOT sufficient unless private credit funds, non-bank lenders, or acquisition financing structures are clearly involved. "
                        "Articles are relevant only if private debt/private credit is a MAIN topic or core investment theme; a passing mention is NOT sufficient. "
                        "Exclude generic PF, bridge loan, NPL, asset quality, company management, executive/CEO, or macro/geopolitical articles unless private credit investors, credit funds, non-bank lenders, or lending structures are central to the story. "
                        "Exclude pure PE/M&A exit or equity-sale articles unless the private credit or special situations strategy itself is a central focus of the article, not just a referenced background detail. "
                        "Return JSON only."
                    ),
                },
                {"role": "user", "content": user_prompt},
            ]
            resp = _openai_chat(messages, api_key, model, max_tokens=int(cfg.get("LLM_MAX_TOKENS", 400)))

            j = None
            try:
                j = json.loads(resp.strip())
            except Exception:
                m = re.search(r"\{[\s\S]*\}$", resp.strip())
                if m:
                    j = json.loads(m.group(0))

            cat = (j or {}).get("category", "").lower()
            if (
                isinstance(j, dict)
                and j.get("relevant") is True
                and float(j.get("confidence", 0.0)) >= conf_th
                and cat in {"private debt", "credit fund", "distressed/special sits", "general finance"}
            ):
                it["_llm"] = j
                out.append(it)

        except Exception as e:
            log.warning("LLM 필터 처리 실패: %s", e)
            out.append(it)

    return out

# -------------------------
# LLM 기반 2차 중복 제거
# -------------------------
def _llm_is_same_story(a: dict, b: dict, env: dict, cfg: dict) -> Optional[bool]:
    api_key = env.get("OPENAI_API_KEY", "")
    if not api_key:
        return None

    def _fmt(it: dict) -> str:
        t = (it.get("title") or "").strip()
        d = (it.get("description") or "").strip()
        u = (it.get("url") or "").strip()
        s = (domain_of(u) or it.get("source") or "").strip()
        when = it.get("publishedAt") or ""
        return f"- 제목: {t}\n- 요약: {d}\n- 출처: {s}\n- 시각(UTC): {when}\n- 링크: {u}"

    sys = (
        "You are a professional news deduplication judge for Korean finance news. "
        "Decide if two news items are about the SAME underlying story or issue, "
        "even if titles differ slightly due to portal edits, rewrites, or follow-up angles. "
        "Answer strictly in JSON: {\"same\": true|false, \"reason\": \"...\"}"
    )
    usr = "기사 A\n" + _fmt(a) + "\n\n기사 B\n" + _fmt(b) + "\n\n판정:"

    try:
        resp = _openai_chat(
            [{"role": "system", "content": sys}, {"role": "user", "content": usr}],
            api_key,
            cfg.get("LLM_MODEL", "gpt-4o-mini"),
            max_tokens=200,
            temperature=0.0,
        )
        j = None
        try:
            j = json.loads(resp.strip())
        except Exception:
            m = re.search(r"\{[\s\S]*\}$", resp.strip())
            if m:
                j = json.loads(m.group(0))
        if isinstance(j, dict) and "same" in j:
            return bool(j.get("same"))
    except Exception as e:
        log.warning("LLM dedup 오류: %s", e)
    return None

def _utc_to_kst(ts: str) -> dt.datetime:
    try:
        return dt.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=dt.timezone.utc
        ).astimezone(APP_TZ)
    except Exception:
        return now_kst()

def llm_dedup_items(items: List[dict], cfg: dict, env: dict) -> List[dict]:
    if not items or not cfg.get("USE_LLM_DEDUP", False):
        return items
    if not env.get("OPENAI_API_KEY"):
        return items

    win_hours = int(cfg.get("LLM_DEDUP_WINDOW_HOURS", 24))
    keep_mask = [True] * len(items)

    meta = []
    for it in items:
        tnorm = normalize_title(it.get("title", ""), cfg)
        src = domain_of(it.get("url", ""))
        ts_kst = _utc_to_kst(it.get("publishedAt", ""))
        sc = float(it.get("_score", 0.0))
        meta.append((tnorm, src, ts_kst, sc))

    n = len(items)
    for i in range(n):
        if not keep_mask[i]:
            continue
        for j in range(i + 1, n):
            if not keep_mask[j]:
                continue

            a_t, a_s, a_ts, a_sc = meta[i]
            b_t, b_s, b_ts, b_sc = meta[j]

            time_ok = abs((a_ts - b_ts).total_seconds()) <= win_hours * 3600
            src_same = (a_s == b_s)
            sim = _sim_norm_title(a_t, b_t)

            if (0.50 <= sim < 0.72) or (src_same and time_ok and sim >= 0.45):
                same = _llm_is_same_story(items[i], items[j], env, cfg)
                if same is True:
                    if a_sc >= b_sc:
                        keep_mask[j] = False
                    else:
                        keep_mask[i] = False
                        break

    return [it for k, it in enumerate(items) if keep_mask[k]]

# -------------------------
# 수집/전송
# -------------------------
def collect_all(cfg: dict, env: dict) -> List[dict]:
    keywords = cfg.get("KEYWORDS", []) or []
    page_size = int(cfg.get("PAGE_SIZE", 30))
    recency_hours = int(cfg.get("RECENCY_HOURS", 72))
    max_per_keyword = int(cfg.get("MAX_PER_KEYWORD", page_size))

    all_items: List[dict] = []

    for kw in keywords:
        batch = search_naver_news(
            kw,
            env.get("NAVER_CLIENT_ID", ""),
            env.get("NAVER_CLIENT_SECRET", ""),
            recency_hours=recency_hours,
            page_size=page_size,
        )
        if max_per_keyword > 0:
            batch = batch[:max_per_keyword]
        all_items += batch

    if env.get("NEWSAPI_KEY") and keywords:
        query = " OR ".join(keywords)
        batch = search_newsapi(
            query,
            page_size=page_size,
            api_key=env["NEWSAPI_KEY"],
            from_hours=recency_hours,
            cfg=cfg,
        )
        all_items += batch

    return all_items

def format_telegram_text(items: List[dict], cfg: dict = {}) -> str:
    if not items:
        return "📭 신규 뉴스 없음"

    lines = ["📌 <b>국내 PD 시장 관련 뉴스</b>"]
    for it in items:
        t = it.get("title", "").strip()
        u = it.get("url", "")
        src = domain_of(u)
        try:
            pub = dt.datetime.strptime(it["publishedAt"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=dt.timezone.utc
            ).astimezone(APP_TZ)
            when = pub.strftime("%Y-%m-%d %H:%M")
        except Exception:
            when = "-"

        if bool(cfg.get("SHOW_SOURCE_DOMAIN", False)):
            lines.append(f"• <a href=\"{u}\">{t}</a> — {src} ({when})")
        else:
            lines.append(f"• <a href=\"{u}\">{t}</a> ({when})")

    return "\n".join(lines)

def send_telegram(bot_token: str, chat_id: str, text: str, disable_preview: bool = True) -> bool:
    if not bot_token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview,
    }
    try:
        r = requests.post(url, json=payload, timeout=12)
        r.raise_for_status()
        return True
    except Exception as e:
        log.warning("텔레그램 전송 실패: %s", e)
        return False

def _should_skip_by_time(cfg: dict) -> bool:
    kst_now = now_kst()
    if cfg.get("ONLY_WORKING_HOURS") and not between_working_hours(kst_now, 6, 20):
        return True
    if cfg.get("BLOCK_WEEKEND") and is_weekend(kst_now):
        return True
    if cfg.get("BLOCK_HOLIDAY") and is_holiday(kst_now, cfg.get("HOLIDAYS", [])):
        return True
    return False

# -------------------------
# 실행 겹침 방지 락
# -------------------------
@st.cache_resource(show_spinner=False)
def get_run_lock() -> Lock:
    return Lock()

def transmit_once(cfg: dict, env: dict, preview=False) -> dict:
    run_lock = get_run_lock()
    if not run_lock.acquire(blocking=False):
        log.info("다른 실행이 진행 중이어서 이번 주기는 스킵합니다.")
        return {"count": 0, "items": []}

    try:
        all_items = collect_all(cfg, env)
        ranked = rank_filtered(all_items, cfg)
        ranked = platform_focus_filter(ranked, cfg)
        ranked = llm_filter_items(ranked, cfg, env)
        ranked = llm_dedup_items(ranked, cfg, env)

        if preview:
            return {"count": len(ranked), "items": ranked}

        if _should_skip_by_time(cfg):
            log.info("시간 정책에 의해 전송 건너뜀 (업무시간/주말/공휴일)")
            return {"count": 0, "items": []}

        retention = int(cfg.get("CACHE_RETENTION_HOURS", cfg.get("RECENCY_HOURS", 72)))
        url_cache, story_cache = load_sent_cache_v2(retention_hours=retention)
        now_iso = _utcnow_iso()

        new_items = []
        for it in ranked:
            uhash = sha1(it.get("url", ""))
            skey = story_key(it, cfg)
            if (uhash in url_cache) or (skey in story_cache):
                continue
            new_items.append(it)

        if not new_items:
            if bool(cfg.get("NO_NEWS_SILENT", True)):
                log.info("신규 뉴스 없음 (무소식 알림 억제 옵션으로 미전송)")
                return {"count": 0, "items": []}
            else:
                send_telegram(
                    env.get("TELEGRAM_BOT_TOKEN", ""),
                    env.get("TELEGRAM_CHAT_ID", ""),
                    "📭 신규 뉴스 없음",
                )
                return {"count": 0, "items": []}

        BATCH = 30
        sent_any = False
        for i in range(0, len(new_items), BATCH):
            chunk = new_items[i:i + BATCH]
            text = format_telegram_text(chunk, cfg)
            ok = send_telegram(
                env.get("TELEGRAM_BOT_TOKEN", ""),
                env.get("TELEGRAM_CHAT_ID", ""),
                text,
                disable_preview=bool(cfg.get("TELEGRAM_DISABLE_PREVIEW", True)),
            )
            sent_any = sent_any or ok
            time.sleep(0.6)

        if sent_any:
            for it in new_items:
                url_cache[sha1(it.get("url", ""))] = now_iso
                story_cache[story_key(it, cfg)] = now_iso
            save_sent_cache_v2(url_cache, story_cache)

        return {"count": len(new_items), "items": new_items}
    finally:
        run_lock.release()

# -------------------------
# 스케줄러
# -------------------------
@st.cache_resource(show_spinner=False)
def get_scheduler() -> BackgroundScheduler:
    sched = BackgroundScheduler(timezone=APP_TZ)
    sched.start()
    return sched

def scheduled_job():
    cfg = CURRENT_CFG_DICT or load_config(CURRENT_CFG_PATH)
    try:
        transmit_once(cfg, CURRENT_ENV, preview=False)
    except Exception as e:
        log.exception("스케줄 작업 실패: %s", e)

def is_running(_: BackgroundScheduler = None) -> bool:
    try:
        sched = get_scheduler()
        return any(j.id == "pd_news_job" for j in sched.get_jobs())
    except Exception:
        return False

def start_schedule(cfg_path: str, cfg_dict: dict, env: dict, minutes: int):
    global CURRENT_CFG_PATH, CURRENT_CFG_DICT, CURRENT_ENV
    CURRENT_CFG_PATH = cfg_path
    CURRENT_CFG_DICT = dict(cfg_dict)
    CURRENT_ENV = env

    sched = get_scheduler()
    ensure_scheduled_job(sched, CURRENT_CFG_DICT)

def stop_schedule():
    sched = get_scheduler()
    try:
        sched.remove_job("pd_news_job")
    except Exception:
        pass

# -------------------------
# 특정 플랫폼/운용사 포커스 필터
# -------------------------
def platform_focus_filter(items: list[dict], cfg: dict) -> list[dict]:
    focus = [s.strip() for s in cfg.get("PE_FOCUS", []) if isinstance(s, str) and s.strip()]
    if not focus:
        return items

    def _hit(text: str) -> bool:
        if not text:
            return False
        low = text.lower()
        return any(kw.lower() in low for kw in focus)

    out = []
    for it in items:
        text = f"{it.get('title', '')} {it.get('summary', '')} {it.get('description', '')} {it.get('content', '')}"
        if _hit(text):
            out.append(it)
    return out

# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(page_title="PD 시장 뉴스 모니터링", page_icon="📰", layout="wide")

cfg_path = st.sidebar.text_input("config.json 경로", value=DEFAULT_CONFIG_PATH)
cfg_file = load_config(cfg_path)
st.sidebar.caption(f"Config 로드 상태: {'✅' if cfg_file else '❌'}  · 경로: {cfg_path}")

cfg = dict(cfg_file)

naver_id = st.sidebar.text_input("Naver Client ID", type="password", value=os.getenv("NAVER_CLIENT_ID", ""))
naver_secret = st.sidebar.text_input("Naver Client Secret", type="password", value=os.getenv("NAVER_CLIENT_SECRET", ""))
newsapi_key = st.sidebar.text_input("NewsAPI Key (선택)", type="password", value=os.getenv("NEWSAPI_KEY", ""))
openai_key = st.sidebar.text_input("OpenAI API Key (선택)", type="password", value=os.getenv("OPENAI_API_KEY", ""))
bot_token = st.sidebar.text_input("Telegram Bot Token", type="password", value=os.getenv("TELEGRAM_BOT_TOKEN", ""))
chat_id = st.sidebar.text_input("Telegram Chat ID (채널/그룹)", value=os.getenv("TELEGRAM_CHAT_ID", ""))

st.sidebar.divider()
st.sidebar.subheader("전송/수집 파라미터")
cfg["PAGE_SIZE"] = int(
    st.sidebar.number_input(
        "페이지당 수집 수",
        min_value=10,
        max_value=100,
        step=1,
        value=int(cfg.get("PAGE_SIZE", 30)),
    )
)
cfg["RECENCY_HOURS"] = int(
    st.sidebar.number_input(
        "신선도(최근 N시간)",
        min_value=6,
        max_value=168,
        step=6,
        value=int(cfg.get("RECENCY_HOURS", 72)),
    )
)
cfg["MAX_PER_KEYWORD"] = int(
    st.sidebar.number_input(
        "키워드별 최대 수집 수",
        min_value=1,
        max_value=100,
        step=1,
        value=int(cfg.get("MAX_PER_KEYWORD", 8)),
    )
)

st.sidebar.subheader("시간 정책")
cfg["ONLY_WORKING_HOURS"] = bool(
    st.sidebar.checkbox("✅ 업무시간(06~20 KST) 내 전송", value=bool(cfg.get("ONLY_WORKING_HOURS", True)))
)
cfg["BLOCK_WEEKEND"] = bool(
    st.sidebar.checkbox("🚫 주말 미전송", value=bool(cfg.get("BLOCK_WEEKEND", False)))
)
cfg["BLOCK_HOLIDAY"] = bool(
    st.sidebar.checkbox("🚫 공휴일 미전송", value=bool(cfg.get("BLOCK_HOLIDAY", False)))
)
holidays_text = st.sidebar.text_area(
    "공휴일(YYYY-MM-DD, 쉼표 또는 줄바꿈 구분)",
    value=", ".join(cfg.get("HOLIDAYS", [])),
)
cfg["HOLIDAYS"] = [s.strip() for s in re.split(r"[,\n]", holidays_text) if s.strip()]

st.sidebar.subheader("전송 스케줄")
cfg["SCHEDULE_MODE"] = st.sidebar.radio(
    "스케줄 방식",
    options=["주기(분)", "요일/시각(주간/매일)"],
    index=0 if cfg.get("SCHEDULE_MODE", "주기(분)") == "주기(분)" else 1,
    horizontal=False,
)

if cfg["SCHEDULE_MODE"] == "주기(분)":
    cfg["INTERVAL_MIN"] = int(
        st.sidebar.number_input(
            "전송 주기(분)",
            min_value=5,
            max_value=10080,
            step=5,
            value=int(cfg.get("INTERVAL_MIN", 60)),
        )
    )
    st.sidebar.caption("최대 1주일까지 설정 가능. 시작 시 즉시 1회 전송 후 주기적으로 실행합니다.")
else:
    WEEKDAY_LABELS = ["매일", "월", "화", "수", "목", "금", "토", "일"]
    default_days = cfg.get("CRON_DAYS_UI", ["매일"])
    selected_days = st.sidebar.multiselect(
        "전송 요일 선택 (매일을 선택하면 다른 요일은 무시됩니다)",
        options=WEEKDAY_LABELS,
        default=default_days,
    )
    cfg["CRON_DAYS_UI"] = selected_days[:] if selected_days else ["매일"]

    t = st.sidebar.time_input(
        "전송 시각 (KST)",
        value=dt.time(hour=int(cfg.get("CRON_HOUR", 9)), minute=int(cfg.get("CRON_MINUTE", 0))),
    )
    cfg["CRON_HOUR"] = int(t.hour)
    cfg["CRON_MINUTE"] = int(t.minute)

    st.sidebar.caption("요일/시각 모드에서는 시작 즉시 전송하지 않고 지정 시각에만 전송합니다.")

st.sidebar.subheader("기타 필터")
cfg["ALLOWLIST_STRICT"] = bool(
    st.sidebar.checkbox("🧱 ALLOWLIST_STRICT (허용 도메인 외 차단)", value=bool(cfg.get("ALLOWLIST_STRICT", False)))
)

st.sidebar.subheader("LLM 필터(선택)")
cfg["USE_LLM_FILTER"] = bool(
    st.sidebar.checkbox("🤖 OpenAI로 2차 필터링", value=bool(cfg.get("USE_LLM_FILTER", False)))
)
cfg["LLM_MODEL"] = st.sidebar.text_input("모델", value=cfg.get("LLM_MODEL", "gpt-4o-mini"))
cfg["LLM_CONF_THRESHOLD"] = float(
    st.sidebar.slider(
        "채택 임계치(신뢰도)",
        min_value=0.0,
        max_value=1.0,
        value=float(cfg.get("LLM_CONF_THRESHOLD", 0.7)),
        step=0.05,
    )
)
cfg["LLM_MAX_TOKENS"] = int(
    st.sidebar.number_input(
        "max_tokens",
        min_value=64,
        max_value=1000,
        step=10,
        value=int(cfg.get("LLM_MAX_TOKENS", 300)),
    )
)

st.sidebar.subheader("🎯 특정 크레딧 운용사/플랫폼 선택(선택)")
platform_candidates_default = [
    "Apollo", "Ares", "Blue Owl", "Oaktree", "Golub", "HPS", "Sixth Street",
    "Blackstone", "KKR", "Carlyle", "PIMCO", "Davidson Kempner",
    "IMM", "MBK", "한앤컴퍼니", "VIG", "스틱", "맥쿼리", "KCGI"
]
platform_candidates = cfg.get("PE_CANDIDATES", platform_candidates_default)
cfg["PE_FOCUS"] = st.sidebar.multiselect(
    "특정 운용사/플랫폼만 선별(비워두면 전체)",
    options=platform_candidates,
    default=cfg.get("PE_FOCUS", []),
)

st.sidebar.divider()
if st.sidebar.button("구성 리로드", use_container_width=True):
    st.rerun()

st.title("📰 국내 PD 시장 뉴스 자동 모니터링")
st.caption("Streamlit + Naver/NewsAPI + OpenAI Filter + Telegram + APScheduler")

def make_env() -> dict:
    return {
        "NAVER_CLIENT_ID": naver_id,
        "NAVER_CLIENT_SECRET": naver_secret,
        "NEWSAPI_KEY": newsapi_key,
        "OPENAI_API_KEY": openai_key,
        "TELEGRAM_BOT_TOKEN": bot_token,
        "TELEGRAM_CHAT_ID": chat_id,
    }

col1, col2, col3, col4 = st.columns(4)
sched = get_scheduler()

with col1:
    if st.button("지금 한번 실행(미리보기)", type="primary"):
        res = transmit_once(cfg, make_env(), preview=True)
        st.session_state["preview"] = res

with col2:
    if st.button("지금 한번 전송"):
        res = transmit_once(cfg, make_env(), preview=False)
        st.session_state["preview"] = res

with col3:
    if st.button("스케줄 시작"):
        start_schedule(cfg_path=cfg_path, cfg_dict=cfg, env=make_env(), minutes=int(cfg.get("INTERVAL_MIN", 60)))
        if cfg.get("SCHEDULE_MODE", "주기(분)") == "주기(분)":
            st.success("스케줄 시작됨: 즉시 1회 전송 후 주기적으로 실행")
        else:
            st.success("스케줄 시작됨: 지정된 요일/시각에만 전송(시작 즉시 전송 없음)")
        st.rerun()

with col4:
    if st.button("스케줄 중지"):
        stop_schedule()
        st.warning("스케줄 중지됨")
        st.rerun()

_running = is_running()
st.subheader("상태")
sched = get_scheduler()
jobs = []
try:
    jobs = sched.get_jobs()
except Exception:
    jobs = []

st.info(f"Scheduler 실행 중: {_running}")
for j in jobs:
    st.caption(f"• Job: {j.id} / 다음 실행: {j.next_run_time}")

st.subheader("📋 필터링된 전체 기사")
res = st.session_state.get("preview", {"items": []})
items = res.get("items", [])

if not items:
    st.write("결과 없음")
else:
    for it in items:
        t = it.get("title", "")
        u = it.get("url", "")
        desc = it.get("description", "")
        try:
            pub = dt.datetime.strptime(it["publishedAt"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=dt.timezone.utc
            ).astimezone(APP_TZ)
            when = pub.strftime("%Y-%m-%d %H:%M")
        except Exception:
            when = "-"

        meta = it.get("_llm")
        if meta:
            _m = ", ".join([str(x) for x in (meta.get("matched") or [])][:6])
            reason = meta.get("reason", "")
            st.markdown(
                f"- <a href='{u}'>{t}</a> ({when})"
                f"<br><span style='color:gray'>{desc}</span>"
                f"<br><span style='color:gray'>LLM: {meta.get('confidence', 0):.2f}"
                + (f" · {_m}" if _m else "")
                + (f" · {reason}" if reason else "")
                + "</span>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"- <a href='{u}'>{t}</a> ({when})"
                f"<br><span style='color:gray'>{desc}</span>",
                unsafe_allow_html=True,
            )
