# ingest/fetch_gdelt.py
# GDELT DOC 2.0 ArtList fetcher with robust English handling + fallbacks.
# Strategy:
#   1) Try QUERY + "sourcelang:english" (when ONLY_ENGLISH=1) for the given TIMESPAN.
#   2) If 0 rows, retry SAME window WITHOUT "sourcelang:" then post-filter to English.
#   3) If still 0, widen window once (e.g., 1h -> 2h) WITHOUT "sourcelang:" then post-filter.
# Also: SORT=DateDesc so latest items appear first.
#
# Outputs:
#   docs/parquet/YYYY/MM/YYYY-MM-DD.parquet
#   docs/manifests/index.json  (absolute URLs if REPO_BASE_URL is set)
#
# Python 3.6+ compatible typing.

import sys, os, json, hashlib, time, random, io, glob, re
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import requests
import pandas as pd
from dateutil import parser as dtp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- settings ----------------
QUERY = os.environ.get(
    "GDELT_QUERY",
    '("artificial intelligence" OR "generative ai" OR "large language model" '
    'OR "ai safety" OR "frontier model" OR OpenAI OR Anthropic OR DeepMind '
    'OR "Google DeepMind" OR "Meta AI" OR "Mistral AI" OR "Llama 3" '
    'OR "GPT-4o" OR "Claude 3")'
)

# accept either env var spelling
_only_en = os.environ.get("GDELT_ONLY_ENGLISH", os.environ.get("GDELT_ONLY_EN", "1"))
ONLY_ENGLISH = (_only_en or "1") == "1"

TIMESPAN = os.environ.get("GDELT_TIMESPAN", "1h")  # e.g., 15min, 1h, 2h, 1d, etc. (DOC supports these)  # ref: Debut blog
MAXRECORDS = int(os.environ.get("GDELT_MAXRECORDS", "200"))
USER_AGENT = os.environ.get("USER_AGENT", "ai-news-graph/1.0 (+https://github.com/<your-username>/ai-news-graph)")
SORT = os.environ.get("GDELT_SORT", "DateDesc")  # DateDesc | DateAsc | ToneDesc | ToneAsc | HybridRel

OUT_ROOT = sys.argv[1] if len(sys.argv) > 1 else "."
PARQUET_DIR = os.path.join(OUT_ROOT, "parquet")
MANIFEST_DIR = os.path.join(OUT_ROOT, "manifests")
MANIFEST_PATH = os.path.join(MANIFEST_DIR, "index.json")
REPO_BASE_URL = os.environ.get("REPO_BASE_URL", "")

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})\.parquet$")

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{ts}] {msg}", flush=True)

def stable_id(url: str) -> str:
    return hashlib.sha1(url.strip().lower().encode("utf-8")).hexdigest()

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=1.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, application/feed+json, text/csv;q=0.9, */*;q=0.8"
    })
    return s

def _params(fmt: str, query: str, timespan: str) -> Dict[str, str]:
    return {
        "query": query,
        "mode": "artlist",
        "timespan": timespan,
        "maxrecords": str(MAXRECORDS),
        "format": fmt,         # json | jsonfeed | csv
        "sort": SORT           # ensure newest first
    }

def _parse_articles_from_json(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    arts = d.get("articles", [])
    rows: List[Dict[str, Any]] = []
    for a in arts:
        url = (a.get("url") or "").strip()
        if not url:
            continue
        seen = a.get("seendate")
        try:
            published_at = dtp.parse(seen).astimezone(timezone.utc).isoformat()
        except Exception:
            published_at = None
        rows.append({
            "id": stable_id(url),
            "url": url,
            "title": a.get("title"),
            "published_at": published_at,
            "domain": a.get("domain"),
            "language": a.get("language"),
            "source_country": a.get("sourcecountry"),
            "social_image": a.get("socialimage"),
        })
    return rows

def _parse_articles_from_jsonfeed(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = d.get("items", [])
    rows: List[Dict[str, Any]] = []
    for it in items:
        url = (it.get("url") or it.get("external_url") or "").strip()
        if not url:
            continue
        ts = it.get("date_published") or it.get("date_modified")
        try:
            published_at = dtp.parse(ts).astimezone(timezone.utc).isoformat() if ts else None
        except Exception:
            published_at = None
        rows.append({
            "id": stable_id(url),
            "url": url,
            "title": it.get("title"),
            "published_at": published_at,
            "domain": None,
            "language": None,
            "source_country": None,
            "social_image": None,
        })
    return rows

def _parse_articles_from_csv(txt: str) -> List[Dict[str, Any]]:
    df = pd.read_csv(io.StringIO(txt))
    for col in ["url","title","seendate","domain","language","sourcecountry","socialimage"]:
        if col not in df.columns:
            df[col] = None
    rows: List[Dict[str, Any]] = []
    for _, a in df.iterrows():
        url = (str(a.get("url") or "").strip())
        if not url:
            continue
        seen = a.get("seendate")
        try:
            published_at = dtp.parse(str(seen)).astimezone(timezone.utc).isoformat() if pd.notna(seen) else None
        except Exception:
            published_at = None
        rows.append({
            "id": stable_id(url),
            "url": url,
            "title": a.get("title") if pd.notna(a.get("title")) else None,
            "published_at": published_at,
            "domain": a.get("domain") if pd.notna(a.get("domain")) else None,
            "language": a.get("language") if pd.notna(a.get("language")) else None,
            "source_country": a.get("sourcecountry") if pd.notna(a.get("sourcecountry")) else None,
            "social_image": a.get("socialimage") if pd.notna(a.get("socialimage")) else None,
        })
    return rows

def _one_fetch(session: requests.Session, q: str, timespan: str) -> pd.DataFrame:
    """Attempt JSON → JSONFeed → CSV for given query+timespan."""
    time.sleep(random.uniform(0.3, 1.2))  # jitter

    # JSON
    r = session.get(BASE_URL, params=_params("json", q, timespan), timeout=30)
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    if "json" in ct:
        try:
            rows = _parse_articles_from_json(r.json())
            if rows:
                return pd.DataFrame(rows)
        except Exception as e:
            log(f"JSON parse failed ({e}); trying JSONFeed")

    # JSONFeed
    r2 = session.get(BASE_URL, params=_params("jsonfeed", q, timespan), timeout=30)
    r2.raise_for_status()
    ct2 = (r2.headers.get("content-type") or "").lower()
    if "json" in ct2:
        try:
            rows = _parse_articles_from_jsonfeed(r2.json())
            if rows:
                return pd.DataFrame(rows)
        except Exception as e:
            log(f"JSONFeed parse failed ({e}); trying CSV")

    # CSV
    r3 = session.get(BASE_URL, params=_params("csv", q, timespan), timeout=30)
    r3.raise_for_status()
    txt = r3.text
    if txt and len(txt.strip()) > 0:
        try:
            rows = _parse_articles_from_csv(txt)
            return pd.DataFrame(rows)
        except Exception as e:
            sample = txt[:200].replace("\n", " ")
            raise RuntimeError("CSV parse failed. Sample: {}".format(sample)) from e

    # truly empty
    return pd.DataFrame([])

def _english_post_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "language" not in df.columns:
        return pd.DataFrame([]) if ONLY_ENGLISH else df
    if ONLY_ENGLISH:
        return df[df["language"].fillna("").str.lower() == "english"]
    return df

def _wider_timespan(ts: str) -> str:
    # Simple “one step wider” for common forms: 15min -> 30min, 30min -> 1h, 1h -> 2h, 2h -> 3h, 6h -> 12h, 1d -> 2d
    ts = ts.strip().lower()
    mapping = {"15min":"30min","30min":"1h","45min":"1h","1h":"2h","2h":"3h","3h":"6h","6h":"12h","12h":"1d","1d":"2d"}
    return mapping.get(ts, ts)

def fetch_gdelt_artlist(session: requests.Session) -> pd.DataFrame:
    """Try inline-English first; fallback to post-filter (same window); fallback widen window."""
    base_q = QUERY
    inline_en_q = f"({base_q}) sourcelang:english"
    use_inline = ONLY_ENGLISH

    # Attempt A: inline English
    if use_inline:
        log(f"Attempt A: inline sourcelang (TIMESPAN={TIMESPAN}, SORT={SORT})")
        df_a = _one_fetch(session, inline_en_q, TIMESPAN)
        log(f"Attempt A rows: {len(df_a)}")
        if len(df_a) > 0:
            # normalize
            if "published_at" in df_a.columns:
                df_a["published_at"] = pd.to_datetime(df_a["published_at"], utc=True, errors="coerce")
            return df_a

    # Attempt B: no inline language, post-filter
    log(f"Attempt B: post-filter English (TIMESPAN={TIMESPAN}, SORT={SORT})")
    df_b = _one_fetch(session, base_q, TIMESPAN)
    log(f"Attempt B raw rows: {len(df_b)}")
    if not df_b.empty:
        if "published_at" in df_b.columns:
            df_b["published_at"] = pd.to_datetime(df_b["published_at"], utc=True, errors="coerce")
        df_b_en = _english_post_filter(df_b)
        log(f"Attempt B after English filter: {len(df_b_en)}")
        if len(df_b_en) > 0:
            return df_b_en

    # Attempt C: widen timespan once, post-filter
    wider = _wider_timespan(TIMESPAN)
    if wider != TIMESPAN:
        log(f"Attempt C: widen timespan to {wider} (post-filter English)")
        df_c = _one_fetch(session, base_q, wider)
        log(f"Attempt C raw rows: {len(df_c)}")
        if not df_c.empty:
            if "published_at" in df_c.columns:
                df_c["published_at"] = pd.to_datetime(df_c["published_at"], utc=True, errors="coerce")
            df_c_en = _english_post_filter(df_c)
            log(f"Attempt C after English filter: {len(df_c_en)}")
            if len(df_c_en) > 0:
                return df_c_en

    # nada
    return pd.DataFrame([])

def write_daily_parquet(df: pd.DataFrame) -> Tuple[List[str], int, int]:
    if df.empty:
        log("No rows to write.")
        return [], 0, 0

    df = df.dropna(subset=["id","url"]).copy()

    def day_of(row: pd.Series) -> str:
        try:
            return row["published_at"].date().isoformat()
        except Exception:
            return datetime.now(timezone.utc).date().isoformat()

    df["day"] = df.apply(day_of, axis=1)
    os.makedirs(PARQUET_DIR, exist_ok=True)
    written: List[str] = []
    new_total = 0
    existing_total = 0

    for day, g in df.groupby("day"):
        y, m, d = day.split("-")
        out_dir = os.path.join(PARQUET_DIR, y, m)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{day}.parquet")

        if os.path.exists(out_path):
            old = pd.read_parquet(out_path)
            prev_ids = set(old["id"].astype(str))
            g_ids = set(g["id"].astype(str))
            new_ids = g_ids - prev_ids
            existing_ids = g_ids & prev_ids
            new_total += len(new_ids)
            existing_total += len(existing_ids)
            combined = old if not new_ids else pd.concat([old, g[g["id"].astype(str).isin(new_ids)]], ignore_index=True)
            cols = ["id","url","title","published_at","domain","language","source_country","social_image"]
            combined[cols].to_parquet(out_path, index=False)
            written.append(out_path)
            log(f"{day}: existing={len(existing_ids)} new={len(new_ids)} total_now={len(combined)} → {out_path}")
        else:
            cols = ["id","url","title","published_at","domain","language","source_country","social_image"]
            g[cols].to_parquet(out_path, index=False)
            new_total += len(g)
            written.append(out_path)
            log(f"{day}: first file with {len(g)} rows → {out_path}")

    return written, new_total, existing_total

def update_manifest(max_files: int = 30) -> None:
    os.makedirs(MANIFEST_DIR, exist_ok=True)
    pattern = os.path.join(OUT_ROOT, "parquet", "**", "*.parquet")
    paths = glob.glob(pattern, recursive=True)

    def file_key(p: str) -> Tuple[int, int, int, float]:
        m = DATE_RE.search(os.path.basename(p))
        if m:
            y, mo, d = list(map(int, m.groups()))
            return (y, mo, d, 0.0)
        try:
            return (0, 0, 0, float(os.path.getmtime(p)))
        except OSError:
            return (0, 0, 0, 0.0)

    paths.sort(key=file_key)
    latest = paths[-max_files:]

    urls: List[str] = []
    for p in latest:
        rel_from_out = os.path.relpath(p, OUT_ROOT).replace(os.sep, "/")  # parquet/…
        if REPO_BASE_URL:
            url = f"{REPO_BASE_URL}/{rel_from_out}"
        else:
            rel_from_repo = os.path.relpath(p, ".").replace(os.sep, "/")
            url = f"/{rel_from_repo}"
        urls.append(url)

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump({"files": urls}, f, indent=2)

    log(f"Manifest updated at {MANIFEST_PATH} with {len(urls)} file(s).")

if __name__ == "__main__":
    log(f"OUT_ROOT={OUT_ROOT}  REPO_BASE_URL={REPO_BASE_URL or '(none)'}")
    log(f"QUERY=(hidden)  TIMESPAN={TIMESPAN}  MAXRECORDS={MAXRECORDS}  ONLY_ENGLISH={ONLY_ENGLISH}  SORT={SORT}")
    session = make_session()
    df = fetch_gdelt_artlist(session)
    wrote, new_total, existing_total = write_daily_parquet(df)
    update_manifest()
    log(f"Fetched {len(df)} rows; new={new_total}, existing={existing_total}. Updated files: {wrote}")
