# ingest/fetch_gdelt.py
# Robust GDELT DOC 2.0 ArtList fetcher:
# - English-only (query + post-filter)
# - Retries with backoff
# - Validates content-type & JSON
# - Fallbacks: jsonfeed -> csv
import sys, os, json, hashlib, time, random, io, requests
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtp
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------- settings (can override via env) --------
QUERY = os.environ.get(
    "GDELT_QUERY",
    '("artificial intelligence" OR "generative ai" OR "large language model" '
    'OR "ai safety" OR "frontier model" OR OpenAI OR Anthropic OR DeepMind '
    'OR "Google DeepMind" OR "Meta AI" OR "Mistral AI" OR "Llama 3" '
    'OR "GPT-4o" OR "Claude 3")'
)
ONLY_ENGLISH = os.environ.get("GDELT_ONLY_ENGLISH", "1") == "1"
TIMESPAN = os.environ.get("GDELT_TIMESPAN", "1h")
MAXRECORDS = int(os.environ.get("GDELT_MAXRECORDS", "200"))
USER_AGENT = os.environ.get(
    "USER_AGENT",
    "ai-news-graph/1.0 (+https://github.com/<your-username>/ai-news-graph)"
)

OUT_ROOT = sys.argv[1] if len(sys.argv) > 1 else "."   # e.g. "docs"
PARQUET_DIR = os.path.join(OUT_ROOT, "parquet")
MANIFEST_PATH = os.path.join(OUT_ROOT, "manifests", "index.json")
REPO_BASE_URL = os.environ.get("REPO_BASE_URL", "")

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

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

def _params(fmt: str) -> dict:
    q = f"({QUERY}) sourcelang:english" if ONLY_ENGLISH else QUERY
    return {
        "query": q,
        "mode": "artlist",
        "timespan": TIMESPAN,
        "maxrecords": str(MAXRECORDS),
        "format": fmt  # 'json' | 'jsonfeed' | 'csv'
    }

def _parse_articles_from_json(d: dict) -> list[dict]:
    # DOC 2.0 JSON: { "articles": [ {...} ] }
    arts = d.get("articles", [])
    rows = []
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

def _parse_articles_from_jsonfeed(d: dict) -> list[dict]:
    # JSONFeed: { "items": [ {"url","title","date_published",...} ] }
    items = d.get("items", [])
    rows = []
    for it in items:
        url = (it.get("url") or it.get("external_url") or "").strip()
        if not url:
            continue
        ts = it.get("date_published") or it.get("date_modified")
        try:
            published_at = dtp.parse(ts).astimezone(timezone.utc).isoformat() if ts else None
        except Exception:
            published_at = None
        # Some fields aren’t present in jsonfeed; keep what we can
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

def _parse_articles_from_csv(txt: str) -> list[dict]:
    # CSV columns generally include: url,url_mobile,title,seendate,domain,language,sourcecountry,socialimage,...
    df = pd.read_csv(io.StringIO(txt))
    # Standardize columns if missing
    for col in ["url","title","seendate","domain","language","sourcecountry","socialimage"]:
        if col not in df.columns:
            df[col] = None
    rows = []
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

def fetch_gdelt_artlist(session: requests.Session) -> pd.DataFrame:
    # jitter to avoid stampedes
    time.sleep(random.uniform(0.5, 2.0))

    # 1) Try JSON
    r = session.get(BASE_URL, params=_params("json"), timeout=30)
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    rows: list[dict] = []
    if "json" in ct:
        try:
            data = r.json()
            rows = _parse_articles_from_json(data)
        except Exception:
            # continue to jsonfeed
            rows = []
    if not rows:
        # 2) Try JSONFeed
        r2 = session.get(BASE_URL, params=_params("jsonfeed"), timeout=30)
        r2.raise_for_status()
        ct2 = (r2.headers.get("content-type") or "").lower()
        if "json" in ct2:
            try:
                data2 = r2.json()
                rows = _parse_articles_from_jsonfeed(data2)
            except Exception:
                rows = []
    if not rows:
        # 3) Try CSV
        r3 = session.get(BASE_URL, params=_params("csv"), timeout=30)
        r3.raise_for_status()
        txt = r3.text
        if txt and len(txt.strip()) > 0:
            try:
                rows = _parse_articles_from_csv(txt)
            except Exception as e:
                sample = txt[:200].replace("\n", " ")
                raise RuntimeError(f"GDELT returned non-JSON and CSV parse failed. Sample: {sample}") from e
        else:
            raise RuntimeError("GDELT returned empty response for JSON and CSV.")

    df = pd.DataFrame(rows)

    # English-only post-filter (belt & suspenders)
    if ONLY_ENGLISH and not df.empty and "language" in df.columns:
        df = df[df["language"].fillna("").str.lower() == "english"]

    return df

def write_daily_parquet(df: pd.DataFrame):
    if df.empty:
        return []
    df = df.dropna(subset=["id","url"]).copy()

    def day_of(row):
        try:
            return datetime.fromisoformat(row["published_at"].replace("Z","+00:00")).date().isoformat()
        except Exception:
            return datetime.now(timezone.utc).date().isoformat()

    df["day"] = df.apply(day_of, axis=1)
    os.makedirs(PARQUET_DIR, exist_ok=True)
    written = []

    for day, g in df.groupby("day"):
        y, m, d = day.split("-")
        out_dir = os.path.join(PARQUET_DIR, y, m)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{day}.parquet")

        if os.path.exists(out_path):
            old = pd.read_parquet(out_path)
            g = pd.concat([old, g], ignore_index=True).drop_duplicates("id", keep="first")

        cols = ["id","url","title","published_at","domain","language","source_country","social_image"]
        g[cols].to_parquet(out_path, index=False)
        written.append(out_path)
    return written

def update_manifest():
    """
    Update manifests/index.json with URLs to the latest 30 daily Parquet files.
    Looks for files under OUT_ROOT (e.g., 'docs/parquet/...').
    """
    manifest_dir = os.path.join(OUT_ROOT, "manifests")
    os.makedirs(manifest_dir, exist_ok=True)
    manifest_path = os.path.join(manifest_dir, "index.json")

    today = datetime.now(timezone.utc).date()
    urls = []

    for i in range(30):
        day = today - timedelta(days=i)
        y, m, d = f"{day.year}", f"{day.month:02d}", f"{day.day:02d}"
        rel = f"parquet/{y}/{m}/{y}-{m}-{d}.parquet"
        full_path = os.path.join(OUT_ROOT, rel)  # <-- check under OUT_ROOT
        if os.path.exists(full_path):
            # emit absolute URL if REPO_BASE_URL is set; otherwise relative
            urls.append(f"{REPO_BASE_URL}/{rel}" if REPO_BASE_URL else rel)

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({"files": list(reversed(urls))}, f, indent=2)

    print(f"Manifest updated at {manifest_path} with {len(urls)} file(s).")