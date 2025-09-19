# ingest/fetch_gdelt.py
# Forces English-only results:
# - Adds `sourcelang:english` to the DOC 2.0 query
# - Post-filters rows where language == "English"
import sys, os, json, hashlib, time, random, requests
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

# Toggle with env if you ever need non-English: export GDELT_ONLY_ENGLISH=0
ONLY_ENGLISH = os.environ.get("GDELT_ONLY_ENGLISH", "1") == "1"

TIMESPAN = os.environ.get("GDELT_TIMESPAN", "1h")            # keep 1h for hourly runs
MAXRECORDS = int(os.environ.get("GDELT_MAXRECORDS", "200"))  # 200 to be polite
USER_AGENT = os.environ.get(
    "USER_AGENT",
    "ai-news-graph/1.0 (+https://github.com/<your-username>/ai-news-graph)"
)

OUT_ROOT = sys.argv[1] if len(sys.argv) > 1 else "."   # e.g. "docs"
PARQUET_DIR = os.path.join(OUT_ROOT, "parquet")
MANIFEST_PATH = os.path.join(OUT_ROOT, "manifests", "index.json")
REPO_BASE_URL = os.environ.get("REPO_BASE_URL", "")

def stable_id(url: str) -> str:
    return hashlib.sha1(url.strip().lower().encode("utf-8")).hexdigest()

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,                # up to 6 tries
        backoff_factor=1.5,     # 1.5s, 3s, 4.5s, 6.75s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": USER_AGENT})
    return s

def fetch_gdelt_artlist(session: requests.Session) -> pd.DataFrame:
    """Fetch the last hour of AI-related articles from GDELT DOC 2.0 (ArtList) with retry/backoff."""
    base = "https://api.gdeltproject.org/api/v2/doc/doc"

    # Add English-only constraint at the query level
    q = f"({QUERY}) sourcelang:english" if ONLY_ENGLISH else QUERY

    params = {
        "query": q,
        "mode": "artlist",
        "timespan": TIMESPAN,
        "maxrecords": str(MAXRECORDS),
        "format": "json",
    }

    # small jitter before request (reduces stampedes)
    time.sleep(random.uniform(0.5, 2.0))

    r = session.get(base, params=params, timeout=30)
    r.raise_for_status()  # will trigger urllib3.Retry on 429/5xx
    data = r.json()
    arts = data.get("articles", [])
    rows = []
    for a in arts:
        url = (a.get("url") or "").strip()
        if not url:
            continue
        rid = stable_id(url)
        seen = a.get("seendate")
        try:
            published_at = dtp.parse(seen).astimezone(timezone.utc).isoformat()
        except Exception:
            published_at = None
        rows.append({
            "id": rid,
            "url": url,
            "title": a.get("title"),
            "published_at": published_at,
            "domain": a.get("domain"),
            "language": a.get("language"),
            "source_country": a.get("sourcecountry"),
            "social_image": a.get("socialimage"),
        })

    df = pd.DataFrame(rows)

    # Post-filter safeguard: keep only rows whose original language is English
    if ONLY_ENGLISH and not df.empty:
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
    os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)
    today = datetime.now(timezone.utc).date()

    urls = []
    for i in range(30):
        day = today - timedelta(days=i)
        y, m, d = f"{day.year}", f"{day.month:02d}", f"{day.day:02d}"
        rel = f"parquet/{y}/{m}/{y}-{m}-{d}.parquet"
        if os.path.exists(rel):
            urls.append(f"{REPO_BASE_URL}/{rel}" if REPO_BASE_URL else rel)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump({"files": list(reversed(urls))}, f, indent=2)

if __name__ == "__main__":
    session = make_session()
    df = fetch_gdelt_artlist(session)
    wrote = write_daily_parquet(df)
    update_manifest()
    print(f"Fetched {len(df)} articles (English-only={ONLY_ENGLISH}). Updated: {wrote}")
