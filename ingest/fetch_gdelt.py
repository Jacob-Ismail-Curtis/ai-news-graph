# ingest/fetch_gdelt.py
# Robust GDELT DOC 2.0 ArtList fetcher with verbose logging:
# - English-only (query + post-filter)
# - Retries with backoff
# - Validates content-type & JSON; fallbacks to jsonfeed → csv
# - Writes daily Parquet under OUT_ROOT/parquet/YYYY/MM/DD.parquet
# - Builds OUT_ROOT/manifests/index.json listing newest Parquet files
# - Prints counts (fetched/kept/new/existing) so CI logs explain "No changes"
import sys, os, json, hashlib, time, random, io, glob, re, requests
from datetime import datetime, timezone
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

# accept either env name (your workflow used GDELT_ONLY_EN previously)
_only_en = os.environ.get("GDELT_ONLY_ENGLISH", os.environ.get("GDELT_ONLY_EN", "1"))
ONLY_ENGLISH = (_only_en or "1") == "1"

TIMESPAN = os.environ.get("GDELT_TIMESPAN", "1h")
MAXRECORDS = int(os.environ.get("GDELT_MAXRECORDS", "200"))
USER_AGENT = os.environ.get(
    "USER_AGENT",
    "ai-news-graph/1.0 (+https://github.com/<your-username>/ai-news-graph)"
)

# OUT_ROOT is where parquet & manifest live (e.g., "docs")
OUT_ROOT = sys.argv[1] if len(sys.argv) > 1 else "."
PARQUET_DIR = os.path.join(OUT_ROOT, "parquet")
MANIFEST_DIR = os.path.join(OUT_ROOT, "manifests")
MANIFEST_PATH = os.path.join(MANIFEST_DIR, "index.json")
REPO_BASE_URL = os.environ.get("REPO_BASE_URL", "")  # set in CI for absolute URLs

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})\.parquet$")

def log(msg: str):
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
    df = pd.read_csv(io.StringIO(txt))
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
    time.sleep(random.uniform(0.5, 2.0))

    # 1) JSON
    log(f"Requesting JSON: timespan={TIMESPAN} maxrecords={MAXRECORDS} only_english={ONLY_ENGLISH}")
    r = session.get(BASE_URL, params=_params("json"), timeout=30)
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    rows: list[dict] = []
    if "json" in ct:
        try:
            data = r.json()
            rows = _parse_articles_from_json(data)
            log(f"JSON parsed: {len(rows)} articles")
        except Exception as e:
            log(f"JSON parse failed, will try jsonfeed. Error: {e}")

    # 2) JSONFeed
    if not rows:
        r2 = session.get(BASE_URL, params=_params("jsonfeed"), timeout=30)
        r2.raise_for_status()
        ct2 = (r2.headers.get("content-type") or "").lower()
        if "json" in ct2:
            try:
                data2 = r2.json()
                rows = _parse_articles_from_jsonfeed(data2)
                log(f"JSONFeed parsed: {len(rows)} items")
            except Exception as e:
                log(f"JSONFeed parse failed, will try CSV. Error: {e}")

    # 3) CSV
    if not rows:
        r3 = session.get(BASE_URL, params=_params("csv"), timeout=30)
        r3.raise_for_status()
        txt = r3.text
        if txt and len(txt.strip()) > 0:
            try:
                rows = _parse_articles_from_csv(txt)
                log(f"CSV parsed: {len(rows)} rows")
            except Exception as e:
                sample = txt[:200].replace("\n", " ")
                raise RuntimeError(f"GDELT returned non-JSON and CSV parse failed. Sample: {sample}") from e
        else:
            raise RuntimeError("GDELT returned empty response for JSON and CSV.")

    df = pd.DataFrame(rows)
    log(f"Fetched rows total: {len(df)}")

    if not df.empty:
        # Coerce to real UTC timestamp for efficient Parquet scans
        df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")

    # English-only post-filter (belt & suspenders)
    if ONLY_ENGLISH and not df.empty and "language" in df.columns:
        before = len(df)
        df = df[df["language"].fillna("").str.lower() == "english"]
        log(f"After English filter: {len(df)} (dropped {before - len(df)})")

    langs = df["language"].fillna("NA").str.lower().value_counts().to_dict() if not df.empty else {}
    if langs:
        log(f"Language breakdown: {langs}")

    return df

def write_daily_parquet(df: pd.DataFrame):
    if df.empty:
        log("No rows to write.")
        return [], 0, 0

    df = df.dropna(subset=["id","url"]).copy()

    # decide day partition from timestamp if present, else today
    def day_of(row):
        try:
            return row["published_at"].date().isoformat()
        except Exception:
            return datetime.now(timezone.utc).date().isoformat()

    df["day"] = df.apply(day_of, axis=1)
    os.makedirs(PARQUET_DIR, exist_ok=True)
    written = []
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
            if new_ids:
                g_new = g[g["id"].astype(str).isin(new_ids)]
                combined = pd.concat([old, g_new], ignore_index=True)
            else:
                combined = old
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

def update_manifest(max_files: int = 30):
    os.makedirs(MANIFEST_DIR, exist_ok=True)
    pattern = os.path.join(OUT_ROOT, "parquet", "**", "*.parquet")
    paths = glob.glob(pattern, recursive=True)

    def file_key(p: str):
        m = DATE_RE.search(os.path.basename(p))
        if m:
            y, mo, d = map(int, m.groups())
            return (y, mo, d, 0.0)
        try:
            return (0, 0, 0, os.path.getmtime(p))
        except OSError:
            return (0, 0, 0, 0.0)

    paths.sort(key=file_key)
    latest = paths[-max_files:]

    urls = []
    for p in latest:
        rel_from_out = os.path.relpath(p, OUT_ROOT).replace(os.sep, "/")  # parquet/…
        if REPO_BASE_URL:
            url = f"{REPO_BASE_URL}/{rel_from_out}"
        else:
            rel_from_repo = os.path.relpath(p, ".").replace(os.sep, "/")   # /docs/parquet/…
            url = f"/{rel_from_repo}"
        urls.append(url)

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump({"files": urls}, f, indent=2)

    log(f"Manifest updated at {MANIFEST_PATH} with {len(urls)} file(s).")

if __name__ == "__main__":
    log(f"OUT_ROOT={OUT_ROOT}  REPO_BASE_URL={REPO_BASE_URL or '(none)'}")
    log(f"QUERY={'(hidden)'}  TIMESPAN={TIMESPAN}  MAXRECORDS={MAXRECORDS}  ONLY_ENGLISH={ONLY_ENGLISH}")
    session = make_session()
    df = fetch_gdelt_artlist(session)
    wrote, new_total, existing_total = write_daily_parquet(df)
    update_manifest()
    log(f"Fetched {len(df)} rows; new={new_total}, existing={existing_total}. Updated files: {wrote}")
