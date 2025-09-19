# ingest/fetch_gdelt.py
import sys, os, json, hashlib, requests
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtp
import pandas as pd

OUT_ROOT = sys.argv[1] if len(sys.argv) > 1 else "."  # default repo root
PARQUET_DIR = os.path.join(OUT_ROOT, "parquet")
MANIFEST_PATH = os.path.join(OUT_ROOT, "manifests", "index.json")
REPO_BASE_URL = os.environ.get("REPO_BASE_URL", "")

# ... rest of the file unchanged except it now writes to PARQUET_DIR/MANIFEST_PATH ...

# ---- settings ----
QUERY = ('("artificial intelligence" OR "generative ai" OR "large language model" '
         'OR "ai safety" OR "frontier model" OR OpenAI OR Anthropic OR DeepMind '
         'OR "Google DeepMind" OR "Meta AI" OR "Mistral AI" OR "Llama 3" OR "GPT-4o" OR "Claude 3")')
TIMESPAN = "1h"          # run hourly
MAXRECORDS = 250         # DOC 2.0 ArtList max

def stable_id(url: str) -> str:
    return hashlib.sha1(url.strip().lower().encode("utf-8")).hexdigest()

def fetch_gdelt_artlist():
    base = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": QUERY,
        "mode": "artlist",
        "timespan": TIMESPAN,
        "maxrecords": str(MAXRECORDS),
        "format": "json"
    }
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    arts = r.json().get("articles", [])
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
    return pd.DataFrame(rows)

def write_daily_parquet(df: pd.DataFrame):
    if df.empty: return []
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
    """Keep a list of the most recent 30 daily parquet file URLs for the front-end."""
    os.makedirs("manifests", exist_ok=True)
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
    df = fetch_gdelt_artlist()
    wrote = write_daily_parquet(df)
    update_manifest()
    print("Parquet updated:", wrote)
