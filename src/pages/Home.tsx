// src/pages/Home.tsx
import { useEffect, useState } from "react";
import NewsGraph, { type NewsNode } from "../components/NewsGraph";
import { getDuckConn } from "../lib/duckdb";

const MANIFEST_URL =
  import.meta.env.VITE_MANIFEST_URL ??
  new URL(`${import.meta.env.BASE_URL}manifests/index.json`, window.location.href).toString();

/** Normalize any manifest entry into an absolute http(s) URL DuckDB can fetch. */
function resolveFileUrl(u: string): string {
  if (!u) return u;
  if (/^https?:\/\//i.test(u)) return u; // already absolute
  const origin = window.location.origin; // http://localhost:5173 or https://<user>.github.io
  if (u.startsWith("/")) return origin + u; // root-relative → absolute
  const base = new URL(import.meta.env.BASE_URL, origin).toString();
  return new URL(u, base).toString(); // relative → resolved against base
}

/** Safe string coercion for values coming from DuckDB. */
function asString(v: unknown): string {
  if (typeof v === "string") return v;
  if (v == null) return "";
  try {
    return String(v);
  } catch {
    return "";
  }
}

/** Tiny topic heuristic; you can move real tagging into ingestion later. */
function guessTopic(title: string) {
  const t = title.toLowerCase();
  if (t.includes("policy") || t.includes("regulat")) return "policy";
  if (t.includes("agent")) return "agents";
  if (t.includes("robot")) return "robotics";
  if (t.includes("safety") || t.includes("alignment")) return "safety";
  if (t.includes("chip") || t.includes("nvidia")) return "chips";
  if (t.includes("openai") || t.includes("anthropic") || t.includes("deepmind")) return "frontier";
  return "other";
}

export default function Home() {
  const [nodes, setNodes] = useState<NewsNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);

        console.log("Fetching manifest from:", MANIFEST_URL);
        const resp = await fetch(MANIFEST_URL);
        const ct = resp.headers.get("content-type") || "";
        if (!resp.ok || !ct.includes("application/json")) {
          const sample = await resp.text().catch(() => "");
          throw new Error(
            `Manifest is not JSON at ${MANIFEST_URL}. status=${resp.status} content-type=${ct} sample="${sample.slice(
              0,
              120
            )}"`
          );
        }
        const manifest = await resp.json();
        const rawFiles: string[] = Array.isArray(manifest.files) ? manifest.files : [];
        const files = rawFiles.map(resolveFileUrl);
        console.log("Manifest files (resolved):", files);

        if (!files.length) {
          setNodes([]);
          return;
        }

        const conn = await getDuckConn();
        await conn.query("SET TimeZone='UTC';");

        // Escape single quotes in URLs
        const urlLiterals = files.map((u) => `'${u.replace(/'/g, "''")}'`).join(",");
        await conn.query(`
          CREATE OR REPLACE VIEW articles AS
          SELECT * FROM read_parquet([${urlLiterals}]);
        `);

        // Use date_diff for epoch ms and compare with INTERVAL '7' DAY.
        const result = await conn.query(`
          WITH t AS (
            SELECT
              id::STRING                      AS id,
              url::STRING                     AS url,
              title::STRING                   AS title,
              CAST(published_at AS TIMESTAMP) AS ts,
              domain::STRING                  AS domain,
              language::STRING                AS language
            FROM articles
          )
          SELECT
            id,
            url,
            title,
            date_diff('millisecond', TIMESTAMP '1970-01-01 00:00:00', ts) AS published_ms,
            domain,
            language
          FROM t
          WHERE ts >= NOW() - INTERVAL '7' DAY
        `);

        // Rows: [id, url, title, published_ms, domain, language]
        type Row = [unknown, unknown, unknown, unknown, unknown, unknown];
        const rows = (result.toArray() as unknown) as Row[];

        const now = Date.now();
        const out: NewsNode[] = rows.map(([id, url, title, published_ms, domain, language]) => {
          // Coerce types defensively
          const idStr = asString(id);
          const urlStr = asString(url);
          const titleStr = asString(title);
          const domainStr = domain == null ? null : asString(domain);
          const langStr = language == null ? null : asString(language);

          let msNum: number | null = null;
          if (typeof published_ms === "number") {
            msNum = published_ms;
          } else if (typeof published_ms === "bigint") {
            msNum = Number(published_ms);
          } else if (published_ms != null) {
            const n = Number(published_ms as any);
            msNum = Number.isFinite(n) ? n : null;
          }

          const date = typeof msNum === "number" ? new Date(msNum) : null;
          const ageHours = date ? (now - date.getTime()) / 36e5 : 999;
          const val = Math.max(1, 10 - ageHours / 6); // newer → bigger

          return {
            id: idStr,
            url: urlStr,
            title: titleStr || null,
            published_at: date ? date.toISOString() : null,
            domain: domainStr,
            language: langStr,
            topic: guessTopic(titleStr),
            val,
          };
        });

        setNodes(out);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  return (
    <main id="main" className="max-w-6xl mx-auto px-4 py-8">
      <header className="mb-6">
        <h1 className="text-2xl font-semibold">AI News Explorer</h1>
        <p className="text-sm text-zinc-500">Reading Parquet over HTTP with DuckDB-WASM (last 7 days).</p>
      </header>

      {loading && <p className="text-zinc-400">Loading…</p>}
      {error && (
        <div className="text-red-500 text-sm">
          <p>Error: {error}</p>
          <p className="mt-1">
            Tip: open <code>{MANIFEST_URL}</code> in a new tab — it must be JSON and list full HTTP(S) URLs to your
            parquet files.
          </p>
        </div>
      )}
      {!loading && !error && nodes.length === 0 && (
        <p className="text-zinc-400">
          No data yet. Check <code>{MANIFEST_URL}</code> and ensure the <code>files</code> array contains valid URLs.
        </p>
      )}
      {nodes.length > 0 && <NewsGraph nodes={nodes} />}
    </main>
  );
}
