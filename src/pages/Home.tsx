// src/pages/Home.tsx
import { useEffect, useState } from "react";
import NewsGraph, { type NewsNode } from "../components/NewsGraph";
import { getDuckConn } from "../lib/duckdb";

const MANIFEST_URL =
  import.meta.env.VITE_MANIFEST_URL ??
  new URL(`${import.meta.env.BASE_URL}manifests/index.json`, window.location.href).toString();

// simple topic heuristic; you can move this to ingest later
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

        const resp = await fetch(MANIFEST_URL);
        const ct = resp.headers.get("content-type") || "";
        if (!resp.ok || !ct.includes("application/json")) {
          const sample = await resp.text().catch(() => "");
          throw new Error(
            `Manifest is not JSON at ${MANIFEST_URL}. status=${resp.status} content-type=${ct} sample="${sample.slice(0,80)}"`
          );
        }
        const manifest = await resp.json();
        const files: string[] = Array.isArray(manifest.files) ? manifest.files : [];
        if (!files.length) {
          setNodes([]);
          return;
        }

        const conn = await getDuckConn();
        const urlsSQL = files.map((u) => `'${u}'`).join(",");
        await conn.query(`
          CREATE OR REPLACE VIEW articles AS
          SELECT * FROM read_parquet([${urlsSQL}]);
        `);

        const result = await conn.query(`
          SELECT
            id::STRING as id,
            url::STRING as url,
            title::STRING as title,
            published_at::TIMESTAMPTZ as published_at,
            domain::STRING as domain,
            language::STRING as language
          FROM articles
          WHERE published_at >= now() - INTERVAL 7 DAY
        `);

        type Row = [string, string, string | null, Date | null, string | null, string | null];
        const rows = (result.toArray() as unknown) as Row[];

        const now = Date.now();
        const out: NewsNode[] = rows.map(([id, url, title, published, domain, language]) => {
          const ageHours = published ? (now - published.getTime()) / 36e5 : 999;
          const val = Math.max(1, 10 - ageHours / 6); // newer → bigger
          const topic = guessTopic(title ?? "");
          return {
            id,
            url,
            title,
            published_at: published ? published.toISOString() : null,
            domain,
            language,
            topic,
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
      {error && <p className="text-red-500">Error: {error}</p>}
      {!loading && !error && nodes.length === 0 && (
        <p className="text-zinc-400">No data yet. Check <code>/manifests/index.json</code>.</p>
      )}
      {nodes.length > 0 && <NewsGraph nodes={nodes} />}
    </main>
  );
}
