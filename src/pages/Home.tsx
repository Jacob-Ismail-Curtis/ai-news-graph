// src/pages/Home.tsx
import React, { useEffect, useState } from "react";
import { getDuckConn } from "../lib/duckdb";
import NewsGraph, { NewsNode } from "../components/NewsGraph";

const MANIFEST_URL = "/manifests/index.json";

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

const Home: React.FC = () => {
  const [nodes, setNodes] = useState<NewsNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);

        const manifestResp = await fetch(MANIFEST_URL);
        if (!manifestResp.ok) throw new Error(`Manifest HTTP ${manifestResp.status}`);
        const manifest = await manifestResp.json();
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

        // Select minimal columns; DuckDB will fetch only needed byte ranges
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

        // toArray() → array of rows (tuples), strongly type them:
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
        <p className="text-sm text-zinc-500">
          Reading Parquet over HTTP with DuckDB-WASM (last 7 days).
        </p>
      </header>

      {loading && <p className="text-zinc-400">Loading…</p>}
      {error && <p className="text-red-500">Error: {error}</p>}
      {!loading && !error && nodes.length === 0 && (
        <p className="text-zinc-400">
          No data yet. Did your GitHub Action write <code>/manifests/index.json</code> and daily Parquet files?
        </p>
      )}
      {nodes.length > 0 && <NewsGraph nodes={nodes} />}
    </main>
  );
};

export default Home;
