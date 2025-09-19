// src/lib/duckdb.ts
import * as duckdb from "@duckdb/duckdb-wasm";
import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

/**
 * Returns a ready-to-use DuckDB-WASM connection with httpfs enabled.
 */
export async function getDuckConn(): Promise<AsyncDuckDBConnection> {
  // Use official CDN bundles (works well with Vite + GitHub Pages)
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  if (!bundle.mainWorker || !bundle.mainModule) {
    throw new Error("No suitable DuckDB-WASM bundle found for this browser.");
  }

  // Worker: cross-origin → classic worker is fine
  const worker =
    bundle.mainWorker.startsWith("http")
      ? new Worker(bundle.mainWorker)
      : new Worker(new URL(bundle.mainWorker, import.meta.url), { type: "module" });

  // ⚠️ Constructor is (logger, worker) — not (worker, logger)
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);

  // Instantiate the WASM module
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker ?? undefined);

  const conn = await db.connect();
  await conn.query("INSTALL httpfs; LOAD httpfs; SET enable_http_metadata_cache=true;");
  return conn;
}
