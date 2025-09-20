// src/lib/duckdb.ts
import * as duckdb from "@duckdb/duckdb-wasm";
import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

/**
 * Uses the official CDN bundle but wraps the worker code in a Blob so the
 * Worker is same-origin (no cross-origin Worker restriction).
 */
export async function getDuckConn(): Promise<AsyncDuckDBConnection> {
  // Pick a bundle (mvp/eh) compatible with the browser
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  if (!bundle.mainWorker || !bundle.mainModule) {
    throw new Error("No suitable DuckDB-WASM bundle found for this browser.");
  }

  // 1) Fetch worker script text from CDN (CORS permitted by jsDelivr)
  const workerCode = await fetch(bundle.mainWorker).then(async (r) => {
    if (!r.ok) throw new Error(`Failed to fetch DuckDB worker: ${r.status}`);
    return await r.text();
  });

  // 2) Create a Blob URL (same-origin) and spawn the Worker from it
  const blob = new Blob([workerCode], { type: "text/javascript" });
  const workerUrl = URL.createObjectURL(blob);
  const worker = new Worker(workerUrl); // classic worker

  // 3) Create DB and instantiate with the wasm module URL (CDN is fine)
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker ?? undefined);

  const conn = await db.connect();
  await conn.query("INSTALL httpfs; LOAD httpfs; SET enable_http_metadata_cache=true;");
  return conn;
}
