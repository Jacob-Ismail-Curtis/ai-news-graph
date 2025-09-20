// scripts/copy-duckdb-assets.mjs
// Copy a MATCHED worker+wasm pair from @duckdb/duckdb-wasm to public/duckdb.
// Works across versions by probing common filenames.

import fs from "node:fs";
import path from "node:path";
import url from "node:url";

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const srcDir = path.join(root, "node_modules", "@duckdb", "duckdb-wasm", "dist");
const outDir = path.join(root, "public", "duckdb");

function exists(p) { return fs.existsSync(p); }
function must(p) { if (!exists(p)) throw new Error(`[copy-duckdb-assets] missing: ${p}`); }

function pickPair() {
  const pairs = [
    // prefer MVP (single-thread; no COOP/COEP)
    { worker: "duckdb-browser-mvp.worker.js", wasm: "duckdb-browser-mvp.wasm", tag: "mvp" },
    { worker: "duckdb-mvp.worker.js",        wasm: "duckdb-mvp.wasm",        tag: "mvp" },
    // fall back to EH (still single-thread)
    { worker: "duckdb-browser-eh.worker.js", wasm: "duckdb-browser-eh.wasm", tag: "eh" },
    { worker: "duckdb-eh.worker.js",         wasm: "duckdb-eh.wasm",         tag: "eh" },
  ];
  for (const p of pairs) {
    const w = path.join(srcDir, p.worker);
    const m = path.join(srcDir, p.wasm);
    if (exists(w) && exists(m)) return { w, m, tag: p.tag };
  }
  return null;
}

(function main() {
  must(srcDir);
  fs.mkdirSync(outDir, { recursive: true });

  const pair = pickPair();
  if (!pair) {
    console.error("[copy-duckdb-assets] Could not find a matching DuckDB worker+wasm pair in dist/.");
    process.exit(1);
  }

  // Canonical names so app code is stable
  const dstWorker = path.join(outDir, "duckdb-worker.js");
  const dstWasm   = path.join(outDir, "duckdb.wasm");

  fs.copyFileSync(pair.w, dstWorker);
  fs.copyFileSync(pair.m, dstWasm);

  const manifest = {
    chosen: pair.tag,
    source: { worker: path.basename(pair.w), wasm: path.basename(pair.m) },
    copied: { worker: "duckdb-worker.js", wasm: "duckdb.wasm" }
  };
  fs.writeFileSync(path.join(outDir, "manifest.json"), JSON.stringify(manifest, null, 2));

  console.log(`[copy-duckdb-assets] Using ${pair.tag} bundle`);
  console.log(`[copy-duckdb-assets] Copied ${path.basename(pair.w)} → public/duckdb/duckdb-worker.js`);
  console.log(`[copy-duckdb-assets] Copied ${path.basename(pair.m)} → public/duckdb/duckdb.wasm`);
})();
