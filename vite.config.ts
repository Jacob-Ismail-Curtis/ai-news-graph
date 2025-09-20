import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  // Set VITE_BASE=/ai-news-graph/ in prod build so assets & routes work on GH Pages
  base: process.env.VITE_BASE ?? '/',
  build: {
    outDir: 'docs',
    emptyOutDir: false, // keep docs/parquet & docs/manifests
  },
  plugins: [react()],
})
