import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  base: process.env.VITE_BASE ?? '/',     // set to '/<repo>/' on Pages
  build: { outDir: 'docs', emptyOutDir: false }, // don’t wipe data in docs/
  plugins: [react()],
})
