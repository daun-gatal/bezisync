import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { fileURLToPath, URL } from 'node:url';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) } },
  server: { proxy: { '/api': 'http://localhost:8000', '/.well-known/oauth-': 'http://localhost:8000' } },
  build: { sourcemap: true, chunkSizeWarningLimit: 900 },
  // Dialog tests drive many steps through jsdom: on a loaded machine one can pass 5 s for no
  // reason in the code, so they get 15 s.
  test: { environment: 'jsdom', globals: true, setupFiles: ['./src/test/setup.ts'], css: false, testTimeout: 15_000 },
});
