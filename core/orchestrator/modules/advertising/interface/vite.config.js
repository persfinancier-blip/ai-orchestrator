import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import path from 'path';

export default defineConfig({
  base: '/ai-orchestrator/',
  plugins: [svelte()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname),
      '@components': path.resolve(__dirname, 'components'),
      '@stores': path.resolve(__dirname, 'stores'),
    },
  },
  server: {
    port: 3000,
    open: true,
    proxy: {
      '/ai-orchestrator/api': {
        target: process.env.SPACE_API_URL || 'http://localhost:8787',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
    sourcemap: true,
  },
});
