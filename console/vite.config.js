import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The control plane sends no CORS headers, so the console reaches its HTTP endpoints
// through this proxy rather than the backend being changed to accommodate a browser.
// WebSockets are not subject to CORS and connect directly.
const CONTROL = 'http://127.0.0.1:4200';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    strictPort: true,
    proxy: {
      '/approve': { target: CONTROL, changeOrigin: false },
      '/reset': { target: CONTROL, changeOrigin: false },
      '/health': { target: CONTROL, changeOrigin: false },
    },
  },
});
