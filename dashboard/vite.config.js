import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/auth": { target: "http://localhost:9100", changeOrigin: true },
      "/buckets": { target: "http://localhost:9100", changeOrigin: true },
      "/search": { target: "http://localhost:9100", changeOrigin: true },
      "/cluster": { target: "http://localhost:9100", changeOrigin: true },
      "/ring":    { target: "http://localhost:9100", changeOrigin: true },
    },
  },
});
