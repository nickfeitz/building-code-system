import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // For local `npm run dev` against the dockerized backend
      "/api": "http://localhost:8010",
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
