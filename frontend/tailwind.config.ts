import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        // Surface scale roughly mirroring the previous htmx UI's #1a1a1a / #0d0d0d / #333
        surface: {
          900: "#0a0a0a",
          800: "#111111",
          700: "#1a1a1a",
          600: "#222222",
          500: "#2a2a2a",
          400: "#333333",
          300: "#444444",
          200: "#666666",
          100: "#999999",
          50: "#cccccc",
        },
        accent: {
          DEFAULT: "#3b82f6", // blue-500
          hover: "#2563eb",   // blue-600
        },
        success: "#22c55e",
        warn: "#f59e0b",
        danger: "#ef4444",
      },
      fontFamily: {
        sans: [
          "-apple-system",
          "BlinkMacSystemFont",
          "Segoe UI",
          "Roboto",
          "sans-serif",
        ],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
    },
  },
  plugins: [],
} satisfies Config;
