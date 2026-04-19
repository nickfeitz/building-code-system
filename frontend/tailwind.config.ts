import type { Config } from "tailwindcss";

// Surface colours are driven by CSS variables (see styles.css) so the scale
// flips when the `dark` class is toggled on <html>. Values below are declared
// as `rgb(var(--name) / <alpha-value>)` so Tailwind opacity modifiers
// (e.g. `bg-accent/20`) continue to work.
const surfaceVar = (token: string) => `rgb(var(--${token}) / <alpha-value>)`;

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        surface: {
          900: surfaceVar("surface-900"),
          800: surfaceVar("surface-800"),
          700: surfaceVar("surface-700"),
          600: surfaceVar("surface-600"),
          500: surfaceVar("surface-500"),
          400: surfaceVar("surface-400"),
          300: surfaceVar("surface-300"),
          200: surfaceVar("surface-200"),
          100: surfaceVar("surface-100"),
          50: surfaceVar("surface-50"),
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
