import {
  createContext,
  createElement,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";

export type ThemeChoice = "light" | "dark" | "system";
export type EffectiveTheme = "light" | "dark";

const STORAGE_KEY = "theme";

const readChoice = (): ThemeChoice => {
  const v = localStorage.getItem(STORAGE_KEY);
  return v === "light" || v === "dark" ? v : "system";
};

const systemPrefersDark = () =>
  window.matchMedia("(prefers-color-scheme: dark)").matches;

const resolve = (c: ThemeChoice): EffectiveTheme =>
  c === "system" ? (systemPrefersDark() ? "dark" : "light") : c;

const applyToDocument = (eff: EffectiveTheme) => {
  document.documentElement.classList.toggle("dark", eff === "dark");
};

interface ThemeContextValue {
  choice: ThemeChoice;
  effective: EffectiveTheme;
  setTheme: (c: ThemeChoice) => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [choice, setChoice] = useState<ThemeChoice>(readChoice);
  const [effective, setEffective] = useState<EffectiveTheme>(() =>
    resolve(readChoice()),
  );

  useEffect(() => {
    const eff = resolve(choice);
    setEffective(eff);
    applyToDocument(eff);
    if (choice === "system") localStorage.removeItem(STORAGE_KEY);
    else localStorage.setItem(STORAGE_KEY, choice);
  }, [choice]);

  useEffect(() => {
    if (choice !== "system") return;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const onChange = () => {
      const eff: EffectiveTheme = mq.matches ? "dark" : "light";
      setEffective(eff);
      applyToDocument(eff);
    };
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, [choice]);

  const setTheme = useCallback((c: ThemeChoice) => setChoice(c), []);

  return createElement(
    ThemeContext.Provider,
    { value: { choice, effective, setTheme } },
    children,
  );
}

export function useTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used within ThemeProvider");
  return ctx;
}
