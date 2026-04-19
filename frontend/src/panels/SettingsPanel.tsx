import { useTheme, type ThemeChoice } from "../hooks/useTheme";

const OPTIONS: { value: ThemeChoice; label: string; hint: string }[] = [
  { value: "system", label: "System", hint: "Match OS preference" },
  { value: "light", label: "Light", hint: "Always light" },
  { value: "dark", label: "Dark", hint: "Always dark" },
];

export function SettingsPanel() {
  const { choice, effective, setTheme } = useTheme();

  return (
    <div className="h-full overflow-y-auto p-6 space-y-6 max-w-2xl">
      <section className="card space-y-4">
        <div>
          <h2 className="text-base font-semibold text-surface-50">Appearance</h2>
          <p className="text-xs text-surface-100 mt-1">
            Currently showing:{" "}
            <span className="text-surface-50 font-medium capitalize">
              {effective}
            </span>
          </p>
        </div>
        <div className="grid grid-cols-3 gap-2">
          {OPTIONS.map((o) => {
            const selected = choice === o.value;
            return (
              <button
                key={o.value}
                onClick={() => setTheme(o.value)}
                className={`rounded border px-3 py-3 text-left transition-colors ${
                  selected
                    ? "border-accent bg-accent/10"
                    : "border-surface-400 hover:bg-surface-700"
                }`}
              >
                <div className="text-sm font-medium text-surface-50">
                  {o.label}
                </div>
                <div className="text-xs text-surface-100 mt-0.5">{o.hint}</div>
              </button>
            );
          })}
        </div>
      </section>

      <section className="card space-y-3">
        <h2 className="text-base font-semibold text-surface-50">About</h2>
        <dl className="text-sm grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
          <dt className="text-surface-100">App</dt>
          <dd className="text-surface-50">Building Code Intelligence System</dd>
          <dt className="text-surface-100">Frontend</dt>
          <dd className="text-surface-50">React + Vite + Tailwind</dd>
        </dl>
      </section>
    </div>
  );
}
