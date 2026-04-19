import { useHealth } from "../hooks/useHealth";
import { useTheme } from "../hooks/useTheme";

export type PanelKey =
  | "dashboard"
  | "chat"
  | "browser"
  | "catalog"
  | "import"
  | "quarantine"
  | "settings";

interface NavItem {
  key: PanelKey;
  label: string;
  icon: string;
  group: "Main" | "Management" | "System";
}

const NAV: NavItem[] = [
  { key: "dashboard", label: "Dashboard", icon: "📊", group: "System" },
  { key: "settings", label: "Settings", icon: "⚙️", group: "System" },
  { key: "chat", label: "Chat", icon: "💬", group: "Main" },
  { key: "browser", label: "Code Browser", icon: "📚", group: "Main" },
  { key: "catalog", label: "Catalog", icon: "📖", group: "Management" },
  { key: "import", label: "Import", icon: "📥", group: "Management" },
  { key: "quarantine", label: "Quarantine", icon: "🛡️", group: "Management" },
];

export function Sidebar({
  active,
  onSelect,
}: {
  active: PanelKey;
  onSelect: (k: PanelKey) => void;
}) {
  const health = useHealth();
  const { effective } = useTheme();
  const status = health.data?.status;
  const dot =
    status === "healthy"
      ? "bg-success"
      : status === "degraded"
        ? "bg-warn"
        : "bg-danger";

  const groups: NavItem["group"][] = ["System", "Main", "Management"];

  // The "light" mark (hat only) reads well on dark surfaces; the "dark"
  // mark (hat + black gear) pops on light surfaces. So we swap to the
  // inverse of the effective theme.
  const logoSrc =
    effective === "dark" ? "/favicon-light.svg" : "/favicon-dark.svg";

  return (
    <aside className="w-60 bg-surface-900 border-r border-surface-400 flex flex-col">
      <div className="px-5 py-4 border-b border-surface-400 flex items-center gap-3">
        <img src={logoSrc} alt="" className="w-9 h-9 shrink-0" />
        <div>
          <div className="text-base font-semibold text-surface-50 leading-tight">
            Building Code
          </div>
          <div className="text-xs text-surface-100 leading-tight">
            Intelligence System
          </div>
        </div>
      </div>

      <nav className="flex-1 overflow-y-auto py-2">
        {groups.map((g) => (
          <div key={g} className="mb-2">
            <div className="px-5 py-1 text-xs uppercase tracking-wider text-surface-100">
              {g}
            </div>
            {NAV.filter((n) => n.group === g).map((n) => {
              const isActive = active === n.key;
              return (
                <button
                  key={n.key}
                  onClick={() => onSelect(n.key)}
                  className={`w-full flex items-center gap-3 px-5 py-2 text-sm text-left transition-colors ${
                    isActive
                      ? "bg-accent/20 text-surface-50 border-l-2 border-accent"
                      : "text-surface-50 hover:bg-surface-800 border-l-2 border-transparent"
                  }`}
                >
                  <span className="text-base">{n.icon}</span>
                  <span>{n.label}</span>
                </button>
              );
            })}
          </div>
        ))}
      </nav>

      <div className="px-5 py-3 border-t border-surface-400 text-xs flex items-center gap-2">
        <span className={`w-2 h-2 rounded-full ${dot}`} />
        <span className="text-surface-50">
          {health.isLoading ? "checking…" : status ?? "offline"}
        </span>
      </div>
    </aside>
  );
}
