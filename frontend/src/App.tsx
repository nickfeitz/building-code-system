import { useState } from "react";
import { Sidebar, type PanelKey } from "./components/Sidebar";
import { DashboardPanel } from "./panels/DashboardPanel";
import { ChatPanel } from "./panels/ChatPanel";
import { BrowserPanel } from "./panels/BrowserPanel";
import { ImportPanel } from "./panels/ImportPanel";
import { QuarantinePanel } from "./panels/QuarantinePanel";

const TITLES: Record<PanelKey, string> = {
  dashboard: "Dashboard",
  chat: "Ask the Code",
  browser: "Code Browser",
  import: "Import",
  quarantine: "Quarantine Review",
};

export default function App() {
  const [panel, setPanel] = useState<PanelKey>("dashboard");

  return (
    <div className="flex h-full">
      <Sidebar active={panel} onSelect={setPanel} />
      <main className="flex-1 flex flex-col min-w-0">
        <header className="h-14 flex items-center px-6 border-b border-surface-400 bg-surface-800">
          <h1 className="text-lg font-semibold text-white">{TITLES[panel]}</h1>
        </header>
        <div className="flex-1 overflow-hidden">
          {panel === "dashboard" && <DashboardPanel />}
          {panel === "chat" && <ChatPanel />}
          {panel === "browser" && <BrowserPanel />}
          {panel === "import" && <ImportPanel />}
          {panel === "quarantine" && <QuarantinePanel />}
        </div>
      </main>
    </div>
  );
}
