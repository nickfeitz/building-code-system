import { useState } from "react";
import { Sidebar, type PanelKey } from "./components/Sidebar";
import { DashboardPanel } from "./panels/DashboardPanel";
import { ChatPanel } from "./panels/ChatPanel";
import { BrowserPanel } from "./panels/BrowserPanel";
import { CatalogPanel } from "./panels/CatalogPanel";
import { ImportPanel } from "./panels/ImportPanel";
import { QuarantinePanel } from "./panels/QuarantinePanel";
import { ReviewPanel } from "./panels/ReviewPanel";
import type { ReviewContext } from "./api/types";

const TITLES: Record<PanelKey, string> = {
  dashboard: "Dashboard",
  chat: "Ask the Code",
  browser: "Code Browser",
  catalog: "Code Catalog",
  import: "Import",
  quarantine: "Quarantine Review",
};

export default function App() {
  const [panel, setPanel] = useState<PanelKey>("dashboard");
  const [reviewCtx, setReviewCtx] = useState<ReviewContext | null>(null);

  // Entering review always lands on the catalog panel so "Back" returns
  // users to where they came from.
  const startReview = (ctx: ReviewContext) => {
    setPanel("catalog");
    setReviewCtx(ctx);
  };

  // Switching panels from the sidebar implicitly closes the reviewer.
  const selectPanel = (k: PanelKey) => {
    setReviewCtx(null);
    setPanel(k);
  };

  return (
    <div className="flex h-full">
      <Sidebar active={panel} onSelect={selectPanel} />
      <main className="flex-1 flex flex-col min-w-0">
        <header className="h-14 flex items-center px-6 border-b border-surface-400 bg-surface-800">
          <h1 className="text-lg font-semibold text-white">
            {reviewCtx ? "Image Review" : TITLES[panel]}
          </h1>
        </header>
        <div className="flex-1 overflow-hidden">
          {reviewCtx ? (
            <ReviewPanel
              context={reviewCtx}
              onClose={() => setReviewCtx(null)}
            />
          ) : (
            <>
              {panel === "dashboard" && <DashboardPanel />}
              {panel === "chat" && <ChatPanel />}
              {panel === "browser" && <BrowserPanel />}
              {panel === "catalog" && <CatalogPanel onReview={startReview} />}
              {panel === "import" && <ImportPanel />}
              {panel === "quarantine" && <QuarantinePanel />}
            </>
          )}
        </div>
      </main>
    </div>
  );
}
