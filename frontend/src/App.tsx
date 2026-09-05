import { useState } from "react";
import TopicsPanel from "./components/TopicsPanel";
import LagPanel from "./components/LagPanel";
import DLQPanel from "./components/DLQPanel";
import TailPanel from "./components/TailPanel";

type Tab = "topics" | "lag" | "dlq" | "tail";

export default function App() {
  const [tab, setTab] = useState<Tab>("topics");
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);

  return (
    <div className="app">
      <header>
        <h1>Event Streaming Broker</h1>
      </header>
      <nav>
        <button className={tab === "topics" ? "active" : ""} onClick={() => setTab("topics")}>
          Topics
        </button>
        <button className={tab === "lag" ? "active" : ""} onClick={() => setTab("lag")}>
          Consumer Lag
        </button>
        <button className={tab === "dlq" ? "active" : ""} onClick={() => setTab("dlq")}>
          DLQ
        </button>
        <button
          className={tab === "tail" ? "active" : ""}
          onClick={() => setTab("tail")}
        >
          Live Tail
        </button>
      </nav>
      <main>
        {tab === "topics" && (
          <TopicsPanel
            onSelectTopic={(name) => {
              setSelectedTopic(name);
              setTab("tail");
            }}
          />
        )}
        {tab === "lag" && <LagPanel />}
        {tab === "dlq" && <DLQPanel />}
        {tab === "tail" && <TailPanel topic={selectedTopic} />}
      </main>
    </div>
  );
}
