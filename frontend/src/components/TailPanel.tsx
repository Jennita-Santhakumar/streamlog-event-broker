import { useEffect, useRef, useState } from "react";
import { wsBaseURL } from "../api/client";
import type { TailEvent } from "../types";

export default function TailPanel({ topic }: { topic: string | null }) {
  const [events, setEvents] = useState<TailEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const socketRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    setEvents([]);
    if (!topic) return;

    const ws = new WebSocket(`${wsBaseURL()}/api/v1/events/${topic}/ws`);
    socketRef.current = ws;
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onmessage = (msg) => {
      const evt: TailEvent = JSON.parse(msg.data);
      setEvents((prev) => [evt, ...prev].slice(0, 100));
    };

    return () => ws.close();
  }, [topic]);

  if (!topic) {
    return (
      <div className="panel">
        <h2>Live Tail</h2>
        <p>Select a topic from the Topics tab to start tailing it in real time.</p>
      </div>
    );
  }

  return (
    <div className="panel">
      <h2>
        Live Tail: {topic} <span className={connected ? "status-ok" : "status-down"}>{connected ? "connected" : "disconnected"}</span>
      </h2>
      <ul className="tail-list">
        {events.map((e, i) => (
          <li key={i}>
            <code>
              p{e.partition}@{e.offset} key={e.key || "(none)"} {JSON.stringify(e.value)}
            </code>
          </li>
        ))}
      </ul>
    </div>
  );
}
