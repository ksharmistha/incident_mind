import { useEffect, useRef, useState } from 'react';

// One socket, reconnecting with backoff. The console must never freeze or need a refresh
// during a demo: a dropped stream shows as stale and recovers on its own.
export function useSocket(url) {
  const [message, setMessage] = useState(null);
  const [connected, setConnected] = useState(false);
  const [lastAt, setLastAt] = useState(null);
  const backoff = useRef(500);
  const socket = useRef(null);
  const stopped = useRef(false);

  useEffect(() => {
    stopped.current = false;

    const open = () => {
      if (stopped.current) return;
      const ws = new WebSocket(url);
      socket.current = ws;

      ws.onopen = () => { backoff.current = 500; setConnected(true); };
      ws.onmessage = (ev) => {
        try {
          setMessage(JSON.parse(ev.data));
          setLastAt(Date.now());
        } catch {
          // A malformed frame is dropped rather than tearing down the view.
        }
      };
      ws.onclose = () => {
        setConnected(false);
        if (stopped.current) return;
        const wait = backoff.current;
        backoff.current = Math.min(backoff.current * 2, 5000);
        setTimeout(open, wait);
      };
      ws.onerror = () => ws.close();
    };

    open();
    return () => { stopped.current = true; socket.current?.close(); };
  }, [url]);

  return { message, connected, lastAt };
}
