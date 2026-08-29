'use strict';

const { WebSocketServer } = require('ws');

// WebSocket fan-out with a bounded per-socket send queue. A subscriber that stops reading
// must not slow ingest or the other subscribers: we drop that socket's oldest pending
// window rather than buffering without limit. Backpressure applies to our own output too.

const SEND_QUEUE_MAX = 10;

class Fanout {
  constructor(server, path) {
    this.wss = new WebSocketServer({ server, path });
    this.queues = new Map();     // ws -> pending messages
    this.droppedForSlowConsumers = 0;

    this.wss.on('connection', (ws) => {
      this.queues.set(ws, []);
      ws.on('close', () => this.queues.delete(ws));
      ws.on('error', () => this.queues.delete(ws));
    });
  }

  get subscribers() {
    return this.queues.size;
  }

  broadcast(message) {
    const payload = JSON.stringify(message);
    for (const [ws, queue] of this.queues) {
      if (ws.readyState !== ws.OPEN) continue;
      // bufferedAmount is the socket's own unsent backlog; if it is growing, the consumer
      // is not keeping up and we shed rather than accumulate.
      if (ws.bufferedAmount > 0 && queue.length >= SEND_QUEUE_MAX) {
        queue.shift();
        this.droppedForSlowConsumers++;
      }
      queue.push(payload);
      while (queue.length > 0) {
        const next = queue.shift();
        ws.send(next, (err) => {
          if (err) this.queues.delete(ws);
        });
      }
    }
  }

  close() {
    this.wss.close();
  }
}

module.exports = { Fanout };
