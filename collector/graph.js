'use strict';

// The dependency graph is observed, not declared. Edges come from the pri-0 edge counters
// the services are already emitting, and an edge that stops being reported decays out.
// That is what makes "isolate auth and the edge disappears within ten seconds" a
// demonstrable property rather than a claim about a hardcoded diagram.

const DECAY_WINDOWS = 10;

class Graph {
  constructor() {
    this.edges = new Map();   // "from→to" -> lastSeenWindowId
  }

  observe(windowId, edgeKeys) {
    for (const key of edgeKeys) this.edges.set(key, windowId);
    for (const [key, lastSeen] of this.edges) {
      if (windowId - lastSeen >= DECAY_WINDOWS) this.edges.delete(key);
    }
  }

  // In-degree per node: how many distinct callers depend on it. P2's scorer penalises
  // high in-degree, so this has to be observed rather than assumed.
  inDegree() {
    const counts = {};
    for (const key of this.edges.keys()) {
      const to = key.split('→')[1];
      counts[to] = (counts[to] ?? 0) + 1;
    }
    return counts;
  }

  snapshot() {
    return [...this.edges.keys()];
  }

  reset() {
    this.edges.clear();
  }
}

module.exports = { Graph, DECAY_WINDOWS };
