"use strict";

// Pure helpers for the trace-graph view (trace-graph.html).
//
// The `/api/trace-graph` endpoint streams a service-architecture graph over
// Server-Sent Events: nodes are the `crate::module`s that appear in the sampled
// stacks, edges are the observed caller->callee transitions (weighted), each
// node annotated with total vs off-CPU samples. The server refines as source
// files fold and pushes a fresh full graph per event; the client re-renders on
// every event.
//
// These functions are factored out (and CommonJS-exported) so they can be
// unit-tested under Node without a browser DOM or ELK. In the browser they
// attach as globals via the top-level `function`/`const` declarations.

// ── Latency model (wall-clock sampling) ─────────────────────────────────────
// dial9 is a wall-clock sampler: total observed wall time = span x workers, so
// one sample represents ~ span*workers/totalSamples of wall time (~= the sample
// period). A module's wall time = its samples x msPerSample; dividing by the
// request count (when known) yields per-request latency.

function msPerSample(meta) {
  const durMs = (meta.time_span_ns || 0) / 1e6;
  const workers = Math.max(1, meta.worker_count || 1);
  const total = Math.max(1, meta.total_samples || 0);
  return (durMs * workers) / total;
}

// Per-node wall-ms (or per-request ms when meta.requests > 1).
function nodeWallMs(node, meta) {
  const per = msPerSample(meta);
  const reqs = Math.max(1, meta.requests || 1);
  return (node.samples * per) / reqs;
}

function nodeBlockedMs(node, meta) {
  const per = msPerSample(meta);
  const reqs = Math.max(1, meta.requests || 1);
  return (node.off_cpu * per) / reqs;
}

// Heat color for a node's wall-ms: >5ms red, 1-5ms amber, else green.
const HEAT_RED = "#e5484d";
const HEAT_AMBER = "#ffcc56";
const HEAT_GREEN = "#25e192";
function latHeat(ms) {
  return ms > 5 ? HEAT_RED : ms >= 1 ? HEAT_AMBER : HEAT_GREEN;
}

// Compact ms formatter: >=1ms shows ms (1 decimal), else microseconds.
function fmtMs(ms) {
  if (!(ms > 0)) return "0";
  return ms >= 1 ? `${ms.toFixed(1)}ms` : `${Math.round(ms * 1000)}µs`;
}

// ── Crate cluster color ─────────────────────────────────────────────────────
// Deterministic per-crate color so a subsystem keeps its color across renders.
// `runtime` (the collapsed non-app node) is a muted grey.
const CRATE_PALETTE = [
  "#23c4f8", // cyan
  "#a78bfa", // purple
  "#25e192", // green
  "#4e74f8", // blue
  "#f24769", // pink
  "#ffcc56", // amber
];
function crateColor(crateName) {
  if (crateName === "runtime") return "#52525b";
  let h = 0;
  for (let i = 0; i < crateName.length; i++) h = (h * 31 + crateName.charCodeAt(i)) >>> 0;
  return CRATE_PALETTE[h % CRATE_PALETTE.length];
}

// ── View filters ────────────────────────────────────────────────────────────
// Drop the collapsed `runtime` node, keep only edges at/above the min weight
// between surviving nodes, then drop nodes left with no surviving edge. Mirrors
// the trace-graph render filter: the runtime node is always excluded from the
// drawn graph (it's the "gap" between app modules), weak edges are noise.
function filterGraph(graph, opts) {
  const o = opts || {};
  const hideRuntime = o.hideRuntime !== false; // default true
  // Default 1 (keep every observed transition): dial9's on-demand fold produces
  // far smaller per-snapshot sample counts than a batch export, so a high
  // threshold (IRIS used 8, tuned for 80-leaf batches) empties the graph. The
  // slider still lets you raise it to declutter a large capture.
  const minEdge = o.minEdge != null ? o.minEdge : 1;

  const keepNode = (n) => !(hideRuntime && n.id === "runtime");
  let nodes = (graph.nodes || []).filter(keepNode);
  const ids = new Set(nodes.map((n) => n.id));
  const edges = (graph.edges || []).filter(
    (e) => e.count >= minEdge && ids.has(e.source) && ids.has(e.target),
  );
  // Drop disconnected nodes once any filtering is active, so the view isn't a
  // field of orphan boxes.
  if (minEdge > 1 || hideRuntime) {
    const connected = new Set();
    for (const e of edges) {
      connected.add(e.source);
      connected.add(e.target);
    }
    nodes = nodes.filter((n) => connected.has(n.id));
  }
  return { nodes, edges };
}

// ── ELK layout input ────────────────────────────────────────────────────────
// Build the ELK graph: a layered top-down layout, clustering nodes into per-crate
// compound (group) nodes so subsystems read as groups. Node dimensions and
// spacing constants match the reference architecture renderer.
const NODE_W = 250;
const NODE_H = 56;

function buildElkGraph(graph) {
  const byCrate = new Map();
  for (const n of graph.nodes) {
    const arr = byCrate.get(n.crate_name) || [];
    arr.push(n);
    byCrate.set(n.crate_name, arr);
  }
  const children = [...byCrate.entries()].map(([crate, mods]) => ({
    id: `crate:${crate}`,
    layoutOptions: { "elk.padding": "[top=44,left=18,bottom=18,right=18]" },
    children: mods.map((n) => ({ id: n.id, width: NODE_W, height: NODE_H })),
  }));
  return {
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "DOWN",
      "elk.layered.spacing.nodeNodeBetweenLayers": "60",
      "elk.spacing.nodeNode": "34",
      "elk.hierarchyHandling": "INCLUDE_CHILDREN",
    },
    children,
    edges: graph.edges.map((e, i) => ({
      id: `e${i}`,
      sources: [e.source],
      targets: [e.target],
    })),
  };
}

// Edge visual weight: stroke width + opacity scaled by relative count.
function edgeStyle(count, maxCount) {
  const frac = maxCount > 0 ? count / maxCount : 0;
  return {
    width: 1 + 2.5 * frac,
    opacity: 0.2 + 0.6 * frac,
  };
}

// ── API URL ─────────────────────────────────────────────────────────────────
// Build the /api/trace-graph URL from the page's scope params (mirrors the
// flamegraph page's buildApiUrl). `params` is a URLSearchParams; `origin` the
// page origin. `appPrefixes` (array) and `requests` (number) are optional.
function buildTraceGraphUrl(origin, params, extra) {
  const e = extra || {};
  const u = new URL("/api/trace-graph", origin);
  const pass = ["bucket", "prefix", "service", "start_ns", "end_ns", "max_files"];
  for (const k of pass) {
    const v = params.get(k);
    if (v) u.searchParams.set(k, v);
  }
  for (const h of params.getAll("host")) u.searchParams.append("host", h);
  for (const p of e.appPrefixes || []) u.searchParams.append("app_prefix", p);
  if (e.requests && e.requests > 1) u.searchParams.set("requests", String(e.requests));
  return u;
}

const TraceGraphApi = {
  msPerSample,
  nodeWallMs,
  nodeBlockedMs,
  latHeat,
  fmtMs,
  crateColor,
  filterGraph,
  buildElkGraph,
  edgeStyle,
  buildTraceGraphUrl,
  NODE_W,
  NODE_H,
};

if (typeof module !== "undefined" && module.exports) {
  module.exports = TraceGraphApi;
} else if (typeof window !== "undefined") {
  window.TraceGraphApi = TraceGraphApi;
}
