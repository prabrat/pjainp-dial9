#!/usr/bin/env node
"use strict";

// Unit tests for the pure trace-graph view helpers in trace_graph_api.js:
// latency math (wall-clock sampling model), heat thresholds, ms formatting,
// crate coloring, the view filter (drop runtime / min-edge / drop disconnected),
// the ELK graph builder, and the API URL builder. These are the browser-free
// pieces of trace-graph.html, tested without a DOM or ELK.

const assert = require("assert");
const {
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
} = require("./trace_graph_api.js");

// 1. Latency model.
{
  // 1s span, 4 workers, 100 samples -> msPerSample = 1000*4/100 = 40ms.
  const meta = { time_span_ns: 1e9, worker_count: 4, total_samples: 100, requests: 1 };
  assert.strictEqual(msPerSample(meta), 40);
  assert.strictEqual(nodeWallMs({ samples: 10, off_cpu: 0 }, meta), 400);
  assert.strictEqual(nodeBlockedMs({ samples: 10, off_cpu: 2 }, meta), 80);
  // Per-request: divide by request count.
  const perReq = { time_span_ns: 1e9, worker_count: 4, total_samples: 100, requests: 8 };
  assert.strictEqual(nodeWallMs({ samples: 10 }, perReq), 50); // 400 / 8
  // Degenerate inputs: no NaN/Infinity.
  const zero = { time_span_ns: 0, worker_count: 0, total_samples: 0, requests: 0 };
  assert.ok(Number.isFinite(msPerSample(zero)));
  assert.ok(Number.isFinite(nodeWallMs({ samples: 5, off_cpu: 0 }, zero)));
}

// 2. Heat thresholds.
{
  assert.strictEqual(latHeat(10), "#e5484d", ">5ms red");
  assert.strictEqual(latHeat(5), "#ffcc56", "5ms amber (>=1)");
  assert.strictEqual(latHeat(1), "#ffcc56", "1ms amber");
  assert.strictEqual(latHeat(0.4), "#25e192", "<1ms green");
}

// 3. ms formatter.
{
  assert.strictEqual(fmtMs(2.5), "2.5ms");
  assert.strictEqual(fmtMs(0.25), "250µs");
  assert.strictEqual(fmtMs(0), "0");
}

// 4. Crate color: runtime is grey; others deterministic + stable.
{
  assert.strictEqual(crateColor("runtime"), "#52525b");
  assert.strictEqual(crateColor("metrics_service"), crateColor("metrics_service"));
  assert.ok(crateColor("metrics_service").startsWith("#"));
}

// 5. View filter: drop runtime, min-edge weight, drop disconnected.
{
  const graph = {
    nodes: [
      { id: "svc::a", crate_name: "svc", samples: 100, off_cpu: 0 },
      { id: "svc::b", crate_name: "svc", samples: 50, off_cpu: 0 },
      { id: "runtime", crate_name: "runtime", samples: 200, off_cpu: 200 },
      { id: "svc::orphan", crate_name: "svc", samples: 5, off_cpu: 0 },
    ],
    edges: [
      { source: "svc::a", target: "svc::b", count: 20 },
      { source: "svc::a", target: "runtime", count: 100 }, // to runtime → dropped
      { source: "svc::b", target: "svc::orphan", count: 3 }, // below minEdge 8 → dropped
    ],
  };
  const f = filterGraph(graph, { hideRuntime: true, minEdge: 8 });
  const ids = f.nodes.map((n) => n.id).sort();
  // runtime dropped; orphan dropped (its only edge < minEdge); a & b kept.
  assert.deepStrictEqual(ids, ["svc::a", "svc::b"]);
  assert.strictEqual(f.edges.length, 1);
  assert.strictEqual(f.edges[0].source, "svc::a");
  assert.strictEqual(f.edges[0].target, "svc::b");

  // Showing runtime + minEdge 1 keeps everything connected.
  const all = filterGraph(graph, { hideRuntime: false, minEdge: 1 });
  assert.ok(all.nodes.some((n) => n.id === "runtime"));

  // Default minEdge is 1 (keep every observed transition) — dial9's on-demand
  // fold produces small per-snapshot counts, so a high default empties the graph.
  const dflt = filterGraph(graph, {}); // no opts → hideRuntime true, minEdge 1
  // svc::a↔b (20) and svc::b→orphan (3) both survive minEdge 1; runtime dropped.
  const dfltIds = dflt.nodes.map((n) => n.id).sort();
  assert.deepStrictEqual(dfltIds, ["svc::a", "svc::b", "svc::orphan"]);
  assert.ok(!dfltIds.includes("runtime"), "runtime still hidden by default");
}

// 6. ELK graph builder: crate compound nodes + edges, correct dimensions.
{
  const graph = {
    nodes: [
      { id: "svc::a", crate_name: "svc", samples: 10, off_cpu: 0 },
      { id: "svc::b", crate_name: "svc", samples: 5, off_cpu: 0 },
      { id: "dep::x", crate_name: "dep", samples: 3, off_cpu: 0 },
    ],
    edges: [{ source: "svc::a", target: "dep::x", count: 4 }],
  };
  const elk = buildElkGraph(graph);
  assert.strictEqual(elk.id, "root");
  assert.strictEqual(elk.layoutOptions["elk.algorithm"], "layered");
  assert.strictEqual(elk.layoutOptions["elk.direction"], "DOWN");
  // Two crate clusters (svc, dep).
  assert.strictEqual(elk.children.length, 2);
  const svc = elk.children.find((c) => c.id === "crate:svc");
  assert.strictEqual(svc.children.length, 2);
  assert.strictEqual(svc.children[0].width, NODE_W);
  assert.strictEqual(svc.children[0].height, NODE_H);
  assert.strictEqual(elk.edges.length, 1);
  assert.deepStrictEqual(elk.edges[0].sources, ["svc::a"]);
  assert.deepStrictEqual(elk.edges[0].targets, ["dep::x"]);
}

// 7. Edge style scales with weight.
{
  const light = edgeStyle(1, 100);
  const heavy = edgeStyle(100, 100);
  assert.ok(heavy.width > light.width);
  assert.ok(heavy.opacity > light.opacity);
  assert.strictEqual(edgeStyle(0, 0).width, 1); // no divide-by-zero
}

// 8. API URL builder: passes scope, repeats host + app_prefix, requests>1 only.
{
  const params = new URLSearchParams();
  params.set("bucket", "b");
  params.set("service", "svc");
  params.set("start_ns", "100");
  params.append("host", "h1");
  params.append("host", "h2");
  const u = buildTraceGraphUrl("http://localhost:8080", params, {
    appPrefixes: ["metrics_", "svc_"],
    requests: 500,
  });
  assert.strictEqual(u.pathname, "/api/trace-graph");
  assert.strictEqual(u.searchParams.get("bucket"), "b");
  assert.strictEqual(u.searchParams.get("service"), "svc");
  assert.strictEqual(u.searchParams.get("start_ns"), "100");
  assert.deepStrictEqual(u.searchParams.getAll("host"), ["h1", "h2"]);
  assert.deepStrictEqual(u.searchParams.getAll("app_prefix"), ["metrics_", "svc_"]);
  assert.strictEqual(u.searchParams.get("requests"), "500");

  // requests <= 1 is omitted (per-module wall time, not per-request).
  const u2 = buildTraceGraphUrl("http://localhost:8080", new URLSearchParams(), { requests: 1 });
  assert.strictEqual(u2.searchParams.get("requests"), null);
}

console.log("✓ trace-graph API helper tests passed");
