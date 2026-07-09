# Trace graph architecture for dial9-viewer — plan + upstream issue draft

**Status:** design only, no code written. Ported from observability work in AWSMagnusIris (IRIS).
**Target:** a new `dial9-viewer` view — the dynamic, trace-derived **service architecture graph**.
**Priority:** this is the **first / main** port from IRIS. Everything else (stack-panel drill-in,
mechanism labels, click-into-trace) is secondary and explicitly deferred below.

IRIS source of truth (for reference — the *derivation* ports, the React/ELK *rendering* does not):
- `AWSMagnusIris/src/shared/moduleGraph.ts` — node identity, edges, aggregation, flow lenses
- `AWSMagnusIris/src/features/analysis/TraceGraphPanel.tsx` — layout + latency metrics

---

## 0. The key constraint (why this is cheap)

The graph is built from the **same input the flamegraph already folds**: per-stack sample counts +
a stack→frames dictionary. The flamegraph endpoint already produces exactly this
(`src/server/flamegraph.rs:118` `build_flamegraph_tree(stack_counts, stacks_dict)`,
fed by `AggSnapshot { stack_counts: &[(Vec<u8>, u64)], stacks_dict: HashMap<Vec<u8>, Vec<String>> }`).
A flamegraph is a *root→leaf trie* over those frames; the architecture graph is a *module adjacency
graph* over the same frames. **Same data, different fold** — no schema change, no decode change, no
`SAMPLES_FORMAT_VERSION` bump.

Frames come from the stacks-dictionary Parquet (`src/ingest/parquet_writer.rs:231-234`:
`stack_id → frames: List<Utf8>`, symbol strings leaf→root). That's all the graph needs.

**Consequences:**
- The whole graph derivation is a pure function of `(stack_counts, stacks_dict)` — reuse the
  flamegraph's SSE refine loop verbatim; only the accumulator's *output shape* changes.
- Latency-in-ms needs two more scalars the aggregate already has or can get cheaply: sample count
  (have) + distinct worker count + time span (`time_span_ns` exists on the tokio-stats path,
  `tokio_stats.rs:50`). See §2.3 — one honest caveat about per-request.

---

## 1. What ports (verified against IRIS source)

The IRIS derivation (`moduleGraph.ts`) is fully dynamic — nothing hardcoded, a component that shows
up in the trace gets a node. Three things port, in this order:

### 1a. The plain graph — nodes + edges + clustering
- **Node identity** = the frame's own `crate::module` (`moduleOf`): first two `::` path segments of
  the function symbol, so you get subsystem granularity, not per-function noise. App-crate frames get
  their own node; everything else (core/std/tokio/hyper/serde/…) collapses to a single `runtime` node
  so it reads as a gap without exploding the graph.
- **App-crate detection is by PREFIX**, not an allowlist. IRIS hardcodes
  `APP_CRATE_PREFIXES = ["amzn_", "dial9_"]` (APC-specific).
  ⚠️ **Generalize for the viewer** — other teams' services aren't `amzn_`/`dial9_`. Make the
  app-vs-runtime prefix set a **request parameter / config** (or infer the service crate from the
  scope's `service` column). This is the single biggest APC assumption to remove.
- **Edges** = observed caller→callee module adjacency, weighted by shared-sample count. Consecutive
  same-module frames collapse. (`aggregateModuleGraph` in `moduleGraph.ts`.)
- **Clustering** = by crate. IRIS renders crates as ELK compound (group) boxes. In the viewer, pick a
  vanilla-JS renderer (see §3) — do NOT pull in React/@xyflow/elkjs; the viewer UI is plain
  HTML/JS/SVG (`ui/*.html`, `ui/*.js`).

### 1b. Flow paths (generalize beyond APC)
Flows are highlight lenses: matching nodes stay lit, the rest dim; an edge lights when both ends
match. In IRIS this (`MODULE_FLOWS`) is the **only** curated part of the whole feature — a flow is
just a name-pattern (`test: (id) => boolean`) over the dynamically-discovered `crate::module` ids, so
the nodes/edges stay 100% trace-derived.

IRIS's flows are APC-specific: **Request Path, Auth, HSM Call, Key Lookup, Metering & Audit**
(regexes over `dataplane`, `futurex`, `cache_repository`, `querylog`, …).

⚠️ **These will differ in the viewer** — it serves many teams, none of whom share APC's HSM/Futurex/
querylog topology. Options, easiest first:
- **Ship no default flows** — start with the "All" view; the graph speaks for itself. (Recommended
  first cut — zero APC leakage.)
- **Config-driven flows** — a per-service list of `{label, color, pattern}` the viewer loads (config
  file / URL param / small in-UI editor).
- **Span-derived flows** — group nodes by the enclosing span (dial9 has spans; `trace_analysis.js`
  already reconstructs enclosing spans, `ui/test_enclosing_spans.js`) for team-agnostic flows for free.

Invariant to preserve: a flow is a *pattern over discovered ids*, never a fixed node-id list — that's
what lets it survive an unknown service's topology.

### 1c. Latency metrics (on the nodes)
dial9 is a wall-clock sampling profiler, so sample counts convert to time. IRIS
(`TraceGraphPanel.tsx` `deriveTiming` + `layout`):

```
msPerSample       = durationMs × workers / totalSamples      (≈ the ~1ms sample period)
perModuleWallMs   = node.samples × msPerSample               (wall time observed in the module)
blockedMs         = node.offCpu × msPerSample                (off-CPU: I/O, locks, waits)
```

Per node IRIS shows a top-right **badge** = per-request wall time, description = `total/req · blocked`,
heat = `>5ms` red / `1–5ms` amber / else green.

⚠️ **Per-request needs a request dimension the viewer may not have.** IRIS divides by
`tps.distinctRequests`. The viewer's samples table has no first-class request id — it'd have to come
from the attrs map (`parquet_writer.rs:50-72` keys/values) *if* the service tags samples with one.
**Recommend for v1:** show **per-module wall-ms** and **blocked-ms** (both derivable now:
`msPerSample` from distinct `worker_id` + `time_span_ns` + total sample count), and only add
**÷ requests** when a request-count is actually available for the scope. Don't fake per-request.

`offCpu` = the off-CPU sample subtotal per module. The viewer already classifies on/off-CPU
(`classify_poll`, `tokio_stats.rs:376-398`; samples carry a `source`/thread-class). Feed that
partition into the accumulator so `blocked` is real; a scope with no off-CPU samples honestly shows
`0 blocked`.

---

## 2. Design

### 2.1 Server — a graph accumulator alongside the flamegraph one
Mirror the flamegraph path (`src/server/flamegraph.rs`):
- New `GET /api/trace-graph` SSE endpoint, same scope/filter params + the refine-and-stream loop
  (`get_flamegraph`, `:202`). Reuse `AggContext` / `AggSnapshot` / the order-key fold verbatim.
- New `build_module_graph(stack_counts, stacks_dict, opts) -> ModuleGraph` next to
  `build_flamegraph_tree` (`:118`) — walks each stack's frames, maps each to `crate::module`
  (§1a), accumulates node sample counts + off-CPU subtotals + weighted caller→callee edges.
- `opts` carries the **app-crate prefixes** (§1a) and the on/off-CPU tag so the fold is
  team-agnostic and computes `blocked`.
- Response = `{ nodes: [{id, crate, module, samples, off_cpu}], edges: [{source, target, count}],
  meta: {worker_count, time_span_ns, total_samples} }` — the client derives ms (§1c) and lays out.

### 2.2 Client — a new `ui/trace_graph.{html,js}`
- Vanilla JS + SVG (match `tokio_stats.html` / `flamegraph.html` conventions; `creds.js`, `sse.js`,
  `format.js`, `url_state.js` are already shared).
- Consume the SSE stream, re-render the graph on each refining snapshot (same pattern as flamegraph).
- Layout: a lightweight layered/force layout in JS (candidates in §3) — cluster nodes by crate,
  draw weighted edges, badge each node with wall-ms + heat.
- Flow pills (§1b) toggle a highlight class; start with just "All".

### 2.3 Deferred (NOT in this port) — the drill-in
IRIS also has: click a node → side panel of the actual CPU stacks through it (`NodeStackPanel`),
stacks merged by a meaningful **cause label** (`stackLabel` / `MECHANISMS`: turns a pile of identical
`syscall ×N` into `syscall ← audit emit`, `← HSM call`, …), plus per-stack deep-links.

**All of this is out of scope for the first port.** Reasons:
- The viewer can already open individual stacks natively — the deep-link is redundant here.
- `MECHANISMS` is APC-flavored (audit/HSM/querylog patterns) and needs the same generalization the
  flows do — a separate follow-up.
- The plain graph + latency is the standalone-valuable piece; interaction is additive.

Land the graph first; the stack drill-in is a later issue.

---

## 3. Renderer choice (open build-time question)
IRIS uses React + `@xyflow/react` + `elkjs`. The viewer is dependency-light vanilla JS, so **do not
port that stack.** Options for the layered/clustered layout, lightest first:
- Hand-rolled layered layout (BFS levels + simple x-packing) drawn to SVG — no dep, matches the
  existing hand-rolled `heatmap.js` / flamegraph rendering. Fine for the modest node counts a
  module graph has (tens, not thousands).
- `elkjs` as a single vendored worker script (it's framework-agnostic; only the React binding is not)
  if the hand-rolled layout looks bad on real graphs.
- Decision can wait until we see real graphs; start hand-rolled.

---

## 4. Files that change (implementation map)

| Layer | File | Change |
|---|---|---|
| Rust endpoint | `src/server/mod.rs` (route table) | Register `GET /api/trace-graph` |
| Rust handler | `src/server/trace_graph.rs` (new) | SSE handler mirroring `flamegraph.rs`; `build_module_graph` + `ModuleGraph` serde structs |
| Rust reuse | `src/ingest/aggregate.rs` | Reuse `AggContext`/`AggSnapshot`/order-key fold; no schema change |
| JS view | `ui/trace_graph.html`, `ui/trace_graph.js` (new) | SSE client + layout + render + flow pills |
| JS shared | `ui/creds.js`, `sse.js`, `format.js`, `url_state.js` | Reuse as-is |
| Nav | `ui/index.html` | Add the new view to the viewer nav |
| Tests | `src/server/trace_graph.rs` tests | Assert graph nodes/edges/off-CPU from the demo trace (mirror `flamegraph.rs:508+`) |

**No changes to:** `parquet_writer.rs` schema, `decode.rs`, `SAMPLES_FORMAT_VERSION`. Everything is
derived from the stacks dictionary + sample counts already folded.

---

## 5. Non-breaking / upstream-fit
- **No schema/format bump** — new endpoint + additive response only.
- **No new public Rust API surface** (viewer-internal serde structs), so the
  `#[non_exhaustive]`/builder backwards-compat rules (`AGENTS.md`) don't bite.
- **No `decode.rs` change → no `tests/parser_parity_test.rs` obligation.**
- **Process** (`CONTRIBUTING.md`): work against `main` (§22), open an issue before significant work
  (§24), `cargo fmt --check` + clippy (`AGENTS.md:89`), `cargo nextest run` green, conventional
  `feat:` commits. Keep the diff focused.

---

## 6. Sequencing
1. File upstream issue (below) → get a nod / issue number.
2. **Rust:** `build_module_graph` + `/api/trace-graph` SSE endpoint (reuse the flamegraph fold);
   demo-trace test.
3. **JS:** `trace_graph.{html,js}` — SSE consume + hand-rolled layout + latency badges.
4. **JS:** flow pills (start with "All"; wire config/span-derived flows after).
5. Generalize app-crate prefixes to a param; verify on a non-APC capture.
6. (later issue) stack drill-in panel + mechanism labels.

---

## 7. Open build-time questions
- App-crate detection: explicit prefix param, or infer from the scope's `service`?
- Flows v1: none / config-driven / span-derived? (recommend none, then span-derived)
- Per-request latency: is any scope reliably tagged with a request id in the attrs map, or ship
  per-module wall-ms only?
- Renderer: hand-rolled SVG layout vs. vendored elkjs worker?

---
---

# UPSTREAM ISSUE DRAFT — file at github.com/dial9-rs/dial9/issues

**Title:** `Viewer: trace-derived service architecture graph (module nodes + observed edges + latency)`

**Labels:** enhancement

**Body:**

## Summary
Add a new viewer view that renders a service's **architecture graph directly from a capture**: nodes
are the `crate::module`s that appear in the sampled stacks, edges are the observed caller→callee
transitions between them (weighted), and each node is annotated with the wall-time spent in it
(on-CPU vs off-CPU/blocked). Nothing is hardcoded — a component that shows up in the trace gets a
node. It's a structural, at-a-glance complement to the flamegraph.

## Why it's cheap / non-breaking
It's built from the **same data the flamegraph already folds**: per-stack sample counts + the
stacks→frames dictionary (`src/server/flamegraph.rs:118` `build_flamegraph_tree`, fed by
`AggSnapshot { stack_counts, stacks_dict }`). A flamegraph is a root→leaf trie over those frames; this
is a module-adjacency graph over the same frames. So it needs **no schema change, no decode change,
no `SAMPLES_FORMAT_VERSION` bump** — a new SSE endpoint that reuses the flamegraph refine loop with a
different accumulator output, plus a small JS view.

## Proposed change
1. `GET /api/trace-graph` (SSE, mirrors `/api/flamegraph`): `build_module_graph(stack_counts,
   stacks_dict, opts)` → `{nodes:[{id,crate,module,samples,off_cpu}], edges:[{source,target,count}],
   meta:{worker_count,time_span_ns,total_samples}}`.
2. Node identity = the frame's `crate::module`; non-app crates collapse to a single `runtime` node.
   App-vs-runtime is a **request parameter** (prefix set / service crate), not hardcoded, so it works
   for any team's service.
3. Latency per node from the wall-clock sample model (`samples × time_span × workers / total`), split
   into total vs off-CPU (reusing the existing on/off-CPU classification).
4. A new `ui/trace_graph.{html,js}` (vanilla JS + SVG, like `tokio_stats`/`flamegraph`) that consumes
   the stream and lays the graph out, with optional highlight "flows" (pattern-over-node-ids lenses).

## Scope / non-goals
- Not touching `decode.rs`, the Parquet schema, or the aggregate fold logic.
- Per-node **stack drill-in** and mechanism-based stack labels are a separate follow-up.
- Default "flows" are deliberately minimal/none at first (they're inherently service-specific); happy
  to make them config- or span-driven based on maintainer preference.

## Questions for maintainers
- Preference for app-vs-runtime node detection: explicit prefix param vs. inferring from `service`?
- Renderer: is a small hand-rolled SVG layout acceptable, or is there an existing viewer convention
  you'd prefer I follow?
- Any request-id dimension in typical captures we could use for per-request latency, or keep it to
  per-module wall-time?

Happy to implement (against `main`, `cargo fmt` + clippy + `cargo nextest`, conventional commits) once
there's agreement on shape.
