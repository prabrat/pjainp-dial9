//! `/api/trace-graph` endpoint: stream a trace-derived service-architecture
//! graph over Server-Sent Events, refining as source files fold.
//!
//! The graph is a *module-adjacency graph* over the same sampled stack frames
//! the flamegraph folds: nodes are the `crate::module`s that appear in the
//! sampled stacks, edges are the observed caller→callee transitions between
//! them (weighted by shared-sample count), and each node is annotated with the
//! wall-time spent in it (total vs off-CPU/blocked). Nothing is hardcoded — a
//! component that shows up in the trace gets a node; non-application crates
//! collapse into a single `runtime` node so the graph reads as a structural
//! at-a-glance complement to the flamegraph rather than exploding.
//!
//! This mirrors [`crate::server::flamegraph`]: one request resolves the scope,
//! emits the already-folded snapshot, then folds up to the sampling cap and
//! pushes a fresh full-graph snapshot as each file lands. Each SSE `data:` frame
//! is one [`TraceGraphResponse`] JSON object, so the client re-renders on every
//! event.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use arrow::array::Array;
use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum_extra::extract::Query as QueryExtra;
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::ingest::aggregate::{self, Scope};
use crate::ingest::refine::{self, FoldErrors, FoldOutcome, RefineOpts, Resolved};
use crate::server::AppState;
use crate::server::credentials::MaybeCreds;
use crate::server::metrics::OperationMetrics;

/// The single node all non-application (runtime / std / third-party) frames
/// collapse into, so runtime plumbing reads as one gap instead of exploding the
/// graph into hundreds of library nodes.
const RUNTIME_ID: &str = "runtime";

/// Wire value of the off-CPU (scheduler / context-switch) sample source. Mirrors
/// `SOURCE_SCHED_EVENT` in the aggregator and the JS `source === 1` check: a
/// sample with this source was taken while the thread was blocked off-CPU.
const SOURCE_SCHED_EVENT: u8 = 1;

#[derive(Deserialize)]
pub struct TraceGraphParams {
    pub service: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    /// Host filter. Repeatable (`host=a&host=b`). Empty = all hosts.
    #[serde(default)]
    pub host: Vec<String>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    /// "Fetch more": raise the sampling-cap ceiling for this scope.
    pub max_files: Option<usize>,
    /// S3 bucket override (bring-your-own-credentials).
    pub bucket: Option<String>,
    /// S3 key prefix for source segment listing.
    pub prefix: Option<String>,
    /// Application-crate prefixes: a frame whose crate starts with one of these
    /// is kept as its own node; every other crate collapses into the single
    /// `runtime` node. Repeatable (`app_prefix=foo_&app_prefix=bar_`). When
    /// absent, the server infers the set from the folded frames (any crate whose
    /// symbols are not obviously std/runtime). This is the one knob that makes
    /// the graph a *service* graph rather than a call-graph, and it is a request
    /// parameter — never hardcoded — so it works for any service.
    #[serde(default)]
    pub app_prefix: Vec<String>,
    /// Optional request count to divide per-node wall time by, giving
    /// per-request latency. When absent or ≤1, per-node figures are total
    /// wall-time over the folded window (the client labels which).
    pub requests: Option<u64>,
}

// ── Response shape ───────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct TraceGraphResponse {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub meta: GraphMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coverage: Option<aggregate::Coverage>,
}

#[derive(Serialize)]
pub struct GraphNode {
    /// `crate::module` (or `runtime`) — the node identity.
    pub id: String,
    /// Top-level crate, used to cluster nodes into group boxes.
    pub crate_name: String,
    /// Module label within the crate (the last path segment shown on the node).
    pub module: String,
    /// Samples that passed through this module (its inclusive wall-time weight).
    pub samples: u64,
    /// Of `samples`, how many were off-CPU (blocked: I/O, locks, waits).
    pub off_cpu: u64,
}

#[derive(Serialize)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    /// Shared-sample count of this caller→callee transition.
    pub count: u64,
}

#[derive(Serialize)]
pub struct GraphMeta {
    pub total_samples: u64,
    /// Distinct workers that produced samples — the concurrency multiplier in
    /// the wall-clock sample model the client uses to convert samples to ms.
    pub worker_count: u64,
    /// Wall-clock span of the folded samples (ns), for the ms conversion.
    pub time_span_ns: i64,
    /// Request divisor echoed back (1 when none supplied → per-module wall-time).
    pub requests: u64,
    pub service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range: Option<String>,
    /// The application-crate prefixes actually applied (echoed so the UI can
    /// show what counted as "app" vs collapsed runtime).
    pub app_prefixes: Vec<String>,
}

// ── Module identity (generic port of IRIS `moduleOf`) ────────────────────────

/// Extract a `(crate, module)` identity from a symbolized frame string.
///
/// Finds the first `ident(::ident)+` run — the concrete crate path of the
/// frame's function, past any `<T as Trait>` generic wrapper. Each identifier
/// must be a real lowercase Rust path segment (`[a-z_][a-z0-9_]+`, i.e. 2+
/// chars), so single-letter generic parameters never match. Returns `None` for
/// a symbol with no such path (a bare `syscall`, `__memcpy`, or hex address) —
/// the caller treats those as runtime.
fn module_of(sym: &str) -> Option<(String, String)> {
    let bytes = sym.as_bytes();
    let n = bytes.len();
    let is_seg_start = |b: u8| b == b'_' || b.is_ascii_lowercase();
    let is_seg_cont = |b: u8| b == b'_' || b.is_ascii_lowercase() || b.is_ascii_digit();

    // A path run may only begin at a WORD BOUNDARY: the preceding byte must not
    // be an identifier character. Without this, a CamelCase word like
    // `MyFactory` would match its lowercase tail (`actory`) as a fake crate.
    let is_ident = |b: u8| b == b'_' || b.is_ascii_lowercase() || b.is_ascii_uppercase() || b.is_ascii_digit();

    let mut i = 0;
    while i < n {
        // Try to match a path run starting at i: ident(::ident)+.
        let at_boundary = i == 0 || !is_ident(bytes[i - 1]);
        if at_boundary && is_seg_start(bytes[i]) {
            let run_start = i;
            let mut segs: Vec<(usize, usize)> = Vec::new();
            let mut j = i;
            loop {
                // Read one identifier (≥2 chars: a start byte + ≥1 continue byte).
                let seg_start = j;
                if j < n && is_seg_start(bytes[j]) {
                    j += 1;
                    while j < n && is_seg_cont(bytes[j]) {
                        j += 1;
                    }
                }
                let seg_len = j - seg_start;
                if seg_len < 2 {
                    break; // not a valid ≥2-char segment
                }
                segs.push((seg_start, j));
                // Continue only if followed by "::".
                if j + 1 < n && bytes[j] == b':' && bytes[j + 1] == b':' {
                    j += 2;
                    continue;
                }
                break;
            }
            if segs.len() >= 2 {
                let crate_name = sym[segs[0].0..segs[0].1].to_string();
                let module = sym[segs[1].0..segs[1].1].to_string();
                return Some((crate_name, module));
            }
            // Not a multi-segment run; resume scanning after run_start.
            i = run_start + 1;
        } else {
            i += 1;
        }
    }
    None
}

/// Whether a crate name is an application crate (kept as its own node) given the
/// active prefix set. When the set is empty, nothing is app → the whole graph
/// collapses to `runtime` (the caller avoids that by inferring a set first).
fn is_app_crate(crate_name: &str, app_prefixes: &[String]) -> bool {
    app_prefixes.iter().any(|p| crate_name.starts_with(p))
}

/// Crate-name prefixes that are unmistakably std / toolchain / ubiquitous
/// runtime, used only to *infer* the application-crate set when the caller
/// supplies none. This is a best-effort denylist for inference; the authoritative
/// app-vs-runtime decision is the caller-supplied prefix set.
const KNOWN_RUNTIME_CRATES: &[&str] = &[
    "std", "core", "alloc", "tokio", "hyper", "h2", "mio", "futures",
    "futures_util", "futures_core", "futures_task", "serde", "serde_json",
    "tracing", "tracing_core", "tracing_subscriber", "bytes", "http", "tower",
    "tower_http", "rustls", "tokio_util", "pin_project", "pin_project_lite",
    "backtrace", "gimli", "libc", "parking_lot", "parking_lot_core", "crossbeam",
    "crossbeam_utils", "crossbeam_channel", "once_cell", "smallvec", "hashbrown",
    "aws_smithy_runtime", "aws_smithy_runtime_api", "aws_smithy_async",
    "aws_smithy_types", "aws_config", "aws_sigv4", "aws_runtime", "matchit",
    "num_cpus", "slab", "socket2", "want", "tokio_rustls",
];

/// Node identity for a frame: `(id, crate, module)`. App-crate frames get their
/// own `crate::module` node; everything else collapses to `runtime`.
fn node_id_for(sym: &str, app_prefixes: &[String]) -> (String, String, String) {
    match module_of(sym) {
        Some((crate_name, module)) if is_app_crate(&crate_name, app_prefixes) => {
            (format!("{crate_name}::{module}"), crate_name, module)
        }
        _ => (
            RUNTIME_ID.to_string(),
            RUNTIME_ID.to_string(),
            RUNTIME_ID.to_string(),
        ),
    }
}

// ── Accumulator ──────────────────────────────────────────────────────────────

/// Per-stack sample tallies: total occurrences and the off-CPU subtotal.
#[derive(Default, Clone, Copy)]
struct StackTally {
    total: u64,
    off_cpu: u64,
}

/// Incremental service-graph accumulator. Merges folded part-files one at a
/// time: samples (keyed by stack id, split on/off-CPU via the `source` column)
/// plus the stacks dictionary (stack id → symbolized frames, leaf→root). The
/// graph itself is derived per snapshot from these tallies so the fold direction
/// and collapse rules can be applied uniformly (see [`build_module_graph`]).
#[derive(Default)]
struct GraphAccum {
    /// stack id → (total, off_cpu) sample tallies.
    stacks: HashMap<[u8; 16], StackTally>,
    /// stack id → symbolized frames (leaf→root), from the dict part-files.
    dict: HashMap<Vec<u8>, Vec<String>>,
    workers: HashSet<u32>,
    total_samples: u64,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
}

impl GraphAccum {
    fn time_span_ns(&self) -> i64 {
        match (self.min_ts, self.max_ts) {
            (Some(lo), Some(hi)) => (hi - lo).max(1),
            _ => 1,
        }
    }

    /// Merge one folded file's samples part (+ its optional dict part).
    fn merge(&mut self, samples: Vec<u8>, dict: Option<Vec<u8>>) -> anyhow::Result<()> {
        self.read_samples_part(samples)?;
        if let Some(dict) = dict {
            self.read_dict_part(dict)?;
        }
        Ok(())
    }

    fn read_samples_part(&mut self, data: Vec<u8>) -> anyhow::Result<()> {
        let reader = ::parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(
            bytes::Bytes::from(data),
            4096,
        )?;
        for batch in reader {
            let batch = batch?;
            let Some(stack_arr) = batch.column_by_name("stack_id").and_then(|c| {
                c.as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            }) else {
                continue;
            };
            let ts_arr = batch
                .column_by_name("timestamp_ns")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>());
            let source_arr = batch
                .column_by_name("source")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::UInt8Array>());
            let worker_arr = batch
                .column_by_name("worker_id")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::UInt32Array>());

            for i in 0..batch.num_rows() {
                let mut id = [0u8; 16];
                id.copy_from_slice(stack_arr.value(i));
                let off_cpu = source_arr.is_some_and(|a| a.value(i) == SOURCE_SCHED_EVENT);
                let tally = self.stacks.entry(id).or_default();
                tally.total += 1;
                if off_cpu {
                    tally.off_cpu += 1;
                }
                self.total_samples += 1;
                if let Some(ts) = ts_arr {
                    let v = ts.value(i);
                    self.min_ts = Some(self.min_ts.map_or(v, |m| m.min(v)));
                    self.max_ts = Some(self.max_ts.map_or(v, |m| m.max(v)));
                }
                // Off-worker samples carry the sentinel id (>= 255); exclude them
                // from the distinct-worker count that scales the ms conversion.
                if let Some(w) = worker_arr {
                    if !w.is_null(i) {
                        let wid = w.value(i);
                        if wid < 255 {
                            self.workers.insert(wid);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn read_dict_part(&mut self, data: Vec<u8>) -> anyhow::Result<()> {
        let reader = ::parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(
            bytes::Bytes::from(data),
            4096,
        )?;
        for batch in reader {
            let batch = batch?;
            let stack_arr = batch.column_by_name("stack_id").and_then(|c| {
                c.as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            });
            let frames_arr = batch
                .column_by_name("frames")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::ListArray>());
            let (Some(stack_arr), Some(frames_arr)) = (stack_arr, frames_arr) else {
                continue;
            };
            for i in 0..batch.num_rows() {
                let id = stack_arr.value(i).to_vec();
                if self.dict.contains_key(&id) {
                    continue;
                }
                let frame_list = frames_arr.value(i);
                if let Some(str_arr) = frame_list
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    let frames: Vec<String> =
                        (0..str_arr.len()).map(|j| str_arr.value(j).to_string()).collect();
                    self.dict.insert(id, frames);
                }
            }
        }
        Ok(())
    }

    /// Infer the application-crate prefix set from the folded frames when the
    /// caller supplied none: every crate that appears and is not an obvious
    /// std/runtime crate. Returns exact crate names (used as prefixes, so a name
    /// still matches its own frames). Empty result → fall back to no collapse
    /// caveat handled by the caller.
    fn infer_app_prefixes(&self) -> Vec<String> {
        let mut app: HashSet<String> = HashSet::new();
        for frames in self.dict.values() {
            for f in frames {
                if let Some((crate_name, _)) = module_of(f) {
                    if !KNOWN_RUNTIME_CRATES.contains(&crate_name.as_str()) {
                        app.insert(crate_name);
                    }
                }
            }
        }
        let mut v: Vec<String> = app.into_iter().collect();
        v.sort();
        v
    }
}

/// Build the service-architecture graph from the accumulated stack tallies +
/// dictionary. Generic port of IRIS's `aggregateModuleGraph`:
///
/// - Frames are stored leaf→root; we reverse to walk root→leaf (caller→callee),
///   the direction an edge points.
/// - Each frame maps to its `crate::module` node id (non-app crates → `runtime`).
/// - Consecutive same-node frames collapse (a recursive descent through one
///   module, or several runtime frames in a row, count once).
/// - A node's `samples`/`off_cpu` accumulate the stack's tally once per distinct
///   node it passes through; an edge accumulates the tally for each caller→callee
///   transition between distinct nodes.
fn build_module_graph(accum: &GraphAccum, app_prefixes: &[String]) -> (Vec<GraphNode>, Vec<GraphEdge>) {
    struct NodeAcc {
        crate_name: String,
        module: String,
        samples: u64,
        off_cpu: u64,
    }
    let mut nodes: HashMap<String, NodeAcc> = HashMap::new();
    let mut edges: HashMap<(String, String), u64> = HashMap::new();

    for (stack_id, tally) in &accum.stacks {
        let Some(frames) = accum.dict.get(stack_id.as_slice()) else {
            continue;
        };
        // Reverse (leaf→root stored) to root→leaf, mapping to node ids and
        // collapsing consecutive duplicates.
        let mut chain: Vec<(String, String, String)> = Vec::new();
        for frame in frames.iter().rev() {
            let (id, crate_name, module) = node_id_for(frame, app_prefixes);
            if chain.last().is_some_and(|(prev, _, _)| prev == &id) {
                continue;
            }
            chain.push((id, crate_name, module));
        }
        for (idx, (id, crate_name, module)) in chain.iter().enumerate() {
            let n = nodes.entry(id.clone()).or_insert_with(|| NodeAcc {
                crate_name: crate_name.clone(),
                module: module.clone(),
                samples: 0,
                off_cpu: 0,
            });
            n.samples += tally.total;
            n.off_cpu += tally.off_cpu;
            if idx > 0 {
                let src = chain[idx - 1].0.clone();
                *edges.entry((src, id.clone())).or_insert(0) += tally.total;
            }
        }
    }

    let mut node_list: Vec<GraphNode> = nodes
        .into_iter()
        .map(|(id, n)| GraphNode {
            id,
            crate_name: n.crate_name,
            module: n.module,
            samples: n.samples,
            off_cpu: n.off_cpu,
        })
        .collect();
    node_list.sort_by(|a, b| b.samples.cmp(&a.samples).then_with(|| a.id.cmp(&b.id)));

    let mut edge_list: Vec<GraphEdge> = edges
        .into_iter()
        .map(|((source, target), count)| GraphEdge { source, target, count })
        .collect();
    edge_list.sort_by(|a, b| b.count.cmp(&a.count));

    (node_list, edge_list)
}

// ── SSE handler (mirrors flamegraph.rs) ──────────────────────────────────────

/// Handler for `GET /api/trace-graph` — a Server-Sent Events stream.
pub async fn get_trace_graph(
    State(state): State<AppState>,
    creds: MaybeCreds,
    QueryExtra(params): QueryExtra<TraceGraphParams>,
) -> Result<
    (
        Extension<OperationMetrics>,
        Sse<impl Stream<Item = Result<Event, Infallible>>>,
    ),
    (StatusCode, String),
> {
    let Some(agg) = state
        .agg_context_for(params.bucket.as_deref(), params.prefix.as_deref(), creds)
        .await?
    else {
        return Err((
            StatusCode::NOT_FOUND,
            "trace-graph requires demand-driven aggregation (start with --agg or supply a bucket)"
                .to_string(),
        ));
    };

    let scope = Scope {
        start_ns: params.start_ns,
        end_ns: params.end_ns,
        service: params.service.clone(),
        hosts: params.host.clone(),
    };
    let opts = RefineOpts {
        max_files: params.max_files,
    };
    let Some(resolved) = refine::resolve(&agg, &scope, opts).await else {
        return Err((
            StatusCode::NOT_FOUND,
            "no source files match this scope".to_string(),
        ));
    };

    let op = OperationMetrics::flamegraph(
        resolved.files_matched as u32,
        resolved.files_folded_in(resolved.folded()) as u32,
        None,
    );

    let stream = trace_graph_stream(agg, resolved, &params, state.fold_limits.clone());
    Ok((
        Extension(op),
        Sse::new(stream).keep_alive(KeepAlive::default()),
    ))
}

/// Immutable per-request context threaded through the SSE stream.
struct StreamCtx {
    agg: Arc<aggregate::AggContext>,
    resolved: Resolved,
    app_prefixes: Vec<String>,
    requests: u64,
    service: Option<String>,
    from: Option<String>,
    to: Option<String>,
}

/// Phase of the SSE fold state machine (mirrors [`crate::server::flamegraph`]).
enum Phase {
    Start,
    Folding {
        accum: GraphAccum,
        folded: HashSet<String>,
        errors: FoldErrors,
    },
}

fn trace_graph_stream(
    agg: aggregate::AggContext,
    resolved: Resolved,
    params: &TraceGraphParams,
    limits: aggregate::FoldLimits,
) -> impl Stream<Item = Result<Event, Infallible>> + use<> {
    let agg = Arc::new(agg);
    let ctx = Arc::new(StreamCtx {
        agg: Arc::clone(&agg),
        resolved,
        app_prefixes: params.app_prefix.clone(),
        requests: params.requests.unwrap_or(1).max(1),
        service: params.service.clone(),
        from: params.from.clone(),
        to: params.to.clone(),
    });

    let folds = Box::pin(refine::fold_stream(
        agg,
        limits,
        ctx.resolved.unfolded_capped(),
    ));

    stream::unfold(
        (ctx, folds, Phase::Start),
        |(ctx, mut folds, phase)| async move {
            match phase {
                Phase::Start => {
                    let seed = aggregate::fetch_folded_sample_parts(
                        &*ctx.agg.output,
                        &ctx.agg.output_bucket,
                        &ctx.agg.output_prefix,
                        &ctx.resolved.capped_full_keys(),
                        ctx.resolved.folded(),
                    )
                    .await;
                    let mut accum = GraphAccum::default();
                    for (samples, dict) in seed {
                        if let Err(e) = accum.merge(samples, dict) {
                            rate_limited_warn("trace-graph: seed merge failed", &e);
                        }
                    }
                    let folded = ctx.resolved.folded().clone();
                    let errors = FoldErrors::default();
                    let event = snapshot_event(&ctx, &accum, &folded, &errors);
                    Some((
                        Ok(event),
                        (ctx, folds, Phase::Folding { accum, folded, errors }),
                    ))
                }
                Phase::Folding {
                    mut accum,
                    mut folded,
                    mut errors,
                } => {
                    match folds.next().await? {
                        FoldOutcome::Folded(f) => {
                            if let Some((samples, dict)) = aggregate::fetch_sample_parts(
                                &*ctx.agg.output,
                                &ctx.agg.output_bucket,
                                &ctx.agg.output_prefix,
                                &f.full_key,
                            )
                            .await
                                && let Err(e) = accum.merge(samples, dict)
                            {
                                rate_limited_warn("trace-graph: merge failed", &e);
                            }
                            folded.insert(aggregate::part_leaf_of(&f.full_key));
                        }
                        FoldOutcome::Failed { raw_key, error } => {
                            errors.record(&raw_key, &error);
                        }
                    }
                    let event = snapshot_event(&ctx, &accum, &folded, &errors);
                    Some((
                        Ok(event),
                        (ctx, folds, Phase::Folding { accum, folded, errors }),
                    ))
                }
            }
        },
    )
}

fn rate_limited_warn(msg: &str, err: &anyhow::Error) {
    use dial9_core::rate_limited;
    rate_limited!(std::time::Duration::from_secs(60), {
        tracing::warn!("{msg}: {err}");
    });
}

fn snapshot_event(
    ctx: &StreamCtx,
    accum: &GraphAccum,
    folded: &HashSet<String>,
    errors: &FoldErrors,
) -> Event {
    // Prefer the caller-supplied app-crate prefixes; otherwise infer from the
    // folded frames so the graph is a service graph even without the param.
    let app_prefixes = if ctx.app_prefixes.is_empty() {
        accum.infer_app_prefixes()
    } else {
        ctx.app_prefixes.clone()
    };
    let (nodes, edges) = build_module_graph(accum, &app_prefixes);

    let files_matched = ctx.resolved.files_matched;
    let files_folded = ctx.resolved.files_folded_in(folded);
    let coverage = aggregate::Coverage {
        files_matched,
        files_folded,
        samples_folded: accum.total_samples as usize,
        total_bytes: ctx.resolved.total_bytes,
        hosts_matched: ctx.resolved.hosts_matched,
        hosts_folded: ctx.resolved.folded_hosts(folded),
        fold_errors: errors.count,
        fold_error_sample: errors.sample.clone(),
    };

    let resp = TraceGraphResponse {
        nodes,
        edges,
        meta: GraphMeta {
            total_samples: accum.total_samples,
            worker_count: accum.workers.len().max(1) as u64,
            time_span_ns: accum.time_span_ns(),
            requests: ctx.requests,
            service: ctx.service.clone(),
            time_range: match (&ctx.from, &ctx.to) {
                (Some(f), Some(t)) => Some(format!("{f}–{t}")),
                _ => None,
            },
            app_prefixes,
        },
        coverage: Some(coverage),
    };
    Event::default().json_data(&resp).unwrap_or_else(|e| {
        rate_limited_warn("trace-graph: event serialize failed", &anyhow::anyhow!(e));
        Event::default().comment("serialize error")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_of_extracts_crate_module() {
        assert_eq!(
            module_of("dial9_core::runtime::worker::run"),
            Some(("dial9_core".to_string(), "runtime".to_string()))
        );
        // Trait-impl generic wrapper: the first real lowercase path run wins,
        // skipping the single-letter generic parameter `T`.
        assert_eq!(
            module_of("<T as alloc::string::ToString>::to_string"),
            Some(("alloc".to_string(), "string".to_string()))
        );
        // Bare / kernel / hex frames have no crate::module path.
        assert_eq!(module_of("do_futex"), None);
        assert_eq!(module_of("0xdeadbeef"), None);
        assert_eq!(module_of(""), None);
        // A path run must begin at a WORD BOUNDARY: a CamelCase word like
        // `MyFactory::build` must NOT latch onto its lowercase tail (`actory`) as
        // a crate. The first real crate::path run wins instead.
        assert_eq!(module_of("MyFactory::build"), None);
        assert_eq!(
            module_of("<MyFactory as amzn_svc::traits::Build>::build"),
            Some(("amzn_svc".to_string(), "traits".to_string()))
        );
    }

    #[test]
    fn node_id_collapses_non_app_to_runtime() {
        let app = vec!["metrics_service".to_string()];
        assert_eq!(
            node_id_for("metrics_service::routes::handle", &app),
            (
                "metrics_service::routes".to_string(),
                "metrics_service".to_string(),
                "routes".to_string()
            )
        );
        // tokio is not app → runtime.
        let (id, cr, m) = node_id_for("tokio::runtime::scheduler::poll", &app);
        assert_eq!((id.as_str(), cr.as_str(), m.as_str()), ("runtime", "runtime", "runtime"));
    }

    /// Build a tiny accumulator by hand and check the derived graph: node
    /// counts, runtime collapse, off-CPU attribution, and caller→callee edges.
    #[test]
    fn build_module_graph_from_stacks() {
        let mut accum = GraphAccum::default();
        // Stack (leaf→root): std::futex ← tokio::poll ← metrics_service::routes.
        let sid = [1u8; 16];
        accum.dict.insert(
            sid.to_vec(),
            vec![
                "std::sys::futex".to_string(),
                "tokio::runtime::poll".to_string(),
                "metrics_service::routes::handle".to_string(),
            ],
        );
        // 10 samples through this stack, 4 of them off-CPU.
        accum.stacks.insert(sid, StackTally { total: 10, off_cpu: 4 });
        accum.total_samples = 10;

        let app = vec!["metrics_service".to_string()];
        let (nodes, edges) = build_module_graph(&accum, &app);

        // Two nodes: the app module and the collapsed runtime (std + tokio fold).
        let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains(&"metrics_service::routes"));
        assert!(ids.contains(&"runtime"));
        assert_eq!(nodes.len(), 2);

        let app_node = nodes.iter().find(|n| n.id == "metrics_service::routes").unwrap();
        let rt_node = nodes.iter().find(|n| n.id == "runtime").unwrap();
        assert_eq!(app_node.samples, 10);
        assert_eq!(rt_node.samples, 10);
        // Off-CPU tally attaches to every node the stack passes through.
        assert_eq!(rt_node.off_cpu, 4);

        // One caller→callee edge: routes → runtime (root→leaf direction).
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source, "metrics_service::routes");
        assert_eq!(edges[0].target, "runtime");
        assert_eq!(edges[0].count, 10);
    }

    /// End-to-end over the embedded demo trace: decode → write the samples +
    /// dict part-files → read them back through the accumulator → derive the
    /// graph. Asserts we get real application module nodes (not just runtime),
    /// weighted edges, and a positive worker count — the same fixture pattern
    /// the tokio-stats reader test uses.
    #[test]
    fn build_graph_from_demo_trace() {
        use crate::ingest::decode::decode_samples;
        use crate::ingest::parquet_writer;

        let data =
            std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/ui/demo-trace.bin")).unwrap();
        let decompressed = {
            use std::io::Read;
            let mut dec = flate2::read::GzDecoder::new(data.as_slice());
            let mut buf = Vec::new();
            dec.read_to_end(&mut buf).unwrap();
            buf
        };
        let (samples, stacks, _polls) = decode_samples(&decompressed, "demo-trace.bin").unwrap();
        assert!(!samples.is_empty());

        let metadata = std::collections::HashMap::new();
        let mut samples_buf = Vec::new();
        parquet_writer::write_samples(&mut samples_buf, &samples, &metadata).unwrap();
        let mut dict_buf = Vec::new();
        parquet_writer::write_stacks_dict(&mut dict_buf, &stacks).unwrap();

        let mut accum = GraphAccum::default();
        accum.merge(samples_buf, Some(dict_buf)).unwrap();
        assert_eq!(accum.total_samples, samples.len() as u64);
        assert!(accum.workers.len() >= 1, "expected at least one worker");

        // Infer app crates (no explicit prefixes) and build the graph.
        let app = accum.infer_app_prefixes();
        assert!(!app.is_empty(), "should infer at least one app crate");
        let (nodes, edges) = build_module_graph(&accum, &app);

        // At least one real application module node beyond the runtime collapse.
        let app_nodes = nodes.iter().filter(|n| n.id != RUNTIME_ID).count();
        assert!(app_nodes > 0, "expected application module nodes");
        assert!(!edges.is_empty(), "expected caller→callee edges");
        // A node's off-CPU subtotal never exceeds its total (both accumulate the
        // same per-occurrence weight). Note `samples` itself CAN exceed
        // total_samples: matching IRIS, a node is counted once per (collapsed)
        // frame occurrence, so a node appearing twice non-consecutively in one
        // stack is counted twice — an inclusive weight, not a partition.
        for n in &nodes {
            assert!(n.off_cpu <= n.samples);
        }
        eprintln!(
            "trace-graph demo: {} nodes ({} app), {} edges, {} samples, {} workers",
            nodes.len(),
            app_nodes,
            edges.len(),
            accum.total_samples,
            accum.workers.len()
        );
    }

    #[test]
    fn infer_app_prefixes_excludes_runtime_crates() {
        let mut accum = GraphAccum::default();
        accum.dict.insert(
            vec![0u8; 16],
            vec![
                "metrics_service::routes::handle".to_string(),
                "tokio::runtime::poll".to_string(),
                "std::sys::futex".to_string(),
            ],
        );
        let inferred = accum.infer_app_prefixes();
        assert_eq!(inferred, vec!["metrics_service".to_string()]);
    }
}
