# Prometheus Metrics & Alerting

> **Status:** stable. The single reference for the Prometheus metrics kamu-cli exports and the alerts
> we recommend on top of them. Alert rules themselves live in deployment repositories.
> Names and paths below are drawn from source — when they drift, treat the source as canonical and
> update this page.

---

## Quick-start

**One-paragraph mental model.** Components that want to export metrics implement `MetricsProvider`
and are registered in the dill catalog as singletons. At startup `observability::metrics::register_all`
collects every provider into one `prometheus::Registry`, which is served in text format at
`GET /system/metrics` (API server and web UI server) and, for one-off CLI commands, dumped to
`kamu.metrics.txt` with the `--metrics` flag. Exported metrics focus on **background agents**: they
work unattended, so their failures are silent unless measured.

| You want to… | Start at |
| --- | --- |
| Look up what a metric means | [§2 Catalog](#2-catalog) |
| Set up alerting for a deployment | [§4 Recommended alerts](#4-recommended-alerts) |
| Understand why these metrics and not others | [§3 Design decisions](#3-design-decisions) |
| Add a new metric | [§5 Recipe](#5-recipe-adding-metrics) |

---

## Table of contents

- [1. Exposure](#1-exposure)
- [2. Catalog](#2-catalog)
- [3. Design decisions](#3-design-decisions)
- [4. Recommended alerts](#4-recommended-alerts)
- [5. Recipe: adding metrics](#5-recipe-adding-metrics)
- [6. File reference map](#6-file-reference-map)

---

## 1. Exposure

| Where | How |
| --- | --- |
| `kamu system api-server` | `GET /system/metrics`, routed before the tracing layer so scrapes stay out of logs |
| `kamu ui` | same endpoint |
| Any CLI command | `kamu --metrics <command>` writes `kamu.metrics.txt` when the command ends |

Registration happens once, in `observability::metrics::register_all`, over every
`dyn MetricsProvider` in the catalog. Registering the same metric twice panics at startup, so a
provider must be a `#[scope(Singleton)]` component registered exactly once.

---

## 2. Catalog

### Wakeup listeners (all background agents)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `wakeup_listener_last_wait_timestamp_seconds` | gauge | `agent` | Unix time when the agent last started or finished waiting for changes. An agent waits at least every `backgroundAgents.maxListeningTimeout`, so a stale value means its loop is hung. The task agent waits only while idle (see `task_agent_running_task_started_timestamp_seconds`) |

Recorded by the shared `HubWakeupListener` on every backend, so any wakeup-driven agent reports it
without code of its own ([wakeup-listeners.md](wakeup-listeners.md#monitoring)). `agent` is the
name of the agent the wakeup source serves, the same as its `agent_name()`:
`dev.kamu.utils.messaging.OutboxAgent`, `dev.kamu.domain.flow-system.FlowSystemEventAgent`,
`dev.kamu.domain.flow-system.FlowAgent`, `dev.kamu.domain.task-system.TaskAgent`.

### Task agent

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `task_agent_task_duration_seconds` | histogram | `plan_type`, `outcome` = `success` / `failed` / `cancelled` | Time from taking a task off the queue to its outcome. Its `_count` is the number of finished tasks |
| `task_agent_task_queue_wait_seconds` | histogram | — | Time a task waited in the queue before the agent took it. A webhook delivery retry is a new task, so each attempt waits anew |
| `task_agent_running_task_started_timestamp_seconds` | gauge | `executor` | Unix time when the task running on the executor started; `0` while it is idle |

`plan_type` is `LogicalPlan::plan_type` (e.g. `UpdateDataset`, `HardCompactDataset`, `ResetDataset`,
`DeliverWebhook`, `Probe`); label sets are pre-created at startup for every registered planner.
`executor` is always `main` for now: the task agent runs one task at a time in-process. The label
is reserved for clustered deployments running tasks on several execution nodes.

### Flow agent

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `flow_agent_activations_total` | counter | `flow_type`, `outcome` = `activated` / `skipped` / `failed` | Activation attempts of due flows. `skipped`: the flow changed concurrently, re-read next pass. `failed`: retried after `flowSystem.awaitingStepSecs` |
| `flow_agent_activation_delay_seconds` | histogram | — | Time from a flow's scheduled activation moment to its activation |

`flow_type` is the flow binding's type (e.g. `dev.kamu.flow.dataset.ingest`); label sets are
pre-created at startup for every registered flow controller.

### Flow completion

A flow completes once its final outcome is known: when its task finishes and no retry is left, or
when it is aborted. Recorded by the flow agent (task finished) and `FlowAbortHelper` (abort).

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `flow_system_flow_duration_seconds` | histogram | `flow_type`, `outcome` = `success` / `failed` | Time from the flow's first activation (its first task scheduled) to its completion, across all its tasks and retry backoff. Its `_count` is the number of completed flows |
| `flow_system_flow_retries` | histogram (buckets `0, 1, 2, 3, 5, 10`) | `flow_type`, `outcome` = `success` / `failed` | Retried task attempts of a completed flow: a flakiness indicator |
| `flow_system_flows_aborted_total` | counter | `flow_type` | Flows aborted: by a user, or by removing their trigger or dataset |

A retried failure is not a completion: `failed` counts only flows that failed after exhausting
their retries (or on an unrecoverable error) — the failures users see.

### Flow system event agent

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `flow_system_event_projector_failing` | gauge | `projector` | `1` while the last attempt to apply a batch of flow system events to the projection failed. The batch is retried on every wakeup; until one succeeds, the projection makes no progress |

`projector` is `FlowSystemEventProjector::name()`, e.g.
`dev.kamu.domain.flow-system.FlowProcessStateProjector`.

### Outbox agent

See [outbox.md §8](outbox.md#8-configuration--metrics) for how they are computed.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `outbox_messages_processed_total` | counter | `producer`, `consumer` | Messages consumed |
| `outbox_messages_pending_total` | gauge | `producer`, `consumer` | Best-effort backlog (latest message ID − consumed message ID) |
| `outbox_failed_consumers_total` | gauge | `producer`, `consumer` | `1` while the consumer is failed; it stays failed until restart |

### S3

`S3Metrics` (`s3-utils`) records S3 API calls. kamu-cli itself does not register it; applications
embedding kamu that pass it to `S3Context` do.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `s3_api_call_count_successful_num_total` | counter | `storage_url`, `sdk_method` | Successful S3 calls |
| `s3_api_call_count_failed_num_total` | counter | `storage_url`, `sdk_method` | Failed S3 calls |
| `s3_api_request_time_s_hist` | histogram (default buckets) | `storage_url`, `sdk_method` | S3 call latency |

---

## 3. Design decisions

- **Measure silent degradation, not exits.** When any background agent's `run()` returns, the API
  server shuts down (`api_server.rs`), so an exited agent already shows as a restarted pod. What
  goes unnoticed is an agent that keeps running but is hung, saturated, lagging, or failing the
  same work in a loop. Every agent metric answers one such question.
- **One metric per question, few metrics overall.** Deliberately left out:
  - *queued task count* — needs a new store query; queue wait and the running task gauge already
    show saturation and stuck tasks;
  - *per-projector lag* — needs a query per projector; the failing gauge covers a stuck projection;
  - *tasks per flow* — equal to retries + 1 while a flow runs one task at a time; worth adding
    with composite flows.
- **Flow duration starts at the first activation.** The planned activation time moves earlier on a
  manual run or once batching is satisfied, so measuring from the first plan would count such flows
  as `0`. The first task's scheduling time never moves; retries keep it.
- **Aborts are counted, not timed.** An abort is a user's decision, so its timing says nothing about
  the system; a counter shows spikes. Only successful and failed flows get durations and retries.
- **Flow failures are not task failures.** A task failure that gets retried is visible in
  `task_agent_task_duration_seconds{outcome="failed"}`; a flow fails only when retries run out. A
  spike in flow failures points at configuration or an external dependency (API keys, sources).
- **Projector state mirrors outbox consumer state.** Both apply one ordered stream; a projection
  stuck on a batch is as serious as a failed outbox consumer, so both are `1`/`0` gauges to page on.
  They differ in recovery: a failed outbox consumer stays stopped until restart, a projector
  retries on every wakeup and clears the gauge once a batch succeeds.
- **The heartbeat lives in the wakeup layer, not in agents.** Every agent waits through the shared
  `HubWakeupListener`, so recording there covers all of them, and any future wakeup-driven agent,
  without per-agent code. It records both on entering and on leaving the wait: the flow agent races
  the wait against its activation deadline and drops it when the deadline wins, so recording only on
  return would make a busy, healthy scheduler look hung.
- **Labelled by agent, with no code in agents.** Each wakeup source serves exactly one agent, so it
  labels its handles with that agent's name constant; alerts then name the agent directly.
- **Low-cardinality labels only.** Types and names from code (`plan_type`, `flow_type`,
  `projector`, `agent`, outbox producer/consumer names), never dataset, account, flow or task IDs.
- **A label must explain the value.** Task queue wait has no `plan_type`: the queue is FIFO, so a
  wait depends on the tasks ahead, not on the task's own type. Which types occupy the slot shows in
  `task_agent_task_duration_seconds` by `plan_type`. Task duration, in turn, is split by `outcome`:
  fast failures, timeouts and cancellations would otherwise distort the percentiles of real runs.
- **Counts come from histograms.** A histogram's `_count` already counts its events, so there is no
  separate counter for them (finished tasks are `task_agent_task_duration_seconds_count`, completed
  flows `flow_system_flow_duration_seconds_count`). Fewer metrics to keep consistent, and alerts on
  one metric.
- **Recorded on save, not on commit.** Flow completions and projector states are recorded inside
  their transaction (only the projector instance built there knows its name), so a rolled back
  transaction may count what did not happen, and a failing commit alone does not mark a projector
  failing. Metrics serve rates and alerts, not accounting; outcomes are persisted in the stores.
- **Pre-created label sets.** Counters and histograms are created at `0` for all known label values
  at startup (planners, flow controllers, outbox routes; projectors on their first batch).
  `increase()` over a series that appears only at its first increment misses that increment, so a
  first failure would otherwise not alert.
- **Buckets fit the measured spans.** Task duration and queue wait:
  `1s … 6h` (`1, 5, 15, 60, 300, 900, 1800, 3600, 7200, 21600`), since tasks range from no-op
  polls to large ingests. Flow duration adds `12h, 24h` for retry backoff. Activation delay:
  `10ms … 5min` (`0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300`), since activations are normally
  sub-second late. Flow retries: `0, 1, 2, 3, 5, 10` — `le="0"` separates flows that needed none.
- **Agent metrics use `SystemTimeSource`.** Task and flow timestamps and durations use the
  injected time source, so tests with `FakeSystemTimeSource` assert exact values; in production it
  is the wall clock. The wakeup heartbeat uses the wall clock directly, as waits run on real time on
  every backend.
- **Timestamps, not ages.** Gauges hold Unix times of the last event (`time() - x` in PromQL)
  rather than ages that would only update when the agent does — a hung agent could not update them.

---

## 4. Recommended alerts

Thresholds are starting points; tune them to the deployment (longest normal ingest,
`maxListeningTimeout`, scrape interval).

### Page — stuck, will not recover by itself

| Alert | Expression | Why |
| --- | --- | --- |
| Task stuck | `task_agent_running_task_started_timestamp_seconds > 0 and time() - task_agent_running_task_started_timestamp_seconds > 7200` | Fires per `executor`. An executor runs one task at a time: a stuck task blocks every flow waiting for it. Set above the p99 of `task_agent_task_duration_seconds{outcome="success"}` |
| Agent loop hung | `time() - wakeup_listener_last_wait_timestamp_seconds{agent!="dev.kamu.domain.task-system.TaskAgent"} > 5 * <maxListeningTimeout>`, and for the task agent `time() - wakeup_listener_last_wait_timestamp_seconds{agent="dev.kamu.domain.task-system.TaskAgent"} > 5 * <maxListeningTimeout> unless on() (max(task_agent_running_task_started_timestamp_seconds) > 0)` | Agents wait at least every `maxListeningTimeout`; no wait means the loop is blocked. A busy task agent does not wait, and "Task stuck" covers it |
| Projector failing | `flow_system_event_projector_failing > 0`, `for: 5m` | The projection is stuck on a batch: flow process states (UI, stop policies) go stale. `for` rides over transient errors, as every wakeup retries |
| Outbox consumer failed | `outbox_failed_consumers_total > 0` | The consumer stopped until restart; its producer's messages pile up for it |
| Metrics missing | `absent(wakeup_listener_last_wait_timestamp_seconds)` | Scraping or wiring is broken — every other alert is silently off |

### Warning — degraded, still working

| Alert | Expression | Why |
| --- | --- | --- |
| Task failure rate | `sum by (plan_type) (increase(task_agent_task_duration_seconds_count{outcome="failed"}[30m])) / sum by (plan_type) (increase(task_agent_task_duration_seconds_count[30m])) > 0.2` and at least a few failures | Failing ingests / transforms, by kind |
| Flow failure rate | `sum by (flow_type) (increase(flow_system_flow_duration_seconds_count{outcome="failed"}[30m])) / sum by (flow_type) (increase(flow_system_flow_duration_seconds_count[30m])) > 0.2` and at least a few failures | Failures after retries — what users see. A spike points at configuration or an external dependency |
| Flow activations failing | `increase(flow_agent_activations_total{outcome="failed"}[15m]) > 0` | A failed flow is retried every `awaitingStepSecs`; a lasting increase means a flow stuck in retries |
| Scheduler lagging | `histogram_quantile(0.95, rate(flow_agent_activation_delay_seconds_bucket[10m])) > 60`, `for: 15m` | Slow database, exhausted connection pool, or too low `concurrency.flowActivations` |
| Task slot saturated | `histogram_quantile(0.95, rate(task_agent_task_queue_wait_seconds_bucket[30m])) > 900`, `for: 30m` | More work than one task slot handles — a capacity signal |
| Outbox backlog growing | `outbox_messages_pending_total > 1000`, `for: 15m` | Consumers fall behind producers |

### Dashboards only

- Task duration p95 by `plan_type` of successful tasks against `offset 1d` — regressions.
- `rate(flow_agent_activations_total{outcome="skipped"}[5m])` — near zero on one instance; growth
  means concurrent writers to the same flows.
- `rate(outbox_messages_processed_total[5m])` per consumer — throughput.
- Flow flakiness: share of successful flows that needed retries,
  `1 - sum by (flow_type) (increase(flow_system_flow_retries_bucket{le="0", outcome="success"}[1d])) / sum by (flow_type) (increase(flow_system_flow_retries_count{outcome="success"}[1d]))`.
- Successful flow duration p95 by `flow_type` against `offset 1d` — end-to-end regressions.
- `increase(flow_system_flows_aborted_total[1h])` — abort spikes, e.g. mass trigger removals.

---

## 5. Recipe: adding metrics

Follow `OutboxAgentMetrics` or `TaskAgentMetrics`:

1. A struct with public `prometheus` metric fields, next to the component it measures:

   ```rust
   #[component(pub)]
   #[interface(dyn MetricsProvider)]
   #[scope(Singleton)]
   impl TaskAgentMetrics {
       pub fn new() -> Self { /* HistogramVec::new(HistogramOpts::new(name, help), &labels) ... */ }
   }

   impl MetricsProvider for TaskAgentMetrics {
       fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
           reg.register(Box::new(self.task_duration_seconds.clone()))?;
           Ok(())
       }
   }
   ```

2. Recording methods (`on_task_finished(...)`) on the struct, so call sites stay one line and label
   values are spelled in one place. Pre-create label sets in an `init` method called at startup.
3. Inject it as `Arc<...Metrics>` where it is recorded; register it once — in the domain's
   `register_dependencies`, or in `src/app/cli/src/app.rs` for utility crates.
4. Test catalogs that build the component directly must add the metrics component too. Assert
   through harness helpers reading the fields (`.get()`, `.get_sample_count()`), e.g.
   `TaskAgentHarness::finished_tasks`.
5. Add the metric to [§2](#2-catalog), and an alert to [§4](#4-recommended-alerts) if it answers an
   alertable question.

Naming: `<component>_<what>_<unit>` with Prometheus suffixes — `_total` for counters, `_seconds`
for durations, `_timestamp_seconds` for Unix times.

---

## 6. File reference map

| What | File |
| --- | --- |
| `MetricsProvider`, `register_all`, `/system/metrics` handler | `src/utils/observability/src/metrics.rs` |
| `WakeupListenerMetrics` | `src/utils/wakeup-listener/src/wakeup_listener_metrics.rs` (recorded in `hub_wakeup_listener.rs`) |
| `TaskAgentMetrics` | `src/domain/task-system/services/src/task_agent_metrics.rs` |
| `FlowAgentMetrics` | `src/domain/flow-system/services/src/flow/flow_agent_metrics.rs` |
| `FlowCompletionMetrics` | `src/domain/flow-system/services/src/flow/flow_completion_metrics.rs` (recorded in `flow_agent_impl.rs`, `flow_abort_helper.rs`) |
| `FlowSystemEventAgentMetrics` | `src/domain/flow-system/services/src/flow_system_events/flow_system_event_agent_metrics.rs` |
| `OutboxAgentMetrics` | `src/utils/messaging-outbox/src/agent/outbox_agent_metrics.rs` |
| `S3Metrics` | `src/utils/s3-utils/src/s3_metrics.rs` |
| Registry, `--metrics` dump | `src/app/cli/src/app.rs` |
| Endpoint routing | `src/app/cli/src/explore/api_server.rs`, `web_ui_server.rs` |
