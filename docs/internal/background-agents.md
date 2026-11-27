# Background Agents — Hosting

> **Status:** stable. How long-running background agents are written, supervised and shut down.
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Quick-start

**One-paragraph mental model.** A **background agent** is a loop that runs for the whole life of
the server: the outbox agent, the flow agent, the flow system event agent, the task agent, the
resource controller runtime, and one agent per housekeeping job
([housekeeping.md](housekeeping.md)). Each implements `BackgroundAgent`, and
`BackgroundAgentHost` runs them all (crate `background-agent-host`). The host supervises every
agent on its own: a run that fails, panics or returns early is restarted after a growing backoff
while the other agents keep running.
On a shutdown, the host cancels a token every agent watches, gives them a bounded time to finish
their current step, then aborts the ones still running.

| You want to… | Start at |
| --- | --- |
| Write a new agent | [§1 The contract](#1-the-contract) and [§5 Recipe](#5-recipe-adding-an-agent) |
| Know what happens when an agent fails | [§3 Supervision](#3-supervision) |
| Know what a shutdown waits for | [§4 Shutdown](#4-shutdown) |
| Know what runs after a CLI command | [§2 Where agents run](#2-where-agents-run) |
| Tune restarts or the shutdown bound | [§4 Shutdown](#4-shutdown), [wakeup-listeners.md](wakeup-listeners.md#7-configuration) |

---

## Table of contents

- [1. The contract](#1-the-contract)
- [2. Where agents run](#2-where-agents-run)
- [3. Supervision](#3-supervision)
- [4. Shutdown](#4-shutdown)
- [5. Recipe: adding an agent](#5-recipe-adding-an-agent)
- [6. Rejected approaches](#6-rejected-approaches)
- [7. File reference map](#7-file-reference-map)

---

## 1. The contract

| Part | Contract |
| --- | --- |
| `agent_name()` | A stable name: the `agent` label of the host's metrics and of `wakeup_listener_last_heartbeat_timestamp_seconds` (the resource controller runtime labels its heartbeats per controller instead) |
| `run(&self, shutdown: CancellationToken)` | Runs until `shutdown` is cancelled, then returns `Ok`. The agent checks the token between steps and stops waiting for work when it is cancelled; it never cuts a step short itself |
| Returning `Ok` before a shutdown | An unexpected completion: the host restarts the agent |
| Returning `Err`, or panicking | A failure: the host restarts the agent, unless the failure is fatal |
| `is_failure_fatal()` | `false` by default. `true` makes any failure of the agent stop the host and every other agent |
| A new run | Starts from storage: whatever the previous run left behind (a claimed task, a half-done drain) is found again. State the singleton agent keeps in memory survives, such as the outbox's failed consumers |

A **step** is the unit of work an agent never abandons on its own: a batch of a catch-up, a flow
activation tick, a task, a controller drain. Waits around steps are always droppable, as they only
read; `CancellationToken::run_until_cancelled` wraps them.

| Agent | Steps | Stops |
| --- | --- | --- |
| Outbox agent | each batch of a catch-up: the initial one, then one per wakeup | between batches, and while waiting for messages |
| Flow system event agent | each projector batch of a catch-up: the initial one, then one per wakeup | between batches, and while waiting for events |
| Flow agent | activating the due flows | while waiting for the next activation moment |
| Task agent | taking a task, and running it | while waiting for a task; never between taking and running one |
| Resource controller runtime | each controller loop's drain | after each loop's drain, in its wait; a loop that fails then is not restarted |
| Housekeeping agent | each batch of a pass ([housekeeping.md](housekeeping.md#3-the-agent)) | between batches, in the pause between them, and in the sleep after a pass |

For example, the task agent requeues tasks left `Running` on every run
([task-system.md](task-system.md#startup-recovery)).

---

## 2. Where agents run

| Process | Agents |
| --- | --- |
| `kamu system api-server` | all of them, on `BackgroundAgentHost` (`src/app/cli/src/explore/api_server.rs`) |
| `kamu ui` | none: flows, tasks and resource controllers do not run there |
| One-shot CLI commands | none: `BackgroundWorkFinalizer` completes the work the command caused before it exits, below |

Nothing is finalized after the API server stops (`command_needs_background_work` excludes it), so
a shutdown stays within its bound; the next start picks up what is left.

### Finalizing background work without agents

`BackgroundWorkFinalizer` does, on demand, what the agents would do for a caller that does not run
them or does not wait for them. It has two phases with different contracts:

| Phase | What it does | A failure |
| --- | --- | --- |
| **Settle** | Drains every registered `BackgroundWorkDrain` in turn until one full pass finds all of them idle | Is returned, as the caller's effects are incomplete; it fails a command that succeeded |
| **Housekeeping** | One batch of each enabled housekeeping job, then a second settle if any batch removed items, for the messages it posted ([housekeeping.md](housekeeping.md#4-after-a-cli-command)) | Is logged as a warning; the next finalization retries it |

The outbox agent and the resource controller runtime are drains. A settle alternates them because
each feeds the other: the outbox delivers messages that make resources actionable, and processing
them posts messages of its own.

| Caller | Settle | Housekeeping |
| --- | --- | --- |
| A CLI command (`finalize_background_work_after_command`), whether it succeeded or failed, as a failed command may have committed work of its own | yes | yes |
| The e2e API server, after a mutating request (`e2e_middleware.rs`) | yes | no: the agents run in the server |
| Test harnesses, after a write (`settle`) | yes | when the test asks for it |

A drain reports `Idle` only when no runner of its work, its own session or the agent, did any work
since the session's previous drain, so a settle running next to the agents ends once the agents
are idle too. How each drain does this: [outbox.md](outbox.md#5-the-outbox-agent) and
[resources-framework.md](resources-framework.md#controller-runtime). A drain works on the catalog
it is registered in, so callers finalize the catalog that has both the drains and the
housekeeping jobs; a catalog without the outbox agent, such as one with an immediate outbox, has
no outbox drain.

Agents run in the background catalog, as `Principal::Node` ([identity.md](identity.md)).

---

## 3. Supervision

`BackgroundAgentSupervisor` runs each agent in a task of its own, so a panic ends that run and not
the host.

| Run ended | Logged | Then |
| --- | --- | --- |
| `Ok` after a shutdown | info, "stopped" | the supervisor ends |
| `Ok` before a shutdown | warning | restart, `reason="completed"` |
| `Err` | error, with the error | restart, `reason="failed"`; a fatal agent stops the host instead |
| Panic | error, with the panic text | restart, `reason="panicked"`; a fatal agent stops the host instead |
| `Err` or panic after a shutdown | error, as above | the supervisor ends: no restart, and not fatal |
| Aborted after `shutdownTimeout` | warning, "did not stop in time" | the supervisor ends |

The delay before a restart is `backgroundAgents.restartBackoff.delay(n)` for the `n`-th consecutive
restart ([defaults](wakeup-listeners.md#7-configuration)). A run that lasted at least `maxDelay` counts as
healthy and resets `n` to 1. A shutdown cuts a restart delay short.

**Metrics** ([metrics.md](metrics.md#background-agent-host)): whether each agent is running,
restarts by reason, and the time of its last failure.

---

## 4. Shutdown

`api-server` cancels one token on Ctrl+C, on SIGTERM, on the e2e `/e2e/shutdown` route, when its
HTTP server ends unexpectedly, or when an agent fails fatally. The same token stops the HTTP server
from accepting requests and every agent. Once the stop has begun, another Ctrl+C or SIGTERM exits
the process at once with status 1, for a stop that hangs, such as an agent blocked in code an abort
cannot interrupt.

```mermaid
sequenceDiagram
    participant S as api-server
    participant H as BackgroundAgentHost
    participant V as Supervisors
    participant A as Agents

    S->>H: cancel the shutdown token
    H->>V: (token seen by every supervisor)
    V->>A: (token seen by every run)
    A-->>V: return after the current step
    alt all returned within shutdownTimeout
        V-->>H: ended
    else bound reached
        H->>V: cancel the abort token
        V->>A: abort the run, wait for its task to end
        V-->>H: ended
    end
    H-->>S: Ok, or the fatal error
```

- Agents and in-flight requests share `backgroundAgents.shutdownTimeout` ([default](wakeup-listeners.md#7-configuration)). For
  requests it bounds graceful draining: connections still open when it expires are cancelled as
  the process exits.
- The host returns only once every supervisor and every agent's own task has ended. Tasks an
  agent spawned itself (controller loops, outbox consumers) are only asked to abort; they end
  with the process, which exits right after.
- The host collects every supervisor's result, aborted or not, so a fatal error is returned even
  when the bound is reached at once.
- An aborted step leaves storage as a crash would; the next start recovers it like any other
  interrupted work.

---

## 5. Recipe: adding an agent

1. Implement `BackgroundAgent` on a `#[scope(Singleton)]` component bound as
   `dyn BackgroundAgent`. Resolve repositories per call through `CatalogWeakRef` (`kamu-dill-di`,
   "Singletons must not depend on repositories").
2. In `run`, create the agent's wakeup listener once, before the loop
   ([wakeup-listeners.md](wakeup-listeners.md)), then loop: one step, then a wait that a shutdown
   ends, returning `Ok` when it yields `None`. A plain wakeup wait is
   `WakeupListener::wait_wake_until_shutdown`; a wait combined with a deadline goes inside
   `shutdown.run_until_cancelled(...)`.
3. Make each step safe to repeat after an abort or a crash: keep its state in storage and commit
   it in short transactions.
4. Return `Err` for failures a restart may fix. Override `is_failure_fatal()` only when running on
   without the agent is worse than stopping the server.
5. Register the component in `app.rs`; the host picks it up from the catalog.
6. Test that `run` returns `Ok` once the token is cancelled while the agent waits, and that a step in
   progress finishes first.

---

## 6. Rejected approaches

| Approach | Why it was rejected |
| --- | --- |
| Stopping the server when any agent fails, panics or returns | One faulty agent took every other agent and the API down with it |
| Dropping the supervisors after the shutdown bound | Dropping a task set only requests abortion: the host could return while agent tasks still held transactions, and an aborted agent's running gauge stayed at 1 |
| Waiting for the agents and the bound in one unbiased `select!` | With a bound already due, the timeout branch could win before a fatal supervisor's error was collected, and the host returned `Ok` |
| Restarting the task agent without recovering on each run | A task taken by a failed run stayed `Running` until the next process start |
| The trait in `async-utils` and the host in a crate of its own | Splits one concept across crates; the host's dependencies (prometheus, dill, time source) do not belong in `async-utils` either |

---

## 7. File reference map

| What | File |
| --- | --- |
| `BackgroundAgent`, `CancellationToken` | `src/utils/background-agent-host/src/background_agent.rs` |
| `BackgroundAgentHost` | `src/utils/background-agent-host/src/background_agent_host.rs` |
| `BackgroundAgentSupervisor` | `src/utils/background-agent-host/src/background_agent_supervisor.rs` |
| `BackgroundAgentHostConfig` | `src/utils/background-agent-host/src/background_agent_host_config.rs`, mapped from `backgroundAgents` in `src/app/cli/src/services/config/models.rs` |
| `BackgroundAgentHostMetrics`, `BackgroundAgentRestartReason` | `src/utils/background-agent-host/src/background_agent_host_metrics.rs`, `background_agent_restart_reason.rs` |
| Shutdown sources, HTTP server bound | `src/app/cli/src/explore/api_server.rs` |
| `BackgroundWorkFinalizer`, `BackgroundWorkDrain` | `src/utils/background-agent-host/src/finalizer/`; the CLI's call in `src/app/cli/src/background_work.rs` |
| `HousekeepingAgent` and the jobs | [housekeeping.md](housekeeping.md#12-file-reference-map) |
| Host and finalizer tests | `src/utils/background-agent-host/tests/tests/test_background_agent_host.rs`, `test_background_work_finalizer.rs` |
