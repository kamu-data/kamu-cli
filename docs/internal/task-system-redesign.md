# Task System Redesign — Tasks as Resources  <!-- omit in toc -->

> **Status:** design discussion, nothing implemented. Plans how the [task system](task-system.md) moves onto the [resources framework](resources-framework.md) to follow ODF RFC-018 ([IaC Resource Framework](https://github.com/kamu-data/open-data-fabric/pull/126)) and RFC-019 ([IaC Task and Flow System](https://github.com/kamu-data/open-data-fabric/pull/131)).

---

## Table of contents  <!-- omit in toc -->

- [1. Proposal](#1-proposal)
  - [1.1 Key Components](#11-key-components)
  - [1.2 Storage](#12-storage)
  - [1.3 Scheduling](#13-scheduling)
  - [1.4 Execution](#14-execution)
  - [1.5 Completion](#15-completion)
  - [1.6 Cancellation](#16-cancellation)
  - [1.7 Ownership and security context](#17-ownership-and-security-context)
  - [1.8 Retries](#18-retries)
  - [1.9 Progress signalling](#19-progress-signalling)
  - [1.10 Multiprocessing](#110-multiprocessing)
  - [1.11 Migration](#111-migration)
- [2. Resource system changes](#2-resource-system-changes)
- [3. Q\&A](#3-qa)

---

## 1. Proposal

### 1.1 Key Components

The task system has five layers:

| Layer | Role |
| --- | --- |
| `Task` resource | The `spec` contains the intent, and `status` holds everything learned while elaborating and executing it |
| Task Controller | Admits new tasks: validates inputs and resolves what scheduling needs, such as the engine a transform runs on, then enqueues them. Also expires claims ([§1.10](#110-multiprocessing)), deletes finished tasks ([§1.5](#15-completion)) and handles cancellations ([§1.6](#16-cancellation)) |
| Task Queue | A dedicated system that schedules and prioritizes queued tasks ([§1.3](#13-scheduling)) |
| Task Worker | A controller that specializes in one task type and pulls the next queued task of that type. It claims the task, executes it step by step, saves progress into the task's `status` conditions, commits, and writes `TaskOutcome` |
| Engines | Workers delegate work to a pool of separately provisioned engines when needed |

**Workers are smart.** Workers have access to the database and core services. They can interact with resources, task queue, read metadata, and commit new blocks.

**Engines are dumb.** The work that workers delegate to engines should be fully broken down, as those will only have access to storage but not any node services.

**Tasks are maximally atomic.** While due to full access worker logic can be arbitrarily complex, workers are encouraged to keep tasks simple and atomic. As a general rule tasks should commit once at the end so that state of the system is either changed or not, avoiding partial completion.

**The generic task lifecycle is `Pending → Queued → Running → Finished`.** Everything between a worker claiming the task and writing outcome is type-specific: fetching for an ingest, elaborating the plan for a transform, requesting an engine and dispatching to it. Each type records its progress in its own conditions, such as `TaskPlan`, as each step completes.

**Progress is saved per major step.** As workers progress on task execution they update the plan in the resource conditions. On top of this state they may implement complex state machines. Upon restart workers may resume execution from the persisted state.


### 1.2 Storage

**Task state lives in resource status conditions.** `Task` and `FlowRun` are ordinary resources in the shared resource tables ([resources-framework.md](resources-framework.md#6-persistence-model)), with no task-specific store. Everything known about a task — `TaskStatus`, `TaskPlan`, type-specific progress and `TaskOutcome` — is a condition in its status, written through the resource's event stream.

**The queue is a separate mechanism.** It has its own storage, optimized for workers taking the next task of their type, and changes in the same transaction as the task it points to ([§1.3](#13-scheduling)). The resource tables never serve the "next task" query.


### 1.3 Scheduling

**The queue holds admitted tasks waiting for a worker.** The Task Controller enqueues a task once it is admitted. A queue entry carries what scheduling needs without loading the task: task type, target, owning account, enqueue time, and hints resolved at admission, such as the engine a transform runs on.

**The queue changes in the same transaction as the task.** An entry is added in the transaction that moves the task to `Queued` and removed in the one that moves it to `Running`, so the queue and task statuses never disagree.

**Workers pull tasks by type.** A worker asks for the next task of its type, optionally narrowed further, e.g. by engine. The queue decides which task that is.

**Workers claim only what they can process right away.** A worker claims a task only when it is certain there is enough downstream capacity to service it, e.g. a free engine. Tasks that cannot be processed immediately stay in the queue.

Ordering starts as FIFO per task type. Priorities, fairness between accounts, and limits on concurrently running tasks per type or per target ([§1.10](#110-multiprocessing)) are TBD.


### 1.4 Execution

**A worker runs a loop.** It waits for a signal, claims the next task of its type from the queue ([§1.3](#13-scheduling)), executes it, and repeats. A node may run several workers of each type ([§1.10](#110-multiprocessing)).

**Execution of a task type is an async Rust function.** The function is the type's state machine: each step that completes is saved into the task's status conditions.

**Transactions are short and never span long work.** Database reads and writes happen in short transactions between steps; engine work, fetching and data processing happen outside any transaction.

**The commit and the outcome are written in one transaction.** A crash therefore cannot leave a committed block without a recorded outcome.

**Cancellation is checked during the status updates.** Saving the progress provides a natural place to detect requested task cancellation.

Example of a `Transform` task worker in pseudocode:

```rust
async fn run(&self, queue: &TaskQueue) -> Result<()> {
    loop {
        let task = queue.claim_next::<TransformSpec>().await?;
        self.execute_transform(task).await?;
    }
}

async fn execute_transform(&self, task: ClaimedTask<TransformSpec>) -> Result<()> {
    // 1: Prepare operation
    let plan = transaction! {
        let plan = self.build_execute_transform_request(task).await?;
        self.save_plan(&task, &plan).await?; // checks for cancellation
        plan
    };

    // 2: Run engine
    let engine = self.engine_provisioner.provision(&plan.engine).await?;
    let engine_response = engine.execute_transform(&plan.request).await?;
    
    // 3: Commit
    transaction! {
      let new_head = self.commit(&task, &engine_response).await?;
      let outcome = TaskOutcome::success(TransformResult::new(
        new_head,
        engine_response,
      ));
      self.save_outcome(&task, outcome).await?;
    }

    Ok(())
}
```

Because the result of every completed step is saved in the task's conditions, a worker that picks the task up again after a crash can resume from the last saved step instead of starting over.


### 1.5 Completion

**Finished tasks are deleted at once.** The Task controller deletes a task as soon as its outcome is recorded, so the live `Task` resources are exactly the work that is pending, queued or running.

**Deleted tasks stay readable.** A tombstone already keeps the whole snapshot, `status` included, so a caller can still read the manifest and its `TaskOutcome` until the retention period ends.

Note that good UX would require:
- ability to read deleted resources
- ability to list deleted resources e.g. `kamu get tasks --deleted`
- a purge of tombstones by configurable retention period
- ideally: keeping the original name


### 1.6 Cancellation

**Cancelling a task is done by deleting the resource.** There is no separate cancel action and no spec field for it, so the spec stays untouched after creation.

**Delete is not immediate.** When deleting a running task only `deletionRequestedAt` header is set and task moves to `Deleting` phase. Task woker may notice it and cancel the task, or drive it to completion. After the either outcome is recorded the controller will finalize the deletion of the task resource.

This is a resource framework feature. This is similar to Kubernetes model of finalizers.


### 1.7 Ownership and security context

Tasks are created under user accounts and execute in their security context.

When we have organizations - we'll need to specify `serviceAccount` on tasks and flows to provide a valid principal for execution (as orgs cannot act themselves).


### 1.8 Retries

Retries exist at two levels with different purposes:

| Level | Purpose | Scope |
| --- | --- | --- |
| Task | Absorb short transient failures of an external party, such as an HTTP 500 from a webhook destination or a polled source | Type-specific, inside the worker; seconds, not hours |
| Flow | Recover from long-standing problems, and eventually stop a misbehaving flow | Generic: a failed run is retried as a new `FlowRun` under the flow's `retryPolicy` |

**Task-level retries are not a generic mechanism.** Only task types that call external parties retry, and only within the worker's execution. The total retry time stays short, so a retrying task does not tie up its worker.

**Flow-level retries are the generic mechanism.** A run that fails recoverably is retried as a new `FlowRun` linked through `FlowRunRetry.retryOf`. A flow that keeps failing is stopped, which needs a stop policy that RFC-019 does not define yet.

**A failure says whether it is worth retrying.** RFC-019's `Failed` outcome carries a required `recoverable` flag.


### 1.9 Progress signalling

**Flow controllers learn about tasks from resource messages.** There is no task-specific message like today's `TaskProgressMessage` ([task-system.md](task-system.md#10-integration-with-the-flow-system)); the `FlowRun` controller consumes the generic `ResourceLifecycleMessage` and filters it to the `Task` schema. It finds the run a task belongs to through the task's `ownerReferences`.


### 1.10 Multiprocessing

Several workers may run at once — threads of one node process, or dedicated node instances sharing its database — and several tasks may target the same resource.

Coordination will require:
- Workers being associated with a task they have claimed
- Detecting workers that crashed to return the task back to the queue
- Avoiding running tasks on the same target that have very high likelihood of conflicting (e.g. `Transformed` and `Reset` at the same time)

Exact mechanisms for this are TBD.


### 1.11 Migration

**Flow and task history is wiped, not migrated.** On upgrade, existing tasks, flow runs and their events are dropped; nothing is backfilled into `Task` or `FlowRun` resources.

Migrating flow configurations and triggers into `Flow` resources is out of scope here and is designed separately.

---

## 2. Resource system changes

Features the resources framework ([resources-framework.md](resources-framework.md)) needs for tasks and runs. None is task-specific: each is a generic mechanism that tasks are the first to use.

| Feature | Needed for | Today |
| --- | --- | --- |
| Two-step deletion: a deletion request that keeps the resource visible, completed once its controller finalizes it. RFC-018 records the steps in the `deletionRequestedAt` and `deletedAt` headers and reports them as the `Deleting` and `Deleted` phases, which the framework sets over the controller's phase | Cancelling a running task, which must still record its outcome ([§1.6](#16-cancellation)), and telling deleted tasks and runs from live ones at a glance | Delete is a single step that hides the resource at once, and the phase of a deleted resource stays as it was |
| Reading deleted resources by `id`, and a search that includes deleted resources (`kamu get tasks --deleted`) | Inspecting outcomes of finished and cancelled tasks ([§1.6](#16-cancellation), [§1.5](#15-completion)) | Every read filters out deleted resources |
| Keeping a deleted resource's original name when the type's names never collide | Showing deleted tasks and runs under their own names ([§1.6](#16-cancellation)) | Delete renames to a tombstone name to free the name |
| Purging deleted resources together with their events, after a retention period configured per resource type | Bounding the storage taken by finished tasks and runs ([§1.2](#12-storage)) | Tombstones and events are kept forever |
| Server-generated names when a manifest omits one | Tasks and runs created in bulk by controllers ([§1.7](#17-ownership-and-security-context)) | A name is required |
| Persisted `ownerReferences`, with cascading deletion of dependents | Cancelling a run together with its tasks ([§1.5](#15-completion)) | Accepted in headers but not stored, and nothing cascades |
| A `statusGeneration` header, defined in RFC-018, that increments on every status update — as `generation` does for headers and spec — and is accepted as a precondition of a status update, which fails on a mismatch | Compare-and-swap status updates that fence off stale workers ([§1.10](#110-multiprocessing)), and API clients telling whether a resource changed since they last read it | Not tracked; the event store's `last_event_id` is internal |
| Status condition updates by controllers other than the main one, outside the reconcile cycle, with an expected `statusGeneration` | Workers saving task progress ([§1.1](#11-key-components)) | Status changes only through the reconcile use case's events |
| A lifecycle message for status changes, posted in the same transaction as the change | Flow controllers following task progress ([§1.9](#19-progress-signalling)) | Messages cover apply, reconciliation and deletion only |
| An internal apply path that controllers call in their own transaction, without the facade's account and authorization checks | Controllers creating runs and tasks ([§1.7](#17-ownership-and-security-context)) | Account resolution and permission checks live in the facade ([resources-framework.md](resources-framework.md#7-account-resolution--authorization)); a path for controllers is not verified |
| Recording the subject that caused each event | Auditing who created or changed a resource ([§1.7](#17-ownership-and-security-context)) | Not verified |

Outside the resources framework, the auth system needs a service-account type of `Account` and a permission to act as one ([§1.7](#17-ownership-and-security-context)).

## 3. Q&A

**Does the resource reconcile model describe a task?** No. The Task controller only admits tasks and puts them into the queue; workers specialized per task type drive execution — see [§1.1](#11-key-components).

**What does `phase` mean for a task?** Admission by the Task controller: `Ready` means admitted, `Failed` means rejected. On deletion the framework sets `Deleting` and `Deleted`. Execution is reported in the `TaskStatus` condition — see [RFC-019](https://github.com/kamu-data/open-data-fabric/pull/131).

**Must every task type split into plan, execute and commit?** No. Each worker defines its own steps and commits as part of execution — see [§1.4](#14-execution).

**Is the execution plan computed before the task is queued?** No. The worker elaborates it step by step and saves each step into the task's conditions — see [§1.4](#14-execution).

**How do today's task types map onto RFC-019?** Update dataset becomes `Ingest` or `Transform`, sync becomes `SyncFrom` or `SyncTo`, hard compaction becomes `Compaction`, reset becomes `Reset`, webhook delivery becomes `WebhookCall`, and the probe placeholder of system GC becomes `GarbageCollection`. `Verify` is new; reset to metadata has no task kind yet.

**Can two tasks on the same resource run at once?** Conflicting tasks on the same target should not run at once; the mechanism is TBD — see [§1.10](#110-multiprocessing).

**Where is task state stored?** In the status conditions of `Task` resources, in the shared resource tables. The queue is a separate mechanism — see [§1.2](#12-storage).

**How is a task cancelled?** By deleting its `Task` resource. A running task moves to `Deleting` until its worker records an outcome, which tells whether it was cancelled or ran to completion — see [§1.6](#16-cancellation).

**What happens to finished tasks?** They are deleted at once and stay readable until a retention period ends — see [§1.5](#15-completion).

**Where do retries live?** At two levels: short type-specific retries inside a task for failures of external parties, and generic retries of recoverable failures at the `FlowRun` level — see [§1.8](#18-retries).

**How do flow controllers monitor task progress?** Only via `Task` resource messages, which need a status-change variant — see [§1.9](#19-progress-signalling) and [§2](#2-resource-system-changes).

**Which account owns a task, and whose permissions does it execute with?** The account that owns its `Flow` and `FlowRun`. It executes as that account's user, or as the service account named in `serviceAccount` — see [§1.7](#17-ownership-and-security-context).

**Whose permissions does a task execute with when `Flow` belongs to an organization?** Those of the service account named in `spec.serviceAccount`, as organizations cannot be principals — see [§1.7](#17-ownership-and-security-context).
