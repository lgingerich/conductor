# Temporal: Architectural Deep-Dive

> Competitor analysis for building a next-generation data orchestration tool with a push-based execution model and WASM container runtimes.

---

## 1. Overview & Positioning

Temporal is a general-purpose durable execution platform that guarantees code runs to completion despite failures, restarts, and infrastructure outages. It is not a data orchestrator in the tradition of Airflow, Prefect, or Dagster. It is a workflow engine born from microservice orchestration at Uber, and its core abstraction — durable execution through event sourcing — sets it fundamentally apart from task-scheduling-based orchestrators.

### History

The lineage of Temporal traces back to Amazon's Simple Workflow Service (SWF), which Maxim Fateev helped build as an internal project around 2010. SWF was the first system to introduce the idea of recording workflow state as an event history and using replay for fault tolerance, but it suffered from poor developer experience. Samar Abbas later built the Durable Task Framework at Microsoft, which evolved into Azure Durable Functions — another incarnation of the same idea. In 2015, Fateev and Abbas both joined Uber's Seattle office and, in 2017, launched Cadence as an open-source project. Cadence was built from scratch on a different software stack, designed from the beginning as open-source, and grew via bottoms-up adoption inside Uber from zero to over a hundred use cases in three years. External companies like HashiCorp, Box, Coinbase, and Checkr adopted it independently.

By 2019, Fateev and Abbas recognized that Uber would never offer a cloud-hosted version of Cadence, and that the codebase had accumulated significant technical debt from four years of backwards-compatible-only changes in production. They left Uber in late 2019 to found Temporal as a company, forked the Cadence codebase in a non-backwards-compatible manner, and spent nearly a year reworking the architecture before issuing their first production release. Temporal has since diverged substantially from Cadence, though the two projects share the same architectural DNA.

### Core Design Philosophy

Temporal's fundamental insight is that execution state can be captured as an append-only log of events, and that replaying this log through deterministic code can reconstruct the exact program state at any point in time. This is event sourcing applied to program execution, not just data. The platform treats a workflow not as a series of scheduled tasks but as a deterministic program whose every decision is recorded. If a worker crashes, another worker picks up the event history, replays the workflow code from the beginning (using cached results for already-completed activities and side effects), and arrives at the identical program state — then continues execution from exactly where the previous worker left off.

This gives Temporal its defining properties: workflows can run for years, survive any transient failure, and never lose state. The workflow is not a configuration file, not a DAG definition, and not a YAML specification of steps. It is actual code — a function written in Go, Java, TypeScript, Python, .NET, or PHP — that the platform guarantees will complete.

### How It Differs From Data Orchestrators

Data orchestrators like Airflow, Prefect, and Dagster are fundamentally task schedulers. Airflow defines workflows as Directed Acyclic Graphs (DAGs) of tasks and is optimized for time-based, batch-oriented execution. It tracks task status in a metadata database and passes data between tasks through XComs or external storage. Prefect is Python-native and dynamic, allowing workflows to branch at runtime, but still operates on a scheduling model where a central scheduler decides what to run and when. Dagster is asset-centric, organizing work around data objects and their lineage rather than tasks, but again relies on a scheduler that materializes stale assets.

Temporal operates on an entirely different axis. It is not about scheduling work at particular times — though it has scheduling capabilities — it is about guaranteeing that code completes. Airflow asks "what should run at 3 AM?" Temporal asks "did this code finish, and if not, let's make sure it does." Where Airflow has sensors that occupy worker slots waiting for conditions, Temporal has signals that activate waiting workflows without consuming resources. Where Prefect retries failed tasks with configurable counts, Temporal guarantees infinite retries with configurable backoff for any activity, forever, because the workflow's execution state is persisted independently of any single worker.

This architectural difference means Temporal is suited for microservice orchestration, long-running business transactions (order fulfillment, payment processing, user onboarding spanning days), saga patterns for distributed transactions, CI/CD pipelines, infrastructure provisioning, AI agent orchestration, and anything where correctness across failure boundaries matters more than scheduled data movement. Many organizations that use Temporal for application workflows also use a separate tool like Airflow or Dagster for their data pipeline needs.

### SDKs

Temporal provides SDKs in Go, Java, TypeScript, Python, .NET, and PHP. The Go and Java SDKs are considered first-class, having the longest history and deepest integration. The TypeScript SDK is notable for its use of V8 isolates via the `isolated-vm` npm package to enforce determinism — each workflow runs in its own V8 isolate with non-deterministic built-ins like `Date` and `Math.random` replaced by workflow-safe versions. The Python SDK runs workflows on a custom asyncio event loop. The .NET SDK uses a custom `TaskScheduler` to control coroutine scheduling. The Rust SDK, still maturing, introduces WASM-based workflow execution via the wasmtime component model, which is a significant architectural development discussed later in this document.

---

## 2. Architecture Deep Dive

### The Temporal Server (Cluster)

The Temporal server is not a monolith. It consists of four independently scalable services, each with distinct responsibilities. These services communicate via gRPC and discover each other through a membership protocol based on Ringpop (derived from Uber's internal infrastructure). Understanding these four services and how they interact is essential to understanding Temporal's scaling characteristics and operational complexity.

#### Frontend Service

The Frontend service is the stateless gRPC API gateway for all external communication. Every client request — starting a workflow, sending a signal, querying workflow state, worker polling — enters through the Frontend. It handles rate limiting, authentication, and authorization before routing requests to the appropriate History or Matching service instance. Because the Frontend is stateless and has no sharding, it scales horizontally by simply adding more instances behind a load balancer. This is the simplest service to operate and the least likely to become a bottleneck.

#### History Service

The History service is the heart of Temporal. It owns individual workflow executions, persists mutable state and event history, and drives workflow execution by enqueuing tasks into the Matching service. The History service is sharded — the cluster is configured at creation time with a fixed number of history shards (typically 512, 1024, up to 16,384 for very large deployments). Each shard is a logical partition that owns a subset of workflow executions, determined by hashing the workflow ID. Each History service host owns multiple shards, and shard ownership is dynamically assigned and reassigned across hosts through the membership protocol.

This sharding model is both Temporal's scaling superpower and one of its most subtle constraints. Because each shard is a single-writer partition — only one History host owns a given shard at a time — all writes for workflows within that shard are serialized through that host's in-memory mutable state cache and then persisted. This means that the throughput of a single shard is bounded by the write throughput of a single History host and its database connection. Temporal mitigates this with the sheer number of shards, distributing load across many hosts, but for very high-throughput workflows on a single workflow ID, the single-shard bottleneck is real and unavoidable.

Each shard maintains several internal task queues that are critical to Temporal's internal operation:

- **Transfer Task Queue**: An "immediate" queue for tasks that need to be dispatched to the Matching service — scheduling a workflow task, scheduling an activity task, canceling a workflow, etc. When a transfer task is executed, an RPC is made to the Matching service to enqueue the corresponding task.
- **Timer Task Queue**: Durably persists timers. When a timer fires, it generates a transfer task to continue advancing the workflow that was waiting on it.
- **Replication Task Queue**: For multi-cluster deployments, manages replication of events to remote clusters.
- **Visibility Task Queue**: Manages writes to the visibility store for workflow search and listing.
- **Outbound Task Queue**: A newer addition for Nexus operations, handling cross-namespace and cross-cluster communication. Unlike the transfer queue which targets internal Matching, the outbound queue targets external destinations with circuit breaking and rate limiting.

When the History service starts on a host, it loads every shard owned by that host and starts per-shard queue processors for each queue type. The constant background processing of these queues is what gives Temporal its "always advancing" character — workflows don't just sit idle; the system is continuously evaluating timers, executing transfers, and moving work forward.

#### Matching Service

The Matching service manages task queues — the logical queues that workers poll for tasks. Despite the name "queue," task queues are not simple FIFO structures. They are partition-based matching engines. When a worker polls a task queue, the Matching service attempts to match that poller with a pending task. If no task is available, the poll becomes a long-poll (up to 60 seconds by default). When a task arrives and a poller is waiting, the match happens synchronously — this is called a "sync match" and is the low-latency path.

Task queues are split into partitions for throughput scaling (default 4, but configurable). Each partition can be hosted on a different Matching service instance, and partitions use consistent hashing for ownership. If a partition has tasks in its backlog but no pollers, it can "forward" tasks to a parent partition where pollers might be waiting. Conversely, pollers on empty partitions can be forwarded to parent partitions. This forwarding mechanism creates a tree structure converging at a root partition, which — if loaded — forces all sibling partitions to load as well, ensuring forwarding can always complete.

The separation between read and write partitions is an operational detail: you can reduce the number of write partitions first (new tasks go to fewer partitions), let existing partitions drain, and then reduce read partitions to match. This avoids stranding tasks in partitions that are being removed.

#### Worker Service

The Worker service runs internal background processing for the Temporal cluster itself — replication queue processing, system workflows, archival, and (in versions before 1.5.0) Kafka-based visibility processing. This is not to be confused with user-facing Workers that execute workflow and activity code. The internal Worker service is part of the cluster infrastructure and primarily handles cross-cutting concerns that don't belong to any single shard.

#### How Services Communicate

All inter-service communication within the Temporal cluster uses gRPC. The Frontend routes requests to the appropriate History or Matching host based on shard ownership or task queue partition ownership. Service discovery uses Ringpop, a consistent hashing-based membership protocol originally built at Uber. Ringpop maintains a hash ring of available hosts for each service, and when a host joins or leaves (due to scaling, failure, or deployment), the ring is updated and ownership is rebalanced.

This architecture means that every History shard and every Matching partition has exactly one owner at any given time, but that owner can change. The system is designed to handle ownership transitions gracefully — when a shard changes hands, its state is reloaded from the persistence store, and processing resumes.

### Persistence Layer

Temporal's only hard dependency is its persistence database. The system requires two logical stores:

**Default Store**: Stores workflow execution mutable state, event history, task metadata, and namespace metadata. Supports Cassandra (v3.11, v4.0), MySQL (v8.0+), and PostgreSQL (v12+). SQLite is supported for development only.

**Visibility Store**: Stores workflow metadata for search and listing operations. Supports MySQL (v8.0.17+), PostgreSQL (v12+), Elasticsearch, and OpenSearch. SQLite is supported for development. Cassandra cannot be used as a visibility store.

The choice of persistence backend has profound operational implications. Cassandra was the original and most battle-tested backend (it powered Cadence at Uber), and for very high-throughput deployments, it remains the recommended choice. However, operating Cassandra is notoriously difficult — it requires deep expertise, careful tuning, and dedicated operational attention. The Hacker News thread by a user running "large on-prem temporal" described Cassandra as a significant operational burden.

PostgreSQL and MySQL offer lower operational overhead since many teams already run managed instances, but they don't scale as well for Temporal's write-heavy workload. A load-testing report from Vymo Engineering found that PostgreSQL alone became a bottleneck at relatively modest throughput (100+ concurrent workflows causing CPU spikes over 120%), and they eventually migrated to Cassandra for persistence with Elasticsearch for visibility to achieve acceptable performance. Temporal's documentation candidly notes that PostgreSQL is "not ideal for medium-to-large-scale systems."

The event history table structure is an append-only log of events. Each event is stored as a serialized protobuf blob keyed by (namespace_id, workflow_id, run_id, event_id). The mutable state — the server's cached summary of workflow state — is stored separately and updated atomically with event history appends. The separation between mutable state and event history is critical: mutable state allows the server to answer queries about workflow state without replaying the entire history, while the event history serves as the durable source of truth for replay-based recovery.

The visibility store is a denormalized index of workflow metadata. The `executions_visibility` table stores columns like namespace_id, workflow_id, run_id, workflow_type_name, status, start_time, close_time, task_queue, history_length, and custom search attributes. In SQL-based visibility (available since Temporal 1.20 on PostgreSQL 12+ and MySQL 8.0.17+), search attributes are stored as JSONB/JSON columns. Elasticsearch-based advanced visibility supports richer query capabilities but adds another system to operate.

For self-hosted deployments, Temporal provides schema migration tools. Server upgrades typically require applying schema changes. The CHASM framework (introduced in v1.31) requires additional schema changes, including a new `current_chasm_executions` table for SQL-based deployments.

### Multi-Cluster and Multi-Region

Temporal supports multi-cluster deployments where clusters replicate workflow state across regions. Each cluster operates independently, and the replication task queue on each History shard ensures that events are propagated. Multi-cluster deployments are complex — they require careful network configuration, conflict resolution strategies, and operational oversight. Temporal Cloud simplifies this by offering multi-region and multi-cloud (AWS and GCP) replication as a managed feature.

### CHASM: The Internal State Machine Framework

CHASM (Coordinated Heterogeneous Application State Machines) is a relatively new internal framework that represents a significant architectural evolution. It provides a hierarchical state machine model for implementing server-side features that require complex state management beyond traditional workflows. CHASM applications are structured as trees of components, where each component has its own persisted state, schedules its own tasks (timers, side effects), handles requests via a request-reply pattern, and can dynamically create or delete child components.

CHASM powers several major features:
- **Nexus Operations**: Cross-namespace and cross-cluster workflow invocations, modeled as state machines with transitions like `Scheduled`, `Started`, and `Succeeded`, generating invocation tasks on the outbound queue with circuit breaker protection.
- **Schedulers**: The schedule primitive (distinct from cron workflows) is implemented as a CHASM component tree.
- **Worker Deployments**: The deployment version management system.
- **Standalone Activities**: Activities that execute independently of workflows.
- **Callbacks**: Workflow completion callbacks to external systems.

CHASM is built on top of HSM (Hierarchical State Machine), a versioned state machine framework that stores machines within `WorkflowExecutionInfo.SubStateMachinesByType`. HSM handles callback state machines, Nexus operation state machines, and other workflow-scoped state machines.

The introduction of CHASM signals Temporal's ambition to make the server itself more extensible. Rather than baking every feature into the core History/Matching service loop, CHASM allows features to be implemented as composable state machines that plug into the existing infrastructure. This is a significant architectural maturation — it moves Temporal from a monolithic server design toward a more modular, component-based internal architecture.

---

### Workflow Execution Model — The Heart of Temporal

The workflow execution model is what makes Temporal more than just a task queue with retries. It is the mechanism by which Temporal guarantees that code runs to completion exactly as written, recovering from any failure without the developer writing any recovery logic.

#### Event History as the Single Source of Truth

Every workflow execution has an append-only event history. This history records everything that happened: `WorkflowExecutionStarted`, `WorkflowTaskScheduled`, `WorkflowTaskStarted`, `WorkflowTaskCompleted`, `ActivityTaskScheduled`, `ActivityTaskStarted`, `ActivityTaskCompleted`, timer firings, signal deliveries, marker events for versioning, and dozens of other event types. The event history is the canonical record of the workflow's execution. If the event history says it happened, it happened. If the event history doesn't record it, it didn't happen.

This event sourcing model is the foundation of Temporal's durability. Because every state transition is recorded as an event, the workflow's entire execution can be reconstructed at any time by replaying the event history through the workflow code.

#### Deterministic Replay

When a worker picks up a workflow task, it receives the complete event history for that workflow. The SDK does not simply "resume from where it left off." It replays the workflow code from the beginning, feeding each event into the workflow function in order. During replay, the SDK recognizes which events correspond to already-completed work:
- If a timer event is encountered during replay, the SDK knows the timer already fired and does not wait again.
- If an activity completion event is encountered, the SDK returns the cached result from the event history rather than re-executing the activity.
- If a side effect event is encountered, the SDK returns the recorded value.

The replay continues until the SDK reaches the last event in the history — the point where the previous worker left off. At that point, the workflow code begins executing new commands: setting timers, scheduling activities, sending signals. These new commands are batched and sent back to the server as part of the `RespondWorkflowTaskCompleted` RPC.

For this replay mechanism to work, the workflow code must be deterministic. Given the same event history, it must produce the same sequence of commands. If a code change causes the workflow to generate a different command sequence during replay — for instance, calling an activity in a different order, or setting a timer with a different duration — the server detects the mismatch and fails the workflow task with a non-determinism error.

The constraints this places on workflow code are severe and represent Temporal's most significant developer experience burden:

- **No direct I/O**: Network calls, file reads, database queries — all must be done in activities, not in workflow code. During replay, these would produce different results or not be called at all.
- **No random number generation**: Use `workflow.random()` or side effects instead. A random number generated during replay would differ from the original execution.
- **No `time.Now()` or equivalent**: Use `workflow.now()` which returns the time from the event history. System time during replay is different from system time during original execution.
- **No goroutine/thread spawning in an uncontrolled way**: The SDK must control all concurrency to ensure deterministic scheduling. The Go SDK asks users not to use native goroutines in workflows. The Rust SDK provides deterministic `select!`, `join!`, and `join_all` macros and includes a runtime nondeterminism detector that fails workflow tasks when non-SDK async wake sources are detected.
- **No mutable global state**: If a workflow depends on mutable global state, that state won't exist during replay on a different worker.

Each SDK handles these constraints differently. The TypeScript SDK is the strictest — it runs workflows in V8 isolates via `isolated-vm`, which creates a completely isolated JavaScript context where non-deterministic built-ins are replaced. The Go SDK trusts developers to follow the rules, which means non-determinism errors are discovered at runtime rather than prevented at development time.

#### Workflow Task Lifecycle

A workflow task is the unit of work that advances a workflow execution. When a workflow needs to make progress — because a timer fired, an activity completed, or a signal arrived — the History service creates a transfer task, which the Transfer Queue Processor sends to the Matching service, which places a workflow task in the appropriate task queue.

When a worker picks up this workflow task, it replays the event history, executes new workflow code until the code blocks (on a timer, activity call, signal wait, etc.) or completes, and sends the resulting commands back to the server. The server appends these commands as events to the event history, updates mutable state, and if the workflow is not yet complete, enqueues the next task.

This means a workflow is never "running" in the traditional sense. It is a sequence of discrete workflow tasks, each of which advances the workflow by some number of steps until it blocks. Between tasks, the workflow consumes no resources — no memory, no CPU, no open connections. A workflow waiting on a timer for 30 days is just a timer task sitting in a database, consuming no worker capacity.

#### Sticky Execution

Replaying the entire event history from scratch for every workflow task would be prohibitively expensive for workflows with long histories. Temporal optimizes this with sticky execution. When a worker first picks up a workflow task, it caches the workflow state in memory and registers a "sticky queue" — a task queue specific to that worker instance. Subsequent workflow tasks for that execution are dispatched to the sticky queue rather than the shared task queue, so the same worker picks them up and can resume from its cached state without a full replay.

If the worker fails to respond to a sticky task within five seconds (the default schedule-to-start timeout for sticky queues), stickiness is disabled and the task is rescheduled on the shared queue. Any worker can then pick it up, replay from the beginning, and establish a new sticky relationship.

Sticky execution is a performance optimization, not a correctness guarantee. The system works correctly without it — it's just slower. Workers can configure `MaxCachedWorkflows` (or `StickyWorkflowCacheSize`) to control how many workflow states they cache. If the cache is too small, forced evictions cause unnecessary replays. Monitoring `sticky_cache_hit`, `sticky_cache_miss`, and `sticky_cache_total_forced_eviction` metrics helps tune cache sizing.

#### Event History Limits and Continue-As-New

The event history has hard limits: 51,200 events or 50 MB in total size. If either limit is reached, the workflow is terminated. For workflows that run indefinitely or process many activities, this means they must periodically "Continue-As-New" — close the current workflow execution and start a new one with a fresh event history, passing the relevant state forward as arguments.

Continue-As-New is not transparent. It requires explicit implementation in workflow code. The developer must decide when to checkpoint (the SDK provides `continueAsNewSuggested` as a signal that limits are approaching), must drain pending signals (signals in flight during Continue-As-New are lost unless explicitly drained), and must serialize and deserialize whatever state the next run needs. The new run has the same workflow ID but a different run ID and starts with a clean event history.

This is one of Temporal's most consistently criticized design constraints. It breaks the abstraction — suddenly the developer must think about event history size as a resource to manage — and it adds complexity to workflow design. For cron workflows, Continue-As-New historically broke the cron schedule (the new run would not inherit the CronSchedule option), though scheduling improvements have mitigated this.

---

### Activity Execution

Activities are the "impure" side of workflows — the place where real-world side effects happen. While workflows must be deterministic and cannot perform I/O, activities can call APIs, write to databases, send emails, process files, and do anything else that a normal program can do. Activities are where Temporal interacts with the outside world.

#### Activity Task Lifecycle

When a workflow schedules an activity (via `workflow.executeActivity()` or equivalent), it issues a `ScheduleActivity` command. The server records an `ActivityTaskScheduled` event and creates a transfer task. The Matching service places an activity task in the specified task queue. A worker polling that queue picks up the task, executes the activity function, and reports the result (success or failure) back to the server.

The server records an `ActivityTaskStarted` event when the worker begins execution and an `ActivityTaskCompleted` or `ActivityTaskFailed` event when it finishes. These events are appended to the workflow's event history. During replay, when the SDK encounters an `ActivityTaskCompleted` event, it returns the cached result without re-executing the activity.

The separation between scheduling and execution means that there is always a window where an activity task is queued but not yet executing. The `ScheduleToStart` timeout controls how long a task can wait in the queue — if exceeded, the task is not retried (a retry would just place it back in the same queue). This timeout is primarily useful for detecting that workers are down or that a task queue is overloaded.

#### Activity Retry Policies

Activities can be configured with retry policies that specify maximum attempts, initial interval, backoff coefficient, maximum interval, and non-retryable error types. The default retry policy is generous. When an activity fails (due to an unhandled exception, a timeout, or the worker crashing mid-execution), the server automatically reschedules it according to the retry policy, incrementing the attempt counter in mutable state.

This is a profound shift from traditional error handling. In a normal program, you wrap fallible operations in try-catch blocks. In Temporal, you configure a retry policy and let the platform handle retries. The workflow code simply awaits the activity result — if the activity eventually succeeds (even after many retries over hours or days), the workflow continues as if nothing happened. If the activity exhausts all retries, the error propagates to the workflow code, which can handle it or let it fail the workflow.

The four activity timeouts deserve careful attention:
- **ScheduleToClose**: Maximum total time for the activity including all retries. Only meaningful with `MaximumAttempts > 1`.
- **StartToClose**: Maximum time for a single execution attempt. This is the most important timeout — it must be set longer than the maximum possible execution time for the activity. Temporal recommends always setting this.
- **Heartbeat**: Maximum time between heartbeat pings. For long-running activities, enables fast detection of worker failures.
- **ScheduleToStart**: Maximum time a task can wait in the queue. Does not trigger retries.

#### Activity Heartbeats

For long-running activities (minutes to hours), heartbeats serve two purposes. First, they act as a liveness check — if a worker crashes, the heartbeat timeout detects the failure quickly (in seconds) rather than waiting for the much longer StartToClose timeout. Second, they allow the activity to record progress information that carries forward to retry attempts. If an activity was 90% done processing a large file when the worker crashed, the heartbeat payload can encode that progress, and the next attempt can resume from 90% rather than starting over.

Heartbeat throttling prevents excessive heartbeat traffic: the worker only sends heartbeats at a controlled interval. The throttling does not apply to the final heartbeat message in case of activity failure, ensuring progress information is preserved.

#### Local Activities

Local activities are a performance optimization for short, fast operations that don't need the full durability machinery. Unlike normal activities, local activities are executed directly in the workflow task rather than being dispatched to the activity task queue. They don't create separate history events, they don't support heartbeats, and they don't benefit from the same retry guarantees. They are ideal for quick lookups, simple computations, or operations where the overhead of a full activity task dispatch would dominate the execution time.

Local activities are executed inline during the workflow task. If the local activity fails, the workflow task fails (triggering retry with backoff), but the failure does not appear as a separate event in the history. This makes local activities faster but less observable.

#### Async Activity Completion

Sometimes an activity is not something a worker can complete directly — it's kicked off to an external system (like a human approval workflow or a long-running batch job), and the external system will signal completion later. Async activity completion supports this pattern. The activity function obtains a task token, communicates it to the external system, and returns without completing. Later, the external system uses the Temporal client to call `CompleteActivity(taskToken, result)` or `FailActivity(taskToken, error)`, and the workflow advances.

This pattern is powerful but requires careful timeout configuration. The StartToClose timeout must be set long enough to accommodate the entire external process — potentially days or weeks for human-in-the-loop workflows.

---

### Task Queues and Workers

#### How Task Queues Work

Task queues in Temporal are not what their name suggests. They are not simple FIFO queues. They are named, partition-based matching engines that pair tasks with polling workers. A task queue is identified by a name and is scoped to a namespace. It exists for both workflow tasks and activity tasks (these are separate logical queues within the same task queue name).

When a new task arrives at a task queue, the Matching service attempts a sync match — is there a poller waiting who can take this task immediately? If yes, the task is dispatched with minimal latency. If no, the task is persisted to the task backlog for that partition. When a new poller arrives, the reverse happens — is there a task waiting? If yes, sync match. If no, the poller waits in a long-poll (up to 60 seconds by default).

This model is inherently pull-based from the worker's perspective. Workers continuously poll for tasks; they are never pushed tasks by the server. The server's role is to efficiently match tasks to pollers, not to route tasks to specific workers.

#### The Matching Algorithm

Task queue partitions are distributed across Matching service hosts using consistent hashing. A single poll request from a worker is forwarded to a randomly chosen partition for that task queue. With many concurrent pollers, load distributes evenly across partitions. When the number of pollers is very low (like 1), partitions can forward tasks and polls between each other to prevent tasks from getting stuck in un-polled partitions.

This architecture creates an interesting dynamic: a task queue with a single worker polling it still works correctly (forwarding ensures all partitions are covered), but throughput is limited to what that single worker can process. Adding more workers increases throughput because more partitions can be polled simultaneously, and the sync match rate increases (fewer tasks waiting in backlog means lower latency).

#### Worker Registration and Polling

Workers register the workflow and activity types they can handle when they start. This registration is local to the worker — the worker tells the server "I can handle these types" so the server can route appropriate tasks. However, workers on the same task queue will happily pull tasks for any workflow type on that queue, even if they haven't registered it. There is no way to "put back" a task that a worker can't handle — the task will fail, and the server will retry it (potentially picking a different worker next time). This is a known pain point: the only way to route different workflow types to different worker pools is to use separate task queues.

Workers poll using long-poll gRPC requests to the Frontend service, which routes them to the appropriate Matching instance. The polling timeout is typically 60 seconds — if no task arrives within that window, the poll returns empty, and the worker immediately starts a new poll. Workers maintain multiple concurrent pollers (configurable per worker) to handle bursts of tasks. The SDKs support poller autoscaling, which dynamically adjusts the number of pollers based on task backlog to optimize throughput and schedule-to-start latency.

#### Serverless Workers

Temporal recently introduced Serverless Workers, which invert the polling model for serverless platforms like AWS Lambda. Instead of a long-lived process polling continuously, Temporal detects when tasks arrive on a task queue and invokes a Lambda function. The function starts a worker, polls for tasks, processes what's available, and shuts down before the Lambda timeout. This is effectively push-to-serverless — Temporal pushes work by invoking the compute function — but it's layered on top of the existing pull model (the invoked function still does a poll when it starts).

Serverless Workers are ideal for bursty, event-driven workloads but have cold-start latency implications. For sustained high-throughput workloads, traditional long-lived workers are more cost-effective.

---

### Determinism and Versioning

The determinism requirement is Temporal's central developer experience challenge. Because workflow execution must be replayable, changing workflow code that is already executing in production is dangerous. Even a seemingly innocuous change — adding a log statement, changing the order of activity calls, modifying a conditional — can cause non-determinism errors during replay.

This is the fundamental tension in Temporal's design: the feature that gives Temporal its durability (event-sourced replay) is also the feature that most constrains developer freedom. Every Temporal user eventually hits this wall.

#### The Versioning API (Patching)

Temporal's original solution to this problem is the versioning API, also called patching. The `Workflow.getVersion()` method (or `patched()` in TypeScript, `workflow.DeprecatePatch()` in Go) allows branching workflow code based on a version identifier. During original execution, the workflow records the version it took. During replay, it takes the same branch regardless of the current code version.

A typical patching workflow is:

1. Deploy new code with `patched('my-change')` wrapping the new behavior. Old workflows (without the patch marker in their history) take the old branch. New workflows take the new branch.
2. Once all old workflows have completed or continued-as-new, deploy code that removes the old branch and uses `deprecatePatch('my-change')` to indicate the old path is gone.
3. Once all pre-deprecation workflows are out of retention, deploy code that removes the patching entirely.

This three-phase deployment process is tedious and error-prone. It requires developers to maintain dead code paths for extended periods and carefully track which versions are "live" in production.

#### Worker Build ID Versioning

A newer and more pragmatic approach is Worker Build ID Versioning. Instead of patching workflow code, you assign a Build ID to each worker deployment and let the server route workflows to compatible workers. A workflow can be "pinned" to a specific Build ID, meaning it will only execute on workers with that ID. Or it can be set to "auto-upgrade," meaning it will move to newer workers as they become available.

The deployment system supports concepts like:
- **Current Version**: The version where new workflows are routed unless pinned elsewhere.
- **Ramping Version**: A configurable percentage of new workflows go to this version (for canary deployments).
- **Target Version**: The version an auto-upgrade workflow will move to, which could be the current version or (with some probability) the ramping version.

This approach shifts the versioning burden from application code to deployment infrastructure. It avoids the three-phase patching dance and allows safer rollouts. However, it requires operational discipline — maintaining multiple worker versions simultaneously, managing ramp percentages, and eventually decommissioning old versions.

#### The Determinism Checker

The Temporal SDKs include determinism checking at various levels. The TypeScript SDK is the most aggressive — V8 isolates prevent non-deterministic operations at the language level. The Rust SDK includes a runtime nondeterminism detector that monitors async wake sources and fails workflow tasks when non-SDK futures resolve. The Go SDK relies primarily on server-side detection — if a worker produces commands that don't match the event history, the server fails the task.

Determinism checking is inherently a runtime concern. There is no static analysis that can guarantee a workflow is deterministic across all possible code paths. This means non-determinism bugs are often discovered in production during replay after a worker failure or deployment.

---

### Signals, Queries, and Updates

These three primitives enable external communication with running workflows, and they represent one of Temporal's most elegant design features.

#### Signals

A signal is an asynchronous, fire-and-forget message sent to a running workflow. It is recorded as an event in the workflow's event history, and the workflow can await signals, process them in signal handlers, or buffer them for later. Signals are the primary mechanism for external systems to communicate with workflows — for example, a webhook handler sending a signal when a payment is confirmed, or a UI sending a signal when a user clicks "approve."

Signals are not blocking for the sender. The caller gets an immediate acknowledgment that the signal was received by the server — not that it was processed by the workflow. If the workflow has a signal handler, it will process the signal when it next executes a workflow task. If the workflow is currently blocked waiting for a signal, receiving the signal unblocks it.

Signal handlers run concurrently with the main workflow method (in async SDKs like Python and TypeScript). This means signal handlers can race with the main workflow logic, and developers must use locks or wait conditions to manage concurrency. Synchronous signal handlers (in Go) run atomically — they execute to completion before the main workflow method resumes — which is simpler but less flexible.

#### Queries

A query is a synchronous, read-only operation that retrieves state from a running workflow. Queries cannot modify workflow state, cannot block, and cannot execute activities. They return immediately with a snapshot of workflow state.

Because queries are read-only, they are not recorded in the event history. They serve the workflow's current mutable state snapshot and bypass the replay path. This makes queries lightweight and fast, but also means they see whatever state the workflow currently has in memory — which may be between workflow tasks.

#### Updates

Updates, introduced in 2023, fill the gap between signals (asynchronous, no response) and queries (synchronous, read-only). An update is a synchronous, tracked write request. The caller sends an update, the workflow processes it, and the caller receives either a result or an error. Updates are recorded in the event history and participate in replay.

Updates represent a significant evolution in Temporal's communication model. Before updates, the only way to get a synchronous response from a workflow was to send a signal (fire-and-forget) and then poll with queries until the desired state appeared — a pattern that added latency and complexity. Updates collapse this into a single operation.

Updates support validators — a read-only check that can accept or reject an update before it's recorded in history — and the handler can do anything normal workflow code can do, including executing activities and waiting on timers. This means an update can be a short, fast operation or a long-running sub-process, and the caller can wait for its completion.

The update API also provides an `update-with-start` variant, which sends an update to a running workflow if it exists, or starts a new workflow with that update if it doesn't. This is useful for idempotent operations where the caller doesn't need to know whether the workflow already exists.

---

### Scheduling

#### Cron Workflows

Originally, Temporal supported recurring execution through cron workflows — workflows with a `CronSchedule` parameter that would be re-executed on a schedule. Each execution is a separate workflow run. Cron workflows are simple but have limitations: they can't be paused (you must terminate the workflow), they can't be backfilled, and they interact poorly with Continue-As-New (the new run loses the cron schedule).

#### Schedules

Schedules are a newer, more sophisticated primitive built on top of the CHASM framework. A schedule is a separate resource that periodically starts workflows (or sends signals/updates) on a defined schedule. Unlike cron workflows, schedules support:
- **Pause/unpause**: A schedule can be paused and resumed without losing state.
- **Backfill**: Run missed executions for a past time window.
- **Overlap policies**: Control what happens if a previous execution is still running when the next one is scheduled — skip, allow all, buffer one, buffer all, or cancel the previous.
- **Calendar-based scheduling**: More expressive than simple cron expressions, with support for time zones, intervals, and exclusion windows.
- **Jitter**: Randomize the exact start time within a window to avoid thundering herds.

Schedules are the recommended approach for recurring work in modern Temporal and address many of the complaints about cron workflow limitations.

---

### Temporal Cloud

Temporal Cloud is the fully managed, multi-tenant SaaS offering. It uses a cell-based architecture: each cell is a self-contained deployment with its own AWS or GCP account, VPC, Kubernetes cluster, Temporal services, databases (with synchronous replication across three availability zones), Elasticsearch, and supporting infrastructure.

Tenancy is multi-tenant within each cell. The control plane manages provisioning, configuration, and lifecycle across cells. When a customer creates a namespace, the control plane allocates it to a cell and configures the necessary resources.

Temporal Cloud is available on both AWS and GCP, with 14+ AWS regions and growing GCP coverage. Multi-region and multi-cloud replication is a managed feature.

#### Pricing

Cloud pricing combines a base plan fee with consumption-based billing. The four plans are Essentials ($100/month minimum), Business ($500/month minimum), Enterprise, and Mission Critical. Each includes a base allocation of Actions (the primary billing unit) and Storage. Beyond the allocation, pricing is pay-as-you-go with volume discounts starting at $50 per million Actions, scaling down to $25 per million.

An "Action" is a billable operation: starting a workflow, completing an activity, recording a heartbeat, sending a signal, and so on. One workflow execution can generate many Actions — community reports describe single workflows generating 5-50 billable Actions, making cost prediction difficult. This opaque billing model is a frequent complaint.

The pricing structure also means that complex, chatty workflows (with many activities, signals, updates) cost proportionally more than simple ones. Teams that underestimate Action volume can face surprise bills.

---

## 3. Version Evolution

### Uber Cadence: The Origin

Cadence was born from the specific needs of Uber's microservice architecture. In a system with hundreds of services, a simple operation like "request a ride" becomes a sprawling sequence of RPC calls spanning dispatch, pricing, driver matching, payment, and notifications. Each call can fail, each service can be slow, and the overall workflow must either complete correctly or leave the system in a consistent state.

Before Cadence, Uber engineers described the experience as "a quagmire of callbacks." Every service interaction required manual retry logic, timeout handling, state persistence, and compensation logic. Business logic was buried under infrastructure concerns.

Cadence solved this by providing a platform where engineers could write workflow code as ordinary functions, and the platform would handle persistence, retries, timeouts, and fault tolerance. The workflow became the unit of business logic, and the infrastructure concerns were offloaded to the Cadence server.

By 2020, Cadence was processing over 12 billion workflow executions and 270 billion actions per month at Uber, powering over 1,000 services from T0 (most critical) to T5. It sustained 100% year-over-year growth.

### The Temporal Fork

The decision to fork Cadence rather than continue evolving it within Uber was driven by both practical and strategic concerns. Practically, the Cadence codebase had accumulated four years of backwards-compatible-only changes — every upgrade had been a live migration with zero downtime. This meant significant technical debt that could only be resolved with breaking changes. Strategically, Uber would never create a managed cloud offering, and the founders believed the technology needed a dedicated company to drive it forward as an industry-wide platform.

The fork allowed the Temporal team to spend nearly a year rethinking architectural decisions, fixing accumulated issues, and adding features that would have been impossible under the Cadence compatibility constraint. The community trusted the founders (Fateev and Abbas were personally known to most early adopters) and followed the fork.

### Temporal 0.x → 1.0

The 1.0 release represented the culmination of that year of rework. Key changes from Cadence included:
- Simplified deployment and configuration
- Improved SDK ergonomics across all languages
- Better testing infrastructure (the time-skipping test server)
- Enhanced visibility and monitoring
- Removal of the Kafka dependency for visibility (replaced with direct database writes plus Elasticsearch)
- Internal architectural cleanup that couldn't be done under Cadence's compatibility requirements

### Temporal 1.x Releases

- **Schedules**: The schedule primitive replaced/supplemented cron workflows with pause/unpause, backfill, and overlap policies.
- **Advanced Visibility with SQL**: Temporal 1.20 introduced advanced visibility on SQL databases (PostgreSQL 12+, MySQL 8.0.17+), reducing the dependency on Elasticsearch for visibility queries.
- **Worker Build ID Versioning**: A deployment-oriented approach to versioning that shifts responsibility from application code to deployment infrastructure.
- **Updates**: The synchronous write primitive that fills the gap between signals and queries.
- **Update-with-Start**: Idempotent update-or-create semantics.
- **Nexus**: Cross-namespace and cross-cluster workflow communication, representing a major architectural expansion.

### Temporal 1.21+ and Current Direction

- **Nexus enabled by default** (v1.31): Nexus is now always-on, with token-based routing, circuit breakers, and full integration with the CHASM framework.
- **CHASM framework enabled by default** (v1.31): The hierarchical state machine framework now powers schedules, standalone activities, Nexus operations, and worker deployments. It represents a significant internal refactoring toward a more modular server architecture.
- **Serverless Workers**: Temporal can now invoke workers on serverless platforms like AWS Lambda, blurring the line between pull-based workers and event-driven execution.
- **Deployment Series**: The worker deployment system continues to mature with ramping, pinning, and auto-upgrade capabilities.

### Key Architectural Lessons from Uber Scale

The experience of running Cadence/Temporal at Uber's scale validated several architectural decisions and revealed important constraints:
- **Event sourcing works at scale**: The append-only event log model proved robust under extreme throughput.
- **The sharding model is effective but has limits**: The single-shard bottleneck is real, and 16,384 shards is the upper limit of the current architecture.
- **Cassandra dependency is a double-edged sword**: It scales well but creates operational complexity that many teams cannot sustain.
- **The worker model needs evolution**: The pull-based polling approach creates latency under variable load that a push model could eliminate.

---

## 4. Known Pain Points & Complaints

Temporal's architectural choices create a distinct set of pain points that surface consistently in community discussions, Hacker News threads, and practitioner reports.

### The Learning Curve

The deterministic replay mental model is genuinely difficult to internalize. Developers accustomed to normal programming must learn an entirely new set of constraints: don't use `time.Now()`, don't make HTTP calls in workflow code, don't use random numbers directly, don't spawn goroutines, wrap all I/O in activities. The workflow/activity separation is conceptually clean but practically burdensome — simple operations that would be one line of code in a normal program become activity definitions, registrations, and retry policy configurations.

The onboarding experience is frequently described as "weeks, not hours" to genuinely understand. Multiple community members report that Temporal "made their codebase worse" because the abstractions forced by the determinism requirement led to fragmented, harder-to-follow code.

### SDK Quality Variance

Go and Java are the first-class SDKs with the deepest feature coverage and most stable APIs. Python and TypeScript have matured significantly but still lag in some areas. The .NET SDK is relatively new. The PHP SDK has the smallest community.

The Rust SDK is in active development and introduces WASM-based workflow execution, but it's still maturing. SDK fragmentation means that multi-language organizations may face different limitations depending on which language their teams use.

### Event History Limits and Continue-As-New

The 51,200 event / 50 MB history limit is a hard constraint that forces architectural decisions into application code. Developers must instrument workflows with Continue-As-New checkpoints, handle signal draining, and manage state serialization across runs. This is not a transparent platform feature — it's a leak in the abstraction that requires explicit developer attention.

### Operational Complexity of Self-Hosting

Self-hosting Temporal is a significant commitment. The production checklist in Temporal's documentation is stark in its honesty: "Significant engineering and ongoing effort is required." Self-hosted deployments must manage:
- A database cluster (Cassandra or PostgreSQL) for persistence
- Elasticsearch or OpenSearch for visibility (or a heavily-scrutinized SQL store)
- Four independently scalable Temporal services (Frontend, History, Matching, Worker)
- Service discovery and membership (Ringpop)
- Schema migrations during upgrades
- Monitoring and alerting across all services
- Multi-region replication if needed

A Hacker News commenter running large on-prem Temporal described requiring 10-30x the resources of building something simpler, and emphasized that "you really need an entire team. You can't have somebody who isn't a dedicated engineer take on-call for it." Another commenter noted that Temporal "quickly becomes mission-critical infrastructure" and that "running Temporal well requires significantly more platform maturity than getting it running in the first place."

Even with managed databases, the operational burden is substantial. A modest number of concurrent workflows (hundreds) can push several thousand transactions per second through a PostgreSQL instance, and PostgreSQL becomes a bottleneck under load. The recommended Cassandra backend requires specialized operational expertise.

### The Single Writer Bottleneck

Because each history shard is owned by exactly one host, all writes for workflows on that shard are serialized through that host. This means that a single very active workflow can saturate its shard, and the only mitigation is scaling the number of shards (which is fixed at cluster creation) or splitting the workload across multiple workflows on different shards.

### Worker Polling Latency

The pull model creates inherent latency. A task sits in the Matching service backlog until a poller picks it up. Under steady load, sync matching keeps this latency near zero, but under bursty load, the schedule-to-start latency spikes. Temporal Cloud reports p95 schedule-to-start latency under 50ms for same-region workers, but self-hosted deployments can see much higher latencies under adverse conditions.

The polling model also means idle workers consume resources continuously making long-poll requests. Poller autoscaling mitigates this but doesn't eliminate it. The Serverless Workers feature is essentially an acknowledgment that the pull model is suboptimal for bursty workloads.

### Testing

Testing Temporal workflows is harder than testing normal code. The time-skipping test server helps — it simulates the Temporal server and lets tests fast-forward through timers — but it's not a perfect simulation of production behavior. Activity mocking requires careful setup. Workflows that use Continue-As-New require test infrastructure to handle the new run. Determinism issues that would cause failures in production may not manifest in test environments because replay doesn't happen in the same way.

### Observability

The Temporal Web UI shows workflow event history as a chronological list of events. For engineers, this is powerful for debugging. For anyone else, reconstructing business logic from a list of `WorkflowTaskStarted`, `ActivityTaskScheduled`, and `ActivityTaskCompleted` events is like reading assembly language. The gap between "what happened in Temporal" and "what happened in the business" requires custom tooling and dashboards to bridge.

Self-hosted deployments must configure their own metrics pipeline (Prometheus/Grafana), logging aggregation, and alerting. The server emits extensive metrics, but interpreting them requires deep Temporal expertise. The community has repeatedly noted that "the documentation is quite bad" for self-hosting, with one commenter describing "500,000 word pages, codegen'd library sites with no comments, and one example for each feature."

### Multi-Tenancy and Isolation

Within a Temporal cluster, namespaces provide logical isolation but not performance isolation. A noisy neighbor namespace can degrade performance for other namespaces by saturating shared History or Matching hosts, consuming database resources, or generating excessive visibility queries. Temporal Cloud's cell-based architecture provides stronger isolation by allocating customers to different cells, but within a cell, multi-tenancy concerns remain.

### Schema Migrations

Each Temporal server upgrade may require schema changes to the persistence database. For SQL-backed deployments, this means running migration scripts. For Cassandra, the migration path is more involved. The 1.31 release introduced CHASM schema changes including a new `current_chasm_executions` table, and custom persistence implementations must be updated to handle the new `ArchetypeID` field in persistence requests.

### Activity Heartbeat Model

Activity failure detection relies entirely on timeouts. If an activity's StartToClose timeout is 5 hours and the worker crashes after 30 minutes, Temporal won't detect the failure until the timeout expires. Heartbeats mitigate this (a shorter heartbeat timeout detects crashes faster), but the fundamental model is timeout-based rather than connection-based. There is no persistent connection between the server and the worker executing an activity — the worker could crash, and the server won't know until a timeout fires.

### SDK Versioning and Worker Deployment

Worker Build ID Versioning helps but adds operational complexity. Running multiple versions of workers simultaneously, managing ramp percentages, and tracking which workflows are pinned to which versions requires dedicated deployment tooling and monitoring. For smaller teams, this overhead may exceed the benefits.

### Cold Start

When a new worker starts, it has no cached workflow state. The first workflow task it picks up requires a full history replay, which is significantly slower than a warm cache hit. If many workers start simultaneously (deployment, scale-up), the resulting cold-start wave can cause latency spikes. Sticky execution mitigates this for steady-state operation, but restarts and deployments reset the cache.

### Temporal Cloud Concerns

- **Pricing opacity**: The Actions billing model is difficult to predict. A single complex workflow can generate dozens of Actions, and estimating monthly costs requires detailed understanding of workflow structure and event generation.
- **Vendor lock-in**: Temporal SDKs are deeply integrated into application code. Migrating off Temporal Cloud to self-hosted is technically possible (the API is the same), but requires standing up and operating all the infrastructure Temporal Cloud abstracts away.
- **Latency overhead**: Cloud deployments add network latency between workers and the Temporal service. For latency-sensitive use cases, self-hosting in the same VPC may be necessary.

### Visibility API Limitations

Visibility queries have two modes: list queries (fast, indexed, but limited to recent workflows and closed workflows within retention) and scan queries (slow, scans all workflows, supports complex filters). The tension between these two modes means that operational queries about workflow state often require careful design of search attributes and query patterns.

---

## 5. Push vs. Pull Model Analysis

Understanding where Temporal pushes and where it pulls is essential for evaluating how a push-based architecture could improve upon it.

### Where Temporal Uses Push

Temporal is not purely pull-based. Several interactions are push-oriented:

- **Signals**: External systems push signals to workflows. The server receives the signal request and enqueues it for the workflow. The workflow doesn't poll for signals — it receives them.
- **Updates**: Like signals, updates are pushed to workflows. The update request arrives at the server and is routed to the workflow's history shard.
- **StartWorkflow**: The initial workflow start is a push — the client pushes a start request to the server.
- **Server-to-Worker "Indirect Push"**: Although workers poll for tasks, the server controls when tasks are created. When a timer fires, the server pushes a workflow task to the Matching service. When a signal arrives, the server pushes a workflow task. The server is making decisions about when work should happen; workers just happen to discover that work through polling.
- **Serverless Workers**: This feature is explicitly push — Temporal invokes the worker function rather than waiting for a worker to poll.

### Where Temporal Uses Pull

- **Worker Task Acquisition**: Workers poll task queues for both workflow and activity tasks. This is the primary pull mechanism.
- **Activity Polling Within Workflows**: When a workflow code calls `executeActivity()`, it's conceptually "pulling" the result, but the underlying mechanism is push (the activity task is scheduled) followed by pull (the workflow waits for the completion event).
- **Query/Update Senders**: Clients that send queries or updates "pull" the response by waiting for the RPC to complete, but the message itself is pushed to the workflow.

### How the Matching Service Bridges Push/Pull

The Matching service is the bridge between Temporal's internal push model (events generating tasks) and the worker-facing pull model (workers polling for tasks). When the History service determines that a workflow needs to advance, it pushes a transfer task through its internal queue, which results in a task being placed in the Matching service. At that point, the Matching service either sync-matches the task to a waiting poller (fast path) or stores it in the backlog until a poller arrives (slow path).

This bridging is elegant in steady state — sync matching means tasks often flow directly from the History service to a worker with minimal latency. But it creates problems at the boundaries:

- **Burst-Induced Latency**: If no pollers are waiting when a burst of tasks arrives, every task enters the backlog and waits for a poller to arrive. The schedule-to-start latency spikes.
- **Polling Overhead**: Even idle workers consume resources making long-poll requests. The overhead is small per worker but adds up across many workers.
- **No Backpressure Signal to the Server**: The server doesn't know how busy workers are. It places tasks in the Matching service regardless of worker capacity. Workers pull what they can, and tasks that exceed capacity sit in the backlog or time out.

### Latency Characteristics

The polling model's latency is bounded by the poll interval and the matching algorithm. In the best case (sync match with a waiting poller), task dispatch latency is near zero. In the median case, it's the time for a poll cycle to complete (a task arrives just after a poll started, so it's picked up on the next poll). In the worst case (no pollers available), it's unbounded until a poller arrives, subject to the ScheduleToStart timeout.

Worker performance tuning can reduce but not eliminate this latency. Increasing the number of concurrent pollers, enabling poller autoscaling, and ensuring sufficient worker capacity all help, but the fundamental limitation of the pull model remains: the server cannot route work to workers that aren't polling.

### What a Fully Push-Based Temporal Would Look Like

A push-based architecture would invert the worker-server relationship. Instead of workers polling for tasks, the server would push tasks directly to workers. This would require:

- **Persistent connections**: The server would need to maintain connections to workers (or be able to reach them via their network addresses). This means workers must be addressable, which conflicts with the current model where workers can be behind NAT, on ephemeral infrastructure, or serverless.
- **Worker capacity tracking**: The server would need to know each worker's capacity (how many concurrent tasks it can handle) to avoid overwhelming workers.
- **Load-aware routing**: The server would need to route tasks to workers based on current load, not just task queue membership.
- **Worker health monitoring**: The server would need active health checking rather than timeout-based failure detection.

The benefits would be substantial: near-zero dispatch latency (no polling delay), natural backpressure (the server only sends tasks when workers have capacity), and reduced idle resource consumption (workers don't poll when there's no work).

The Serverless Workers feature is a partial step toward push — Temporal invokes workers when tasks arrive — but it's still layered on the pull model (the invoked worker polls for tasks). A truly push-based system would stream tasks to workers over persistent gRPC streams or WebSocket connections, with the server pushing tasks as they become available and workers acknowledging completion on the same stream.

### Nexus and Temporal's Push Ambitions

Nexus is Temporal's cross-namespace and cross-cluster communication framework. It enables one workflow (the caller) to invoke an operation on another namespace or cluster (the handler). Nexus uses a queue-based worker architecture — handler workers poll a task queue for Nexus tasks, just like they poll for workflow and activity tasks — but the Nexus machinery on the caller side supports concepts that push toward more event-driven patterns: circuit breakers, rate limiting, concurrency limiting, and automatic retries with exponential backoff.

Nexus also introduces the outbound task queue in the History service, which is designed for long-running external requests (up to 10 seconds) and includes isolation mechanisms (per-destination circuit breakers, rate limiters, and concurrency limiters) that suggest Temporal is building infrastructure for more sophisticated cross-service communication patterns.

Nexus doesn't change Temporal's fundamental pull model — handler workers still poll — but it does show that Temporal is thinking about push-style interactions between services, even if the underlying worker dispatch remains pull-based.

---

## 6. WASM Analysis

WebAssembly intersects with Temporal's architecture at multiple levels, from the existing V8 isolate approach in the TypeScript SDK to the emerging WASM workflow engine in the Rust SDK, and from deterministic sandboxing to the broader implications of WASM as a universal runtime.

### Temporal's Existing Sandboxing Approaches

Temporal already uses sandboxing to enforce determinism in some SDKs. The TypeScript SDK runs each workflow in its own V8 isolate via `isolated-vm`. This provides isolation (no shared state between workflows) and determinism enforcement (non-deterministic built-ins like `Date` and `Math.random` are replaced with workflow-safe versions). The .NET SDK attempted a similar approach with Code Access Security before that feature was deprecated in .NET, and now uses runtime detection via an EventListener. The Go SDK relies entirely on developer discipline.

These sandboxing approaches are SDK-specific and language-dependent. The TypeScript SDK's V8 isolate is brilliant for TypeScript but doesn't help Go or Java developers. Each SDK reinvents determinism enforcement in its own way.

### The Rust SDK's WASM Workflow Engine

The Rust SDK (PR #1239, currently in development) introduces WASM-based workflow execution using the wasmtime component model. This is a fundamentally different approach from previous sandboxing efforts.

The architecture works as follows:

- **WIT Contract**: A WIT (WebAssembly Interface Types) file defines the guest-host interface for workflow lifecycle operations — activation, polling, blocking detection, and completion.
- **Guest Compilation**: Workflow authors write Rust code using the normal Temporal SDK macros (`#[workflow]`, `#[workflow_methods]`). The `export_workflow_module!` macro wires the workflow implementations into WIT exports, and the code is compiled to a `.wasm` component targeting `wasm32-wasip2`.
- **Host Instantiation**: The worker uses `wasmtime::component::bindgen!` to load `.wasm` components from files or bytes, instantiate the guest, and register exported workflows in the same registry as native workflows.
- **Polling Model**: The guest interface is intentionally synchronous in the initial implementation. The guest signals "blocked" by returning `routine-poll-result.made-progress = false`, and the host re-enters `poll-routine` after the relevant activation lands. This matches what stable wasmtime + wit-bindgen supports today, with plans to migrate to WASI 0.3 async funcs when the component model's async primitives mature.

The significance of this approach is that it provides language-independent, runtime-enforced determinism. The WASM sandbox prevents file system access, network calls, thread spawning, and non-deterministic time access at the bytecode level, not through language-specific conventions or runtime checks. A workflow compiled to WASM simply cannot be non-deterministic (within the bounds of what the component model allows).

### How WASM's Deterministic-by-Default Model Aligns with Temporal

WASM's design philosophy aligns remarkably well with Temporal's replay requirements:

- **No ambient authority**: WASM modules cannot access the filesystem, network, or system clock unless explicitly granted those capabilities through WASI interfaces. This makes denying non-deterministic operations the default rather than an opt-in restriction.
- **Deterministic execution**: Given the same inputs (memory state + imported function results), a WASM module produces the same outputs. This is exactly what Temporal needs for replay.
- **Platform independence**: A `.wasm` binary runs identically on any host that supports the component model, regardless of operating system or architecture. This eliminates the "it worked on my machine" class of non-determinism bugs that arise from platform differences.
- **Isolation**: Each WASM instance has its own memory space. Workflows cannot interfere with each other through shared state.

### Would WASM Workers Eliminate the Determinism Contract Burden?

For workflow code, yes — with caveats. A WASM-based workflow that has no access to non-deterministic WASI interfaces (no filesystem, no network, no system clock) literally cannot produce non-deterministic behavior. The determinism contract is enforced by the runtime, not by developer discipline.

However, activities still need access to the outside world (they must make API calls, read files, query databases), so the workflow/activity separation remains. What WASM eliminates is the need for developers to remember not to call `time.Now()` in workflow code, not to spawn goroutines, not to read from `os.Getenv()`. These operations are simply not available in the WASM sandbox.

The developer experience improvement would be substantial. Instead of learning a complex set of constraints and relying on runtime determinism checkers to catch violations in production, developers would get compile-time safety (the WASM component won't compile if it uses non-deterministic imports) or instantiation-time errors (the host refuses to load a component that imports capabilities it shouldn't have).

### WASI and Temporal Integration

The WebAssembly System Interface (WASI) provides standardized interfaces for WASM components to interact with the host. In a Temporal context:

- **WASI 0.2 (Preview 2)**: Provides synchronous interfaces for I/O, clocks, filesystem, and random number generation. The Temporal Rust SDK's WASM implementation targets this version, with a synchronous polling model that matches wasmtime's current capabilities.
- **WASI 0.3 (Preview 3)**: Introduces native async support — `async func`, `future<T>`, and `stream<T,U>` as first-class types in the component model. This would allow Temporal workflows to use native `await` for timers, activity results, and signal handling, rather than the current synchronous polling loop. WASI 0.3's task-based concurrency model maps naturally to Temporal's workflow task model, where each activation is a task that the host schedules.
- **WASI HTTP**: A proposed interface for HTTP client and server capabilities. For Temporal, this could standardize how activities make HTTP requests, though activities would still need network access.
- **Custom WIT interfaces**: Temporal could define its own WIT interfaces for workflow-specific operations: `schedule-activity`, `start-timer`, `await-signal`, etc. These would be the only non-determinism-adjacent operations available to WASM workflows, and they would be implemented by the Temporal host in a deterministic way (recording results in the event history).

The integration path would look something like: define a Temporal workflow WIT world that imports timer, activity, signal, and query interfaces from the host, compiles user workflows against this world, and runs them in wasmtime hosts that implement these interfaces by communicating with the Temporal server.

### Known Experiments and Proposals

The Rust SDK's WASM workflows PR is the most concrete implementation within the Temporal ecosystem. It demonstrates:
- End-to-end execution of WASM workflows against a real Temporal worker
- Integration with the existing worker registry (WASM and native workflows coexist)
- The current synchronous polling model for the guest-host interface
- Planned migration to WASI 0.3 async for a more natural programming model

Outside Temporal, the Obelisk project is a WASM-based deterministic workflow engine built with Rust, wasmtime, and the WASM Component Model. It uses SQLite as its persistence layer and a structured concurrency model for workflow execution. Obelisk demonstrates that the WASM component model can support a full workflow engine with type-safe interfaces, deterministic execution, and automatic state persistence — all without the infrastructural complexity of Temporal's server cluster.

Other projects like Spin, WasmEdge, and Lunatic are exploring WASM for serverless and long-running workloads, but none specifically target the durable execution model that Temporal pioneered.

---

## 7. Relevance to Conductor

This section analyzes what Conductor — the next-generation data orchestration tool under design — should learn from Temporal's strengths and weaknesses. The analysis is framed around Conductor's two defining architectural choices: a push-based execution model and WASM as container runtimes.

### What Temporal Got Right

**Durable Execution as the Core Abstraction.** Temporal's event sourcing model — recording every state transition as an event and using replay for recovery — is the right abstraction for reliability. Conductor should adopt this model for long-running data pipelines, where a pipeline that processes terabytes of data over hours needs the same durability guarantees as Temporal's microservice workflows. The event history becomes both the recovery mechanism and the audit log, which is especially valuable for data pipelines where lineage and reproducibility matter.

**The Workflow/Activity Separation.** Separating deterministic orchestration logic from non-deterministic I/O is a clean conceptual model that applies directly to data pipelines. A data pipeline's orchestration logic — "extract from source A, transform with model B, load to destination C" — is deterministic (given the same inputs, the same steps happen in the same order). The I/O — actually reading from the database, running the model, writing to the warehouse — is where side effects and failures happen. Temporal's separation of workflows (deterministic orchestration) and activities (I/O with retries) maps naturally onto this pattern.

**Multi-Language SDK Model.** Temporal's SDK strategy — providing language-native APIs across multiple languages — means developers write workflows in the language they already know. Conductor should adopt the same strategy, but with WASM components as the execution target, the SDK surface can be thinner: compile workflow code to WASM and let the Conductor runtime handle execution.

**Fault Tolerance Guarantees.** Temporal's guarantee that workflows run to completion regardless of failures is the platform's defining value. For data pipelines, this guarantee transforms reliability from an application-level concern (every pipeline must implement its own retry and recovery logic) to a platform concern (the platform ensures completion). Conductor should aim for the same guarantee.

**The Replay/Debug Model.** Temporal's ability to replay a workflow from its event history — even months after execution — is powerful for debugging and auditing. For data pipelines, replay means being able to reproduce exactly what happened during a specific pipeline run, including all intermediate states and decisions. This is stronger than typical data lineage tooling, which shows what happened at the data level but not what the orchestration code decided.

### What Temporal Got Wrong or Struggles With

**The Determinism Burden.** Making developers responsible for writing deterministic code is Temporal's biggest developer experience failure. The constraints are numerous, non-obvious, and enforced primarily at runtime — meaning bugs are discovered in production during replay after a failure. Conductor, with WASM as its workflow runtime, can make determinism a property of the execution environment rather than a discipline imposed on developers. A WASM component that doesn't import non-deterministic capabilities simply cannot be non-deterministic.

**The Polling Model for Workers.** Workers polling for tasks creates latency under variable load, wastes resources when idle, and provides no natural backpressure mechanism. Conductor's push-based model should push tasks directly to workers over persistent connections, eliminating polling latency and enabling the server to make load-aware routing decisions based on actual worker capacity.

**Operational Complexity.** Temporal's self-hosting requirements — Cassandra, Elasticsearch, four independently scalable services, Ringpop for service discovery, schema migrations — are prohibitive for all but the largest organizations. Conductor should target a dramatically simpler operational footprint. A single binary that embeds SQLite for persistence (like Obelisk demonstrates) could handle a surprisingly large fraction of use cases, with pluggable backends for scale-out deployments. The key insight is that operational simplicity is a feature, not a secondary concern.

**Not Data-Pipeline-Specific.** Temporal is a general-purpose workflow engine, and its abstractions reflect that. It has no concept of data lineage, no native understanding of data quality checks, no built-in integration with data warehouses or lakes, and no opinion about how data flows between pipeline stages. Conductor should be opinionated about data — it should understand that activities produce and consume datasets, track data lineage automatically, and provide data-specific primitives (like "wait for this dataset to have N new partitions" or "reprocess this time range").

### Implications for Push-Based Architecture

A push-based architecture for Conductor changes the worker-server relationship in fundamental ways:

**Persistent Connections.** Workers maintain persistent gRPC stream or WebSocket connections to the Conductor server. The server pushes tasks to workers over these streams, and workers push results back. This eliminates polling latency and provides a natural channel for backpressure (the server can see which workers are busy and throttle task dispatch).

**Worker Capacity Awareness.** Because the server knows which workers are connected and how many tasks each worker is currently processing, it can make intelligent routing decisions. A worker with 8 of 10 slots full gets fewer new tasks. A worker with 0 of 10 slots full gets more. This is load-aware dispatch, not the random-to-any-partition model of Temporal's Matching service.

**Push-Based Activities.** Long-running activities benefit from push. When an activity completes (hours later), the worker pushes the result to the server rather than the server discovering it through a heartbeat timeout. This enables immediate notification of completion and eliminates the timeout-based failure detection model.

**Push-Based Signals and Events.** External events (webhooks, database change notifications, file arrival events) push directly into the Conductor server, which routes them to the appropriate workflow through the same push infrastructure. This creates a uniform push-based event model from external triggers through internal routing to worker dispatch.

**Challenges of Push.** A push-based model requires workers to be network-addressable (the server must be able to reach them), which conflicts with scenarios where workers run behind NAT, on ephemeral infrastructure, or in serverless environments. Conductor should support both push (for always-on workers) and serverless invoke (for ephemeral workers, similar to Temporal's Serverless Workers). It also requires the server to track worker liveness through active connection monitoring rather than passive timeout-based detection.

### Implications for WASM-Based Runtime

**WASM Eliminates the Determinism Contract Problem.** This is the single biggest architectural advantage of a WASM-based workflow runtime. Developers write workflow code in any language that compiles to WASM (Rust, Go, TypeScript, Python via Pyodide, etc.), and the WASM sandbox guarantees determinism. No more "don't use `time.Now()`" rules, no more runtime determinism checkers that catch bugs in production, no more patching APIs for versioning. The determinism contract is enforced by the runtime, not by developer discipline.

This doesn't mean versioning goes away — code changes still need to be compatible with existing event histories — but the surface area of potential non-determinism shrinks dramatically. The only things that can change between code versions are the workflow's control flow decisions (which activities to call, in what order, with what parameters), and those decisions are recorded in the event history during original execution and replayed during recovery.

**WASM Isolation Removes the Need for Docker Containers.** Temporal's workers run in containers today for isolation — one worker per container, scaled via Kubernetes. WASM provides the same isolation (each component runs in its own sandbox with its own memory) without the overhead of container startup, image management, or Kubernetes orchestration. A single Conductor worker process could host hundreds of WASM component instances, each isolated from the others, with near-instant startup and minimal memory overhead.

This has profound implications for cold start. In Temporal, a cold-start worker must replay the entire event history before it can begin executing new tasks — a process that can take seconds or minutes for large histories. With WASM, the worker loads the component (milliseconds), replays the history (still necessary but potentially faster since WASM execution is close to native speed), and begins executing. The isolation model also means that one workflow's memory usage doesn't affect another workflow — no shared caches, no sticky execution complexity.

**Mapping Temporal's Workflow/Activity Model to WASM Components.** In Conductor's WASM-based model:
- **Workflows** are WASM components that import a Conductor-specific WIT interface providing timer, activity scheduling, signal handling, and query capabilities. They export their workflow entry points via WIT exports.
- **Activities** are WASM components with a different capability set — they import WASI interfaces for I/O (HTTP client, filesystem, database access) and export activity execution functions. Because activities perform I/O, they are not deterministic, but they are isolated.
- **The Conductor Runtime** implements the host side of the WIT interfaces, bridging between WASM components and the Conductor server. When a workflow component calls `schedule-activity("extract-data", params)`, the runtime records this as a command, sends it to the server, and suspends the component. When the activity completes, the runtime resumes the component with the result.

This model preserves Temporal's workflow/activity separation while providing stronger isolation and determinism guarantees through WASM.

### What Conductor Should Borrow from Temporal's Durable Execution Model

- **Event Sourcing**: Record every state transition as an event in an append-only log. This is the foundation of durability and the mechanism for replay-based recovery.
- **Event History as the Source of Truth**: The event history is the canonical record of execution. Replay reconstructs state from history, not from snapshots.
- **Activity Retry Policies**: Configurable, platform-managed retries for activities. This removes retry logic from application code.
- **Signals and Updates**: External communication primitives that integrate with the event history and replay model.
- **The Workflow/Activity Separation**: Deterministic orchestration vs. non-deterministic I/O is a useful conceptual model regardless of runtime.

### How Conductor Can Simplify What Temporal Made Complex

- **No Separate Database Cluster**: Embed SQLite for single-node deployments, with pluggable backends (PostgreSQL, FoundationDB) for scale-out. The obelisk project proves this is viable. Eliminating the Cassandra dependency alone removes the largest source of operational complexity.
- **Single Binary**: Ship Conductor as a single binary that contains the server, the WASM runtime, and the embedded database. For development and small deployments, `conductor serve` should start everything needed.
- **No Service Discovery**: With a single-binary model (or a simple leader-follower pattern for HA), there's no need for Ringpop or any membership protocol. The server either is the single node or uses Raft for leader election.
- **Push-Based Worker Dispatch**: Persistent connections with the server mean no polling infrastructure, no Matching service, no partition-based task queues. The server pushes tasks to workers directly.
- **WASM-Enforced Determinism**: No patching APIs, no versioning APIs, no runtime determinism checkers. The sandbox guarantees determinism.
- **Data-First Abstractions**: Make data lineage, data quality, and data freshness first-class concepts. A pipeline doesn't just execute steps — it produces and consumes datasets, and the platform tracks these relationships automatically.
- **No Continue-As-New**: If the WASM component model allows efficient serialization and deserialization of workflow state (which it does, through the component model's value types), the platform can transparently checkpoint and restart workflows without developer intervention. The 51,200 event limit is an implementation detail of Temporal's specific event history storage model, not a fundamental constraint of durable execution.
