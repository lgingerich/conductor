# Prefect: Architectural Deep Dive

## 1. Overview & Positioning

Prefect is a Python-native workflow orchestration platform created by Prefect Technologies (formerly Prefect.io), a Washington, D.C.-based company founded in 2018 by Jeremiah Lowin. Lowin was previously an Apache Airflow committer and member of the Airflow Project Management Committee, having come from a quantitative finance background as a managing director at AQR Capital Management. His experience inside Airflow gave him an intimate understanding of the tool's foundational design choices — and the conviction that several of them could not be patched without a ground-up redesign. Chris White, who had been working on similar problems at Capital One, joined as the first employee and became CTO. The company has since raised multiple funding rounds, including two in 2021 alone, and has grown to serve enterprise customers including Progressive, Humana, Blackstone, and Cox Automotive.

The name "Prefect" itself reflects the product's core philosophy: in the same way a school prefect is responsible for maintaining order and discipline, the Prefect platform is responsible for maintaining order and reliability across data workflows. But the deeper philosophical foundation is what Lowin calls "negative engineering" — the discipline of anticipating and managing failure rather than assuming success. In Lowin's words, "When an automated workflow fails, it can be catastrophic. The time it takes to discover, triage, and repair a broken workflow can more than erase all the gains from automating it in the first place." This insight, drawn from Lowin's background in risk management, shaped Prefect from day one. The platform was designed not for the happy path but for the inevitable failures: network partitions, API outages, memory exhaustion, and transient infrastructure problems.

Prefect positions itself explicitly against Apache Airflow, the incumbent in the workflow orchestration space. The critique is multi-dimensional. First, Airflow's static DAG model requires workflows to be fully defined before execution begins — there is no way to dynamically determine task structure at runtime based on incoming data. Prefect, by contrast, allows the flow itself to manage its own task graph, discovering and creating tasks dynamically as code executes. Second, Airflow ties scheduling, execution, and metadata into a monolithic scheduler, creating a bottleneck that limits scalability. Prefect's decoupled architecture separates the control plane from the execution plane entirely. Third, Airflow's reliance on YAML and operator-based configuration imposes a "tax on time and energy," as Lowin puts it — engineers must translate their natural Python code into the orchestrator's vocabulary. Prefect's "code as workflows" approach treats the Python code engineers already write as the canonical representation of the workflow, minimizing the translation layer.

The positioning has evolved across Prefect's three major versions. Prefect 1.0 was, in many ways, "Airflow done right" — addressing the static DAG problem and execution model while still operating within a similar architectural paradigm. Prefect 2.0 (codenamed Orion during development) was a first-principles rewrite that introduced the concept of a "coordination plane" rather than an "orchestrator." The distinction matters: an orchestrator controls execution at every step, while a coordination plane observes and guides dataflow without requiring total control, enabling the system to work alongside other applications and their APIs. Prefect 3.0 extended this further with transactional semantics, making workflows resilient by design through atomic task grouping and automatic rollback.

Prefect's primary use cases span traditional data engineering (ETL/ELT pipelines, data warehouse refreshes), machine learning operations (model training, evaluation, deployment), event-driven automation (responding to file arrivals, webhooks, database changes), and infrastructure orchestration. The platform supports batch, event-driven, and interactive execution modes — a deliberate move toward "multi-modal" orchestration that can model workflows in whichever form is most natural for the problem at hand.

## 2. Architecture Deep Dive — Prefect 1.0 (Core + Server/Cloud)

### The Execution Model: Flows and Tasks

Prefect 1.0's architecture centered on two core abstractions: `Flow` and `Task`. A Flow was a container for a directed acyclic graph (DAG) of Tasks, where each Task represented a discrete unit of work — a Python function wrapped with the `@task` decorator. The DAG was defined by the data dependencies between tasks: when Task B accepted the output of Task A as input, Prefect automatically established an upstream-downstream relationship. Users could also explicitly define dependencies through `set_upstream()`, `set_downstream()`, or `set_dependencies()` methods, or through the functional `upstream_tasks` keyword argument. This dual API (functional and imperative) was a known source of confusion that Prefect 2.0 would later eliminate by standardizing on the functional approach.

The execution of a Flow was handled by the `FlowRunner` class, which was instantiated when `Flow.run()` was called. The FlowRunner's job was to orchestrate the execution of tasks in dependency order, managing the overall Flow state. Task execution itself was delegated to the `TaskRunner`, which handled the lifecycle of individual tasks. Both runners operated according to a state machine that governed how work transitioned through the system.

The key insight of Prefect 1.0's execution model was that the Flow managed its own execution. Unlike Airflow, where the central scheduler tracks every task across every DAG, Prefect flows were self-contained execution units. Once a flow run was initiated, it operated independently. The central scheduler's only job was to determine when a scheduled flow should start — it did not need to track individual tasks within the flow. This decoupling was fundamental and would be carried forward and amplified in later versions.

### The State Machine

State was the "main currency" of the Prefect 1.0 platform. Every flow run and task run was represented by a `State` object that tracked its current status. The state hierarchy was designed with a clear progression:

- **Pending**: The initial state, indicating work waiting to begin. Subtypes included `Scheduled` (waiting for a specific start time), `Paused` (manually paused), and `Submitted` (a meta-state wrapping another state to indicate it had been handled).
- **Running**: The active execution state, indicating that code was currently executing.
- **Finished**: Terminal states. Subtypes included `Success` (completed normally), `Failed` (completed with an error), `Cached` (result retrieved from cache), `Looped` (a looping task iteration), and `Retrying` (a transient failure triggering a retry).
- **Queued**: A meta-state wrapping another state to indicate that a transition to `Running` could not occur, typically due to resource constraints (concurrency limits, lack of available slots).

State transitions were not arbitrary. They flowed through the Prefect API, which validated each proposed transition against orchestration policies — rule sets that enforced business logic. The `CoreFlowPolicy` and `CoreTaskPolicy` classes defined these rules, checking conditions such as "can a Pending task transition to Running?" or "should a Failed task be retried?" This policy-driven approach meant that orchestration logic was centralized and consistent, rather than scattered across individual flow implementations.

State handlers were callbacks that users could register to respond to state changes. They were invoked whenever a flow or task changed state, enabling custom behaviors such as sending notifications on failure, cleaning up resources, or logging metrics. This was the primary extension mechanism in Prefect 1.0.

### The Hybrid Model: Prefect Core, Server, and Cloud

Prefect 1.0 introduced what the company called the "Hybrid Execution Model" — a patented approach to separating workflow orchestration from workflow execution. The model was built around three components:

**Prefect Core** was the open-source engine. It contained the Flow/Task abstractions, the execution runners, the state machine, and the client libraries. Users wrote their workflow code using Core and could execute it entirely locally without any server component. This was the "Python script with superpowers" experience: add decorators, get observability and retries.

**Prefect Server** was the self-hosted orchestration backend. It provided a GraphQL API (later replaced by REST in 2.0), a PostgreSQL or SQLite database for state persistence, and a web UI for monitoring. When a user "registered" a flow with Server, they sent metadata about the flow's structure — task names, dependency graph, schedule — but not the actual code. This registration step was required before Server could orchestrate the flow.

**Prefect Cloud** was the managed SaaS version of Server, adding multi-tenancy, team management, and enterprise features on top of the same API. The critical architectural property was that in both Server and Cloud modes, the actual workflow code always executed on the user's infrastructure. Prefect Cloud never saw the user's code or data. It only received metadata: state transitions, logs, and run history.

The mechanism for bridging the control plane and execution plane was the **Agent** — a lightweight process running on the user's infrastructure that polled the Server or Cloud API for scheduled work. When an Agent found a flow run in a `Scheduled` state, it would launch the flow execution in the configured environment (local process, Docker container, Kubernetes job, etc.). The Agent would then monitor the execution and report state changes back to the API. This polling model — the Agent pulling work from the server — was a key characteristic of Prefect 1.0 and represented what would later be described as the "pull model" elements that still existed.

### The Pull Model in Prefect 1.0

Despite Prefect's positioning as a more dynamic alternative to Airflow, Prefect 1.0 still had significant pull-model characteristics. The Agent polling loop was fundamentally a pull mechanism: the Agent asked the server "is there work for me?" at regular intervals. There was no server-initiated push of work to agents. This meant that between polling intervals, scheduled work would sit idle. The polling frequency created a trade-off between responsiveness (poll more often) and API load (poll less often).

Additionally, flow registration was itself a form of pull-oriented pre-configuration. Before a flow could be orchestrated, its metadata had to be pushed to the server. This was a one-time push that enabled ongoing pull-based orchestration. The requirement to pre-register flows created friction in CI/CD pipelines and made dynamic, programmatically-generated workflows more cumbersome than they needed to be.

The execution model within a flow run, however, was push-oriented: tasks pushed their state transitions to the API, and the flow runner pushed new tasks into execution as dependencies were satisfied. So Prefect 1.0 was a hybrid in more ways than one — push at the task level, pull at the scheduling level.

## 3. Architecture Deep Dive — Prefect 2.0 (Orion) and 3.0

### The Orchestration Engine

Prefect 2.0, originally developed under the codename "Orion," represented a first-principles rewrite of the orchestration engine. The central insight driving the rewrite was that Prefect 1.0, despite its improvements over Airflow, still demanded too much from users. Engineers still had to contort their code to fit the orchestrator's expectations — pre-registering flows, managing separate deployment configurations, and working within the constraints of a DAG-first model.

Orion inverted the relationship between code and orchestrator. Instead of requiring code to conform to the orchestrator's data structures, Orion embraced "code as workflows" — the idea that the Python code engineers already write is already the best representation of their workflow. Any additional modifications required by the orchestrator are, in Lowin's framework, "negative engineering" — unwanted work imposed by the tool. Orion's goal was to eliminate as much negative engineering as possible.

The architectural change was from an "orchestrator" to a "coordination plane." An orchestrator explicitly controls execution at each step, requiring total authority over the workflow's lifecycle. A coordination plane, by contrast, observes dataflow as it moves through the stack without necessarily controlling it. It works with other applications and their APIs to form a picture of everything that is happening and guide dataflow toward successful outcomes. This philosophical shift had concrete architectural implications: the API became a state-machine-as-a-service, accepting proposed state transitions from any source and validating them against orchestration rules.

The Prefect 2.0 server is built on **FastAPI**, providing a REST API (replacing the GraphQL API from 1.0). The database layer uses **SQLAlchemy** with an asynchronous engine, supporting PostgreSQL (recommended for production) and SQLite (for development and lightweight deployments). **Alembic** manages database migrations, applied automatically when the server starts. The database persists flow run and task run state, run history, logs, deployments, concurrency limits, storage blocks, variables, artifacts, work pool status, events, and automations.

For multi-server deployments, Prefect uses **Redis** as a message bus. The `Docket` library (backed by Redis) coordinates periodic work — scheduling, late run detection, automation trigger evaluation — across multiple background service processes, ensuring each scheduled task runs exactly once per interval even when multiple server replicas are active. This allows horizontal scaling of both the API server and background services behind a load balancer like NGINX or Traefik.

### Work Pools and Workers

The work pool abstraction is one of the most significant architectural innovations in Prefect 2.0, replacing the executor and agent model from 1.0. A work pool is an interface between the orchestration layer and execution infrastructure. It defines the *type* of infrastructure that will execute flow runs and the *delivery method* by which work reaches that infrastructure.

Work pools come in three categories, distinguished by how work is delivered:

**Pull work pools** require a worker (or legacy agent) to poll the work pool for scheduled flow runs. The worker is a lightweight process that runs in the user's execution environment. It polls the Prefect API at a configurable interval, claims flow runs that are ready for execution, provisions the necessary infrastructure (Docker container, Kubernetes pod, ECS task, etc.) according to the work pool's base job template, and monitors execution. Workers are typed: a `docker` worker can only poll `docker` work pools, a `kubernetes` worker can only poll `kubernetes` work pools, and so on. This type binding ensures that deployments assigned to a specific work pool will always execute in a known environment.

The base job template is a JSON configuration that defines the default infrastructure settings for all jobs in a work pool. It has two sections: `variables` (configurable parameters with defaults and descriptions) and `job_configuration` (the actual infrastructure specification, referencing variables via `{{ variable_name }}` syntax). For a Kubernetes work pool, the job configuration is a full Kubernetes Job manifest. For a Docker work pool, it specifies the image, command, environment variables, and resource limits. Deployments and individual flow runs can override these defaults through `job_variables`, enabling per-run infrastructure customization without modifying the work pool.

**Push work pools** invert the polling model. Instead of a worker pulling work from the pool, the Prefect orchestration engine directly submits flow runs to serverless infrastructure — AWS ECS, Azure Container Instances, Google Cloud Run, or Modal. This eliminates the need for a persistent worker process entirely: when a flow run is ready, Prefect Cloud creates the container job, the code executes, reports back, and the container disappears. Push work pools are a Prefect Cloud feature; they require cloud provider credentials stored as Prefect Blocks. The trade-off is simplicity (no worker to manage) versus flexibility (flow runs capped at 24 hours, less control over infrastructure).

**Managed work pools** are administered entirely by Prefect and handle the submission and execution of code on the user's behalf. These are part of Prefect's "Serverless" offering, where Prefect provisions and manages the compute environment.

The worker model represents a progression from the agent model. Workers were introduced in late Prefect 2.x as "next-generation agents" and became standard in Prefect 3.0. The key differences: workers have stronger infrastructure typing, improved governance through base job templates, and more flexible compute layer selection. Unlike agents, which used separate infrastructure blocks and storage blocks, workers consolidate infrastructure configuration directly into the work pool.

### Deployments and Scheduling

Prefect 2.0 eliminated the flow pre-registration requirement. Instead of sending flow metadata to the server before execution, users create **Deployments** — configuration objects that specify the entry point to flow code, a schedule, and the execution infrastructure. A deployment answers three questions: *what* to run (the flow), *when* to run it (the schedule), and *where* to run it (the work pool).

Deployments can be created through two paths. The **Python SDK path** uses `flow.deploy()` or `flow.serve()` to programmatically define deployments. `flow.serve()` creates a long-running process that polls for scheduled runs and executes them locally — ideal for simple deployments without complex infrastructure. `flow.deploy()` registers the deployment with a work pool and requires a worker to pick up the work. The **YAML path** uses a `prefect.yaml` file with declarative configuration that includes `build`, `push`, and `pull` sections. The `build` step typically creates a Docker image; the `push` step uploads it to a registry; the `pull` step — the only required action — defines how the execution environment retrieves the flow code at runtime. The `prefect deploy` CLI command processes this YAML file and creates deployments. A dedicated GitHub Action (`actions-prefect-deploy`) supports CI/CD integration, building images, and deploying flows as part of automated pipelines.

The **Scheduler service** is a background loop that evaluates each deployment's active schedules and creates new flow runs. It runs as part of the Prefect server's background services (started with `prefect server start`) and is a built-in service of Prefect Cloud. The scheduler maintains a buffer of future runs: it ensures at least three runs are always scheduled, caps the total at 100 runs, and will not schedule runs more than 100 days in advance. The scheduler operates independently of execution — it only creates runs in a `Scheduled` state. A separate "recent deployment scheduler" runs on a tighter loop to accelerate scheduling for newly created or updated deployments, ensuring users see runs appear quickly without waiting for the main scheduler cycle.

Schedules support three formats: **Cron** (standard cron expressions with timezone support), **Interval** (fixed intervals in seconds or timedeltas, not tied to absolute time), and **RRule** (iCalendar RFC 5545 recurrence rules, supporting complex patterns like "the last weekday of every month" or "every other Tuesday" with exclusion support). Multiple schedules can be attached to a single deployment, and changing a schedule causes previously scheduled-but-not-started runs to be removed and replaced.

Event-driven scheduling is handled through **Automations** and **Triggers**. An automation consists of a trigger condition and one or more actions. Triggers can be reactive (responding to events that occur, such as a flow run completing or a webhook receiving data) or proactive (responding to the *absence* of expected events within a time window — for example, alerting when an expected flow run hasn't started within 30 minutes of its scheduled time). Triggers can be composite, combining multiple event and metric conditions. Actions include running deployments, pausing or resuming work pools and queues, sending notifications, calling webhooks, and sending emails. Prefect 3.0 open-sourced the event-driven engine that was previously exclusive to Prefect Cloud, making event-driven workflows available to self-hosted users.

### State and Execution Model

Prefect 2.0 refined the state machine from 1.0, making it more explicit and debuggable. The state progression for a flow run is: **Scheduled** → **Pending** → **Running** → **Completed** (or **Failed**, **Crashed**, **Cancelled**). The key distinction between Prefect 1.0 and 2.0 is that in 2.0, every state transition is proposed to the API server, which validates it against orchestration policies before persisting. This means the server is always the source of truth for workflow state, even though the actual execution happens elsewhere.

Task execution within a flow is governed by a **TaskRunner**. Prefect 3.0 ships with a rewritten client-side engine that runs all code on the main thread by default — a change from 2.0's complex async/sync interleaving logic that proved difficult to maintain and created edge cases. The 3.0 engine exposes separate synchronous and asynchronous engines rather than attempting to unify them. This simplification was critical in achieving the 90%+ overhead reduction that 3.0 claims over 2.0.

Tasks can be executed through several patterns: direct invocation (blocking execution in the caller's thread), `.submit()` (returns a `PrefectFuture` immediately for concurrent execution via a task runner), `.map()` (submits multiple instances of a task over an iterable of inputs, enabling data parallelism), and `.delay()` (submits a task to a background task worker for execution in a separate process, useful for web applications that need to dispatch work without blocking).

**Retries** are configured per-task with a maximum count, delay (constant or exponential backoff), and optional retry condition function. When a task fails, the task engine checks whether retries remain; if so, it schedules a new attempt after the configured delay. The state machine transitions through `Retrying` → `Scheduled` → `Pending` → `Running`, preserving the full history of attempts.

**Caching** in Prefect 3.0 is built on top of the transactional layer. Every task run is governed by a transaction that computes a cache key by hashing the task's inputs, definition, and context. Before execution, Prefect looks up this key in the configured result storage. If an unexpired record exists, the task enters a `Cached` state and returns the stored result without executing. Cache isolation levels include `READ_COMMITTED` (default, allows concurrent executions of the same task) and `SERIALIZABLE` (ensures only one execution at a time via a locking mechanism). This makes workflows naturally idempotent — rerunning a flow with the same inputs will reuse cached results rather than recomputing.

### The Prefect Server / API

The Prefect server is a FastAPI application defined in `src/prefect/server/api/server.py`. It mounts a comprehensive set of routers for all orchestration entities: flows, flow runs, tasks, task runs, deployments, work pools, work queues, blocks, variables, artifacts, events, automations, and logs. The API is the single interface through which clients (the Python SDK, the CLI, the UI) interact with the orchestration system.

The database schema is defined by SQLAlchemy ORM models in the server source code. Key models include `Flow`, `FlowRun`, `TaskRun`, `Deployment`, `WorkPool`, `WorkQueue`, `Block`, `Variable`, `Artifact`, `Event`, and `Automation`. All state transitions are persisted through these models, providing a complete audit trail of workflow execution.

The server and workers communicate through a well-defined protocol. Workers poll the API's work pool endpoints to claim scheduled flow runs. When a worker claims a run, it receives the deployment configuration, including the pull steps and job variables. The worker provisions infrastructure according to the work pool's base job template, executes the pull steps to retrieve code, and launches the flow. The running flow process communicates with the API independently — reporting state transitions, sending logs, and persisting results. This means the worker's role is limited to infrastructure provisioning; once the flow is running, it communicates directly with the API.

Background services run alongside the API server: the scheduler (creates scheduled runs), the late run service (detects runs that should have started but haven't), the cancellation cleanup service (cleans up resources for cancelled runs), the event persister (batch-writes events to the database), and the database vacuum service (maintains database health). These services are coordinated through Docket/Redis when multiple server replicas are active.

### Prefect 3.0 Changes

Prefect 3.0 introduced several major architectural changes beyond the performance improvements already discussed:

**Transactional semantics** is the headline feature. Every task in Prefect 3.0 executes within a transaction that governs when and where its result record is persisted. The transaction lifecycle has four stages: **STAGE** (the task's return value is staged for persistence), **ROLLBACK** (if an error occurs after staging, staged data is discarded), **COMMIT** (the result is written to its configured storage location), and completion. Transactions can be grouped using the `transaction()` context manager, creating atomic units where all tasks succeed or all roll back together. Each transaction can register `on_commit` and `on_rollback` hooks for managing side effects — for example, cleaning up external resources if the transaction fails. The `SERIALIZABLE` isolation level uses lock managers (e.g., `FileSystemLockManager`) to prevent race conditions on shared state. This transactional layer is the foundation for Prefect 3.0's caching, automatically providing idempotency without explicit user configuration.

**Events and automations** were open-sourced in 3.0, bringing the event-driven engine previously exclusive to Cloud into the OSS product. Events are structured notifications (JSON objects with an event name, resource labels, a timestamp, and a payload) emitted by Prefect components and integrations. The event system supports webhooks — unique HTTP endpoints that transform incoming requests into Prefect events using Jinja2 templates — and CloudEvents for interoperability with other systems. Automations consume events and execute actions based on trigger conditions.

**Infrastructure decorators** (`@kubernetes`, `@ecs`, `@docker`, etc.) allow binding compute requirements directly to flow functions. This enables composing pipelines that span different compute types in a single Python file — for instance, running a lightweight preprocessing step on a small container and a GPU-intensive training step on a dedicated instance, all defined in the same script. The decorators route to pre-configured work pools, respecting their templates and restrictions.

**Background tasks** via `.delay()` and **Task Workers** introduce a new execution model where tasks are pushed onto a server-side topic and distributed to a pool of worker processes for execution. This is particularly valuable for web applications that need to dispatch work without blocking the request cycle, similar to Celery's task queue pattern.

**Autonomous task execution** in 3.0 allows individual tasks to be called and observed outside the context of a full flow. A decorated function can be called directly and will still receive orchestration and observability benefits — run history, logging, state tracking — without requiring a parent flow deployment. This furthers the "code as workflows" philosophy by eliminating the ceremony of flow setup for simple use cases.

### Prefect Cloud

Prefect Cloud is the managed SaaS control plane. Architecturally, it runs the same FastAPI server as the open-source version but adds enterprise features on top. The critical security property — the one that makes Cloud viable for regulated industries — is that the control plane and execution plane remain completely separated. Prefect Cloud schedules and observes; workers poll outbound from the user's infrastructure. No inbound connections to the user's network are required. The user's code, data, and secrets never leave their environment.

Cloud adds: **SSO** (SAML 2.0 and OIDC with any identity provider), **RBAC** (role-based access control at account, workspace, and object levels), **ACLs** (object-level access control lists for blocks, deployments, and work pools), **Audit logs** (record of every action taken in the platform), **SCIM** (automated user provisioning and directory sync), **IP allowlisting**, **PrivateLink / Private Service Connect / Azure Private Endpoints** (private network connectivity), **automations** (event-driven triggers with metric-based conditions), **webhooks**, **managed work pools**, and **push work pools**. Cloud is SOC 2 Type II certified, GDPR compliant, and HIPAA ready.

Pricing follows a tiered model: Hobby (free, 2 users, 1 workspace, 5 deployments), Starter, Team, Pro, and Enterprise (custom pricing). The hybrid model means Cloud manages the control plane while flow execution happens on the user's infrastructure — so the total cost of ownership includes both Prefect platform fees and the user's cloud compute spend. Prefect Serverless, introduced in 2023, provides a fully managed compute option as an alternative to bring-your-own-infrastructure.

## 4. Version Evolution

### Prefect 0.x → 1.0: The Initial Design

Prefect's earliest versions (0.x, prior to the 1.0 release) were developed during 2018-2020, a period when Airflow was the dominant orchestrator but was showing its age. The initial design was shaped by Lowin's experience as an Airflow contributor who understood both the tool's strengths and its fundamental limitations. The key lessons carried from Airflow were: the importance of Python-native workflow definition (rather than YAML DSLs), the value of a rich state machine for tracking execution, and the need for a UI that made workflow health visible. The key departures were: dynamic DAGs (flows that could determine their task graph at runtime rather than requiring pre-definition), decoupled execution (flows managing their own task graphs rather than a central scheduler tracking everything), and the hybrid model (separating orchestration metadata from execution code).

Prefect 1.0 stabilized these concepts. It preserved backwards compatibility throughout the 1.x lineage while accumulating production experience — by the time Prefect 2.0 was announced in early 2022, Prefect Cloud customers had executed nearly half a billion tasks through the 1.0 engine.

### Prefect 1.0 → 2.0: The Rewrite

The decision to rewrite rather than evolve was driven by a fundamental realization: the "orchestrator" concept itself was the problem. Prefect 1.0, like Airflow, was built around the idea that a central authority should control workflow execution at every step. This required users to pre-register flows, conform to the orchestrator's data structures, and accept the orchestrator's semantics. Even though Prefect 1.0 was more flexible than Airflow, it still imposed an impedance mismatch between natural Python code and the orchestrator's expectations.

Prefect 2.0 (Orion) was announced in early 2022 as a technical preview and reached stable release later that year. The rewrite was "foundational" — the entire orchestration API was rebuilt as a state-machine-as-a-service, the agent model was replaced with work pools and workers, flow pre-registration was eliminated, the GraphQL API was replaced with REST, and the concept of a coordination plane replaced the orchestrator.

Migration from 1.0 to 2.0 was not automatic. While the core concepts (flows, tasks, state machines) carried forward, the APIs changed significantly. Projects were replaced by tags and filters for organizing deployments. The functional and imperative APIs were unified into a single functional API. Caching was rebuilt on the new result persistence system. Storage and execution were consolidated into the work pool model. Prefect maintained 1.0 support for at least one year after the 2.0 release.

### Prefect 2.0 → 3.0: Resilience by Design

Prefect 3.0, released in September 2024, was less of a rewrite than 2.0 was. It built on the 2.0 architecture while introducing transactional semantics as a new foundation. The motivation was a recognition that while 2.0 had dramatically improved the developer experience and execution model, it still left failure handling as an afterthought — retries and caching existed, but data consistency across failures was not a first-class concern.

The transactional layer in 3.0 fundamentally changes how task results are managed. Every task run is wrapped in a transaction, and transactions can be nested and grouped. If any task in a transaction fails, all staged results are rolled back. This means that partial state — where some tasks in a workflow succeeded and others failed, leaving the system in an inconsistent state — can be avoided at the framework level. Combined with automatic caching based on transaction keys, 3.0 makes workflows idempotent by default.

Other 3.0 changes: the client-side engine was rewritten for performance (90%+ overhead reduction), separate sync and async engines replaced the interleaved approach in 2.0, all code runs on the main thread by default, Pydantic v2 is required, the events and automations system was open-sourced, infrastructure decorators were added, task workers and background tasks were introduced, and workers became the standard execution model.

### Key Lessons

The evolution of Prefect reveals several architectural lessons:

1. **Dynamic execution beats static DAGs**: The ability to build task graphs at runtime based on actual data, rather than pre-defining everything, consistently proves more powerful and less constraining than the static approach. Every version of Prefect has pushed further in this direction.

2. **Separation of concerns matters**: Prefect 1.0 separated code from metadata (the hybrid model). Prefect 2.0 separated orchestration from execution (the coordination plane). Each separation reduced coupling and increased flexibility. The lesson is that orchestration systems should specialize — the control plane should focus on state management and observability, while execution should be free to use whatever infrastructure is appropriate.

3. **Polling is a necessary evil with limits**: Prefect 1.0's agent polling model worked but created latency. Prefect 2.0's push work pools and 3.0's task workers show a progression toward reducing polling where possible while accepting it where necessary. The lesson is that push is better than pull for latency, but pull is simpler to implement and secure (outbound-only connections).

4. **Transactions are the right foundation for resilience**: Prefect 3.0's bet is that wrapping every task in a transaction, making caching a natural consequence of transactional semantics, and providing rollback hooks as a first-class feature creates a more robust foundation than bolting retry and caching onto a non-transactional engine. This is a lesson that applies directly to any new orchestration system.

## 5. Known Pain Points & Complaints

### Database Contention and Scaling

The most significant operational pain point with Prefect in production is database contention, particularly on the `flow_run` table. Under high concurrency, Prefect's orchestration logic uses pessimistic locking (`SELECT ... FOR UPDATE`) on flow run rows during state transitions. These locks are held for the entire duration of orchestration logic — loading rules, validating transitions, and performing other operations — causing other queries to queue up and eventually time out. This manifests as cascading timeouts and service degradation under load.

The root cause is architectural: Prefect's orchestration logic requires serialized access to flow run state to prevent race conditions in state transitions. The default SQLAlchemy connection pool size (5) and max overflow (10) are insufficient for heavy workloads, limiting the server to 15 concurrent database connections. Users have reported needing pool sizes of 200+ for production workloads. The `PREFECT_SQLALCHEMY_POOL_SIZE` setting was added in response to community pressure but is not tuned out of the box.

Table bloat on high-write tables (`flow_run`, `events`) compounds the problem. Without aggressive autovacuum tuning, dead tuples accumulate, slowing all queries and causing locks to be held longer — a vicious cycle. Prefect's documentation now includes detailed database maintenance guidance, including monitoring queries, vacuum recommendations, and red flags (disk usage > 80%, table bloat > 100%, autovacuum not running in 24+ hours).

### Work Pool and Worker Complexity

The work pool/worker/queue abstraction, while powerful, has been a persistent source of confusion. Users must understand: work pools (typed infrastructure interfaces), work queues (sub-divisions within pools for prioritization and concurrency control), workers (polling processes that provision infrastructure), base job templates (JSON configuration for infrastructure defaults), job variables (per-deployment overrides), and the distinction between pull, push, and managed pools. The conceptual surface area is large, and the documentation has evolved significantly across versions as the team refined the model.

The migration from agents to workers, while conceptually cleaner, required changes to deployment configuration — replacing infrastructure blocks with work pool configuration, changing `infra_overrides` to `job_variables`, and adopting `flow.from_source()` for remote code retrieval. These changes, documented in an upgrade guide, nonetheless represented a breaking change to the deployment workflow.

### Subflow Orchestration

Subflow handling has been one of the most consistently reported sources of friction. The core issue is that subflows are not first-class flow-level constructs in the orchestration layer. Instead, they are implemented as "virtual task runs" under the parent flow — meaning they inherit task-like orchestration semantics. This has several consequences:

- **Failure propagation**: By default, a subflow failure does not automatically fail the parent flow. The parent can appear "Completed" while children are "Failed," requiring manual inspection to detect problems in deeply nested workflows.
- **Cancellation**: Cascading cancellation does not work reliably for in-process subflows. When a parent is cancelled, the subflow crashes rather than entering a `Cancelling` state, preventing cleanup hooks from running. This is particularly problematic for integrations (Databricks, dbt) that need to tear down resources on cancellation.
- **Concurrency control**: Running many subflows concurrently via `run_deployment()` or async patterns creates race conditions where concurrency limits are exceeded during infrastructure provisioning delays.

The community has explicitly asked for a more native subflow model with built-in state propagation and lifecycle coupling. Prefect's response has been incremental — fixes for specific patterns (task-wrapped deployments, latest-version improvements) rather than a fundamental redesign of subflow semantics.

### Deployment Model Friction

The deployment model, while more flexible than 1.0's registration requirement, introduces its own friction. The `prefect.yaml` file, with its `build`, `push`, and `pull` sections, requires understanding of Docker image building, registry authentication, and code retrieval patterns. The `prefect deploy` command must be able to import the flow module to gather metadata, which means all flow dependencies must be installed in the CI environment — a requirement that the GitHub Action addresses by pre-installing from a requirements file, but which adds complexity.

The `flow.deploy()` Python API and `prefect.yaml` YAML API serve overlapping purposes with different trade-offs, and the documentation has shifted emphasis between them across versions. Prefect 3.7 introduced an auto-`uv run` feature that caused deployments with pre-built Docker images to re-install all dependencies on every run, leading to multi-minute startup delays and increased cloud costs — a bug that was fixed in 3.7.3 with an opt-in setting.

### Cold Start and Import Overhead

Prefect's Python import overhead has been a concern since the earliest versions. `import prefect` itself can take over a second, and the flow run startup process — importing the flow module, establishing API connections, setting up logging — adds latency. For short-lived tasks, this overhead can dominate execution time. The issue is structural: Prefect loads a significant dependency graph (Docker SDK, HTTP clients, serialization libraries) at import time, and the worker model launches a fresh Python process for each flow run.

Prefect 3.0's engine rewrite aimed to reduce this overhead, claiming 90%+ improvement. However, for containerized deployments (ECS, Cloud Run), cold start latency from container scheduling and image pulling still dominates. Users running sub-minute tasks at scale have reported needing workarounds like removing the `@task` decorator to avoid the orchestration overhead.

### Observability Gaps

While Prefect provides rich built-in observability — the UI shows flow run status, task-level state, logs, and CPU/memory metrics — distributed observability across multiple flows, workspaces, and infrastructure components is less mature. The OpenTelemetry integration (added in Prefect 3.1.12) provides automatic span instrumentation for flow and task runs, but exporting those spans to external observability platforms requires manual configuration. Cross-flow tracing — understanding how data flows from one deployment's output to another's input — requires custom instrumentation. The event system provides building blocks but not a pre-built observability solution.

Log aggregation is also fragmented. While `get_run_logger()` captures logs within the flow context and sends them to the Prefect backend, logs from subprocesses, external tools called by flows, and infrastructure components require explicit context propagation using `with_context` or `contextvars`. Prefect 3.0's infrastructure debugging features (lifecycle states, failure diagnostics, resource metrics) have improved this, but the system is not a replacement for a dedicated observability platform.

### Pricing and Open-Source vs. Cloud Gaps

Prefect Cloud's pricing has been a source of community frustration, particularly around the jump from free/hobby tiers to paid professional plans. The Hobby tier is genuinely free (2 users, 5 deployments), but the limits are restrictive for any serious use. At higher tiers, pricing is quote-based rather than transparent, making cost prediction difficult. A notable complaint centered on automation limits: even at the $450/month Team tier, automations were capped at 10, which users argued was insufficient for complex event-driven environments where dozens of "when X finishes, run Y" dependencies are common.

The open-source/core feature gap has narrowed over time — Prefect 3.0 open-sourced events and automations, which were previously Cloud-exclusive. However, metric-based triggers, webhooks, push work pools, managed work pools, SSO, RBAC, audit logs, and advanced analytics remain Cloud features. The gap is primarily in operational and governance features rather than core orchestration capabilities, reflecting Prefect's open-core business model.

### Multi-Tenancy Limitations

Workspace isolation is the primary multi-tenancy mechanism, providing logical separation between environments (dev, staging, prod) or teams. However, true multi-tenancy requires the Enterprise plan for RBAC and ACLs. Without these, all users in a workspace have broad access. Workspace transfer (moving a workspace between accounts) is possible but has significant operational implications — users, API keys, and service accounts may lose access, and flow runs outside retention periods are removed. The self-hosted Prefect server has no built-in multi-tenancy beyond running separate server instances.

## 6. Push Model Deep Analysis

### How Prefect's Push Model Works End-to-End

Prefect's architecture is not purely push-based; it is a hybrid where different components use different communication patterns depending on their role in the system. Understanding the actual flow of a scheduled workflow through the system reveals where push and pull operate:

**Scheduling (push-oriented)**. The Scheduler service evaluates each deployment's schedules and proactively creates flow run records in the database with a `Scheduled` state. This is a push operation: the server-side service determines what should run and creates the records without waiting for external requests.

**Work distribution (pull-oriented, with a push variant)**. In the default pull work pool model, a worker process polls the API at a configurable interval, asking "are there flow runs ready for execution in my work pool?" This is a classic pull pattern — the worker initiates the communication. The polling interval creates latency: a flow run that becomes ready immediately after a poll cycle will wait until the next cycle. The default polling interval is 10-15 seconds, and users can lower it to 1 second at the cost of increased API load. In the push work pool model (Prefect Cloud only), the server pushes work directly to serverless infrastructure, eliminating the polling delay.

**Execution (push-oriented)**. Once a flow run is claimed by a worker and execution begins, the running flow process pushes state transitions to the API. As the flow executes tasks, each state change (Pending → Running → Completed/Failed) is proposed to the API and validated against orchestration policies. This is a push pattern: the executing code initiates communication with the server to report its progress. The server does not poll the executing process.

**Event-driven triggers (push-oriented)**. External systems push events to Prefect via webhooks or the event API. Prefect's automation system evaluates these events against trigger conditions and, when conditions are met, pushes actions (like running a deployment or sending a notification) into execution. This is a server-side push pattern.

The overall architecture is best described as a **server-mediated push model with pull-based work distribution**. The server is the central state authority; work is distributed to execution environments via polling; execution results are pushed back to the server; and events drive reactive automation.

### Where Pull Still Exists

The worker polling loop is the most significant remaining pull element. It exists for practical reasons: workers running in user infrastructure need to initiate connections to the Prefect API (outbound-only, no inbound connections), and polling is the simplest way to achieve this without requiring the server to maintain persistent connections to every worker. The trade-off is latency — even with a 1-second polling interval, there is an average 500ms delay and a maximum of ~1 second delay between a run becoming ready and a worker claiming it.

The `flow.serve()` pattern also uses polling. The serve runner is a lightweight process that polls the API for scheduled runs and executes them in-process. Like workers, it can be tuned for lower latency at the cost of more API calls.

### Latency Characteristics

End-to-end latency for a flow run — from scheduled time to first task execution — has multiple components:

- **Scheduling delay**: The scheduler typically creates runs well in advance, so this is not a factor for on-schedule runs.
- **Work distribution delay**: With default polling (10-15 seconds), 0-15 seconds. With 1-second polling, 0-1 second.
- **Infrastructure provisioning**: For containerized workers (Docker, Kubernetes, ECS), this is typically the dominant factor — seconds to minutes for container startup, image pulling, and dependency installation.
- **Flow import and initialization**: 1-5 seconds for `import prefect`, flow module loading, and API client setup. Prefect 3.0 has reduced this but not eliminated it.

Users running short tasks (sub-minute) have reported that the orchestration overhead can exceed the actual work time. This is an inherent challenge for any orchestration system that provides rich state tracking — the observability comes at a computational cost.

### Late Binding

"Late binding" is the term Prefect uses to describe the decoupling of flow definition from execution infrastructure. In Prefect 1.0, the execution environment was specified at flow registration time — a flow was bound to a particular executor and storage backend. In Prefect 2.0 and 3.0, the execution environment is specified at deployment time (through work pool assignment) and can be further overridden at run time (through job variables). This means the same flow code can execute on different infrastructure — local, Docker, Kubernetes, ECS — without modification, simply by targeting different work pools or overriding job variables.

The innovation matters because it separates the concerns of workflow authors (who define the business logic) from platform engineers (who define the execution infrastructure). A data scientist can write a flow on their laptop and test it locally, and the same code, when deployed, can run on a GPU-equipped Kubernetes cluster with appropriate resource limits — all without changing the flow code. Infrastructure decorators in 3.0 take this further by allowing the code itself to express infrastructure preferences, which are then routed through work pools that enforce platform policies.

### Trade-offs of the Push Model

**Advantages over Airflow's pull model**:
- Lower execution latency: once a flow is running, tasks push their state changes immediately rather than waiting for the scheduler's next cycle.
- Dynamic task graphs: the flow itself manages its task graph, so tasks can be created and executed based on runtime data without central coordination.
- Better scalability: the server is not a bottleneck for task-level orchestration — it only processes state transitions and policy validation, not task scheduling.
- Simplified recovery: the flow process is responsible for its own execution; if the server is temporarily unavailable, the flow can continue executing and report state when the server returns.

**Disadvantages**:
- Worker polling introduces latency at the flow-start boundary. This is mitigated by push work pools (Cloud) and low-latency polling configurations, but not eliminated.
- The server is a single point of state authority. If the server is unreachable, flow runs cannot report state (though execution can continue). This is inherent to any architecture that separates execution from state management.
- Cold start overhead from fresh process initialization per flow run. Persistent worker pools (like Celery's prefork model) would reduce this but introduce state management complexity — Prefect has chosen isolation (fresh process per run) over performance (reusing processes).

## 7. Relevance to Conductor

### What Prefect Got Right

Prefect's architectural decisions that are most relevant to a next-generation orchestration tool using push-based execution and WASM runtimes:

**The coordination plane concept**. Prefect's insight that an orchestration system should observe and guide rather than control is directly applicable to Conductor. A WASM-based runtime, by its nature, provides strong isolation guarantees — the host cannot arbitrarily inspect or control the guest's execution. This aligns with the coordination plane model: the host (Conductor) provides the runtime environment, observes execution through instrumentation points, and manages state transitions, but does not need to control every step of execution.

**The state machine as a service**. Prefect's approach of centralizing state management in an API that validates transitions against policies is clean and composable. For Conductor, a similar state machine could govern WASM module execution, with policies that handle retries, caching, and error recovery. The API-mediated state model also enables multi-language clients — any language that can make HTTP requests can participate in the orchestration system.

**Late binding of execution infrastructure**. Prefect's separation of flow definition from execution environment is a pattern Conductor should adopt. A WASM module should be deployable to different execution contexts — local, edge, cloud — without modification. The deployment configuration (work pool equivalent) should specify the execution environment, not the module code.

**Transactional semantics**. Prefect 3.0's insight that transactions are the right foundation for resilience is particularly relevant for WASM-based execution. WASM modules have deterministic execution boundaries and explicit imports — they are naturally transactional in the sense that a module either completes or fails, with clearly defined side effects (host function calls). Building a transactional layer into Conductor from the start, rather than bolting it on later, would provide the idempotency and rollback guarantees that Prefect 3.0 retrofitted.

**Dynamic workflow construction**. Prefect's support for runtime-determined task graphs — spawning tasks based on query results, iterating over dynamic inputs — is a key advantage over static DAG systems. Conductor should support this natively, allowing WASM modules to dynamically determine their downstream dependencies based on execution results.

### What Prefect Got Wrong or Still Struggles With

**Database as a bottleneck**. Prefect's reliance on a relational database for state management creates scaling challenges under load. The pessimistic locking strategy for flow run state transitions is a fundamental architectural choice that limits horizontal scalability. For Conductor, consider whether an event-sourced model (like Temporal's) or a distributed consensus approach could avoid the single-database bottleneck. If a relational database is used, the locking strategy should be designed for high concurrency from the start.

**Polling-based work distribution**. Despite the "push model" marketing, Prefect's core work distribution mechanism is polling. This creates a latency floor that cannot be eliminated without architectural change. For Conductor, true push-based work distribution — where the server actively dispatches work to available runners — should be a design goal. gRPC streaming, WebSocket connections, or message queues (NATS, Redis streams) could enable genuinely push-based dispatching with sub-millisecond latency.

**Process isolation overhead**. Prefect's model of launching a fresh Python process per flow run provides strong isolation but adds latency. WASM runtimes have a fundamentally different characteristic: module instantiation is extremely fast (microseconds to milliseconds) compared to process or container startup. This means Conductor could achieve both strong isolation (WASM sandbox guarantees) and low latency (fast instantiation), potentially making the "cold start" problem much less severe.

**Subflow semantics**. Prefect's subflow handling — where subflows are virtual task runs rather than first-class flow constructs — creates subtle failure propagation and cancellation issues. Conductor should design sub-workflow orchestration as a first-class concept from the start, with explicit lifecycle coupling, automatic failure propagation, and cascading cancellation.

### Push-Based Architecture Implications for Conductor

Conductor's push-based execution model should be genuinely push-driven at all layers:

- **Work dispatch**: Use persistent connections (gRPC streams or WebSockets) between the control plane and WASM runners, allowing the server to push work to runners as soon as it's ready. This eliminates the polling latency floor.
- **State reporting**: Runners push state transitions back to the server via the same persistent connection. This is essentially what Prefect does, and it works well.
- **Event-driven orchestration**: External events (webhooks, file system notifications, database change feeds) push into the system and trigger workflow execution. Prefect's event system is a good model here.
- **Backpressure**: With true push-based dispatch, backpressure handling becomes critical. If runners are saturated, the server must queue work or reject it. Prefect's work queues handle this at the polling level; Conductor would need to handle it at the dispatch level.

### WASM Runtime Implications

WASM as a container runtime changes the execution model in several ways that differentiate Conductor from Prefect's container-based approach:

**Instantiation speed**: WASM modules can be instantiated in microseconds, compared to seconds or minutes for Docker containers. This fundamentally changes the economics of task granularity — Conductor can support very fine-grained tasks (individual function calls) without the overhead that makes such granularity impractical in container-based systems. Prefect's `@task` decorator adds orchestration overhead that encourages coarser task boundaries; Conductor could make task boundaries nearly free.

**Deterministic execution**: WASM modules have bounded, explicitly declared interfaces. The host controls all imports. This makes execution more predictable and securable than container-based approaches, where the full Linux userspace is available. For a coordination plane, this predictability is valuable — the orchestration system can make stronger guarantees about what a task can and cannot do.

**Portability**: WASM modules are truly portable across architectures and operating systems. A module compiled once can run on any WASM runtime, regardless of the underlying hardware. This extends Prefect's "late binding" concept further — not just different infrastructure providers, but different CPU architectures, edge devices, and embedded systems.

**Resource constraints**: WASM runtimes can enforce fine-grained resource limits (memory, CPU time, fuel/instruction count) at the module level. This enables per-task resource governance that is more precise than container-level cgroups. Conductor could use this to implement exact cost accounting and prevent resource exhaustion at the task level.

**Side-effect management**: WASM's model of explicit imports means that all external interactions (network, filesystem, random number generation) must go through the host. This creates natural interception points for the orchestration system — every external call can be logged, metered, and potentially rolled back. This is a more robust foundation for transactional semantics than Prefect's Python-level hooks, which can be bypassed by direct library calls.

### Containerization Contrast

Prefect handles containerization through Docker-based workers: flow code is baked into a Docker image, pushed to a registry, and pulled at runtime. This provides strong isolation but at the cost of cold start latency, image size management, and registry dependency. WASM offers a different trade-off:

- Instead of a Docker image containing an operating system, libraries, and application code (hundreds of megabytes to gigabytes), a WASM module contains only the compiled application logic (kilobytes to megabytes).
- Instead of a container registry, WASM modules can be distributed through OCI registries (using the OCI artifact specification), HTTP endpoints, or embedded in deployment configurations.
- Instead of a container runtime (containerd, Docker Engine), Conductor uses a WASM runtime (Wasmtime, WasmEdge, WAMR) that can be embedded directly in the worker process.

The result is a system that can achieve the isolation benefits of containerization with the startup speed of function calls. This is not a capability that Prefect's architecture could easily adopt without fundamental changes to its worker and execution model.

### Summary Assessment

Prefect is the most architecturally relevant competitor for Conductor. Its evolution from pull-based orchestration (1.0) to coordination plane (2.0) to transactional resilience (3.0) provides a roadmap of lessons learned. The push model, late binding, state machine design, and transactional semantics are all patterns Conductor should adopt. The areas where Prefect struggles — database contention, polling latency, cold starts, subflow semantics — are areas where WASM-based execution could provide genuine architectural advantages, not just incremental improvements. A coordination plane designed from the start for push-based dispatch over persistent connections, backed by an event-sourced state model, running WASM modules with microsecond instantiation and fine-grained resource control, could address the fundamental tensions that Prefect's architecture has been progressively working around.
