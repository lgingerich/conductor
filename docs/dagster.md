# Dagster: An Architectural Deep Dive

## 1. Overview & Positioning

Dagster is an open-source data orchestration platform created by Nick Schrock, the founder of Elementl (now Dagster Labs). Schrock, a former Facebook engineer and co-creator of GraphQL, laid down the first line of Dagster's code in 2018. The project emerged from his observation that the data engineering tooling landscape suffered from a fundamental architectural mismatch: task-oriented orchestrators like Apache Airflow were increasingly out of step with how modern data teams actually conceived of their work. Where tools like dbt, Fivetran, and Airbyte had shifted the industry toward thinking about data artifacts — tables, models, sources — the orchestrators still demanded that users describe their pipelines as sequences of tasks. This gap motivated Schrock to build a platform that centered data assets rather than execution steps.

The core design philosophy of Dagster is the concept of **software-defined assets** (SDAs). An SDA is a declarative description of a data product — a table, a file, a machine learning model — defined in code, with explicit upstream dependencies, a compute function, and metadata. Unlike Airflow's model, where a DAG describes *what to run and in what order*, Dagster's model describes *what data exists, how it is computed, and what it depends on*. The orchestrator then infers the execution graph from the asset dependency graph, rather than requiring the user to manually construct a task DAG. This inversion is the single most important architectural decision in Dagster and the source of nearly all its differentiating capabilities.

Dagster positions itself as an "asset-oriented orchestrator" versus Airflow's "task-oriented orchestrator" and Prefect's "workflow-oriented orchestrator." It markets itself not merely as a scheduler or runner, but as a unified **control plane for the data platform** — a single pane of glass where teams can see their entire data landscape: what assets exist, their current state, their lineage, their freshness, and their health. The platform is designed to serve the full development lifecycle, from local prototyping and unit testing through CI/CD, staging environments, and production deployment. The primary use cases span data platform engineering (managing complex ELT pipelines), analytics engineering (orchestrating dbt models with full lineage visibility), machine learning pipelines (tracking model artifacts as assets), and general data infrastructure management.

### The Origin Story

Nick Schrock's background deserves attention because it explains much of Dagster's architectural character. As a co-creator of GraphQL at Facebook, Schrock brought a deep understanding of API design, type systems, and developer experience to the problem of data orchestration. He has described Dagster as an attempt to do for data engineering what GraphQL did for API development: provide a typed, self-documenting contract between producers and consumers. The decision to make GraphQL Dagster's primary API surface is not an accident — it reflects Schrock's conviction that a strongly typed query language is the right abstraction for navigating and manipulating a complex graph of interconnected entities, whether they are API resources or data assets.

Schrock's thesis, articulated in his 2021 essay "Dagster and the Decade of Data Engineering," is that the central challenge of the current decade is not big data volume but "Big Complexity" — the proliferation of tools, teams, and dependencies that makes it difficult for organizations to maintain a coherent understanding of their data. Dagster's answer to this complexity is the asset graph: a single, canonical representation of all data products in the organization, their dependencies, and their current state. This graph serves as both a coordination mechanism (knowing what needs to be recomputed when something changes) and an observability tool (understanding the blast radius of a failure).

## 2. Architecture Deep Dive

### The Core Concepts: Software-Defined Assets and Ops/Graphs

Dagster's mental model operates at two layers. The lower layer, inherited from its pre-asset days, is the **op/graph/job** abstraction. An *op* (short for "operation") is a unit of computation — a Python function that takes inputs, does work, and produces outputs. A *graph* is a set of ops wired together by data dependencies, and a *job* is a graph that has been parameterized with a schedule, resources, and configuration to make it executable. This op-based model is Dagster's answer to Airflow's task-based DAGs, and it remains fully supported as a substrate.

The higher layer, introduced experimentally in Dagster 0.12 and stabilized in 0.15, is the **software-defined asset** model. An asset is defined with the `@asset` decorator and consists of three things: an *asset key* (a globally unique name, often mirroring the table or file name it represents), a *compute function* (the Python code that produces the asset's contents), and a set of *upstream asset dependencies* (other assets whose contents are inputs to the computation). Unlike ops, which are wired together explicitly by the user into a graph, assets form their own graph implicitly through dependency declarations. The orchestrator constructs the execution plan by traversing this asset graph, finding which assets need to be recomputed, and determining the optimal execution order.

The relationship between assets and ops is worth understanding precisely. Under the hood, each `@asset` is compiled into an op. The `@asset` decorator is syntactic sugar that creates an op whose output is the asset materialization and whose inputs are the materialized values of the upstream assets. When you define an asset `daily_metrics` that depends on `clean_events`, Dagster creates an op that takes the output of the `clean_events` op as input and produces `daily_metrics` as output. This means the op layer is always present — assets are built on top of it — but the asset layer adds crucial metadata and semantics that the raw op layer lacks: the concept of *identity* (this computation produces *this specific table*), *lineage* (this table depends on *these specific upstream tables*), and *state* (this table was last materialized at *this time* with *this version* of the code).

This separation of business logic from orchestration is one of Dagster's most important architectural contributions. In Airflow, the DAG file mixes scheduling configuration, dependency wiring, and business logic into a single script. In Dagster, the asset definition contains only the business logic and dependency declarations. The scheduling, execution environment, concurrency limits, and monitoring are configured separately — through the Definitions object, the `dagster.yaml` instance configuration, and the Dagster+ deployment settings. This clean separation means you can develop and test an asset's compute function in complete isolation (as a pure Python function) without any orchestration machinery running, then compose it into a production deployment with minimal friction.

**Asset materializations** are the persistent records of an asset being computed. Each time an asset's compute function runs successfully, Dagster records a materialization event that captures the timestamp, the data version, any metadata (row counts, schema snapshots, sample values), and the run ID that produced it. These materializations accumulate over time, forming a complete history of every version of every data product in the organization. **Asset observations** are a lighter-weight concept: they record that an asset was seen or inspected (for example, by querying a source system to check if new data is available) without actually recomputing it. Observations are critical for assets whose computation is managed externally (e.g., a source table produced by Fivetran) — they allow Dagster to track the asset's state without being the executor.

### Dagster's Execution Model

Dagster's execution model is a multi-layered pipeline that transforms a launch request into a completed run. Understanding this pipeline is essential for evaluating its architectural strengths and weaknesses, particularly through the lens of push-versus-pull execution.

#### The Daemon Architecture

The **dagster-daemon** is the long-running background process that powers all asynchronous orchestration in Dagster. It is not a single daemon but a container process that runs multiple daemon threads, each responsible for a specific orchestration concern:

- **Scheduler daemon**: Evaluates cron-based schedules, determines when a scheduled job should fire, and creates run requests. This daemon ticks at a configurable interval (typically every 30 seconds) and checks whether any schedule's next execution time has arrived.
- **Sensor daemon**: Evaluates sensors on a tick interval (also typically every 30 seconds). On each tick, it calls the sensor's evaluation function, which checks some external condition (a new file in S3, a new partition in a source table, a Kafka message) and returns a set of run requests or skip.
- **Run queue daemon**: When the `QueuedRunCoordinator` is configured, this daemon pulls runs from the queue and submits them to the run launcher. It enforces concurrency limits and prioritization rules before launching.
- **Run monitoring daemon**: Monitors the health of run workers. If a run worker crashes or becomes unresponsive, this daemon can detect the failure and mark the run as failed, preventing orphaned runs from consuming resources indefinitely.

The daemon is configured through the `dagster.yaml` instance file, where you specify which daemons are enabled and their tick intervals. Most production deployments run a single daemon process with multiple threads, though the architecture supports running separate daemon processes for different concerns if needed.

#### Run Coordinator and Launch Flow

When a run is submitted — whether from the Dagster UI, a schedule tick, a sensor evaluation, or the GraphQL API — it first passes through the **run coordinator**. The run coordinator is a configurable policy class that decides how to handle the run:

- The **DefaultRunCoordinator** immediately calls the run launcher in the same process, launching the run without any queuing or prioritization. This is simple and low-latency but provides no concurrency control.
- The **QueuedRunCoordinator** enqueues the run into a database-backed queue. The run queue daemon then dequeues runs according to configured prioritization rules and concurrency limits. This enables sophisticated multi-tenancy: you can define instance-level limits on maximum concurrent runs, per-pool concurrency limits, and custom prioritization logic (e.g., "production runs always take priority over development runs").

The run launcher is the component that actually creates the run worker process. Dagster supports multiple launchers:
- The **DefaultRunLauncher** spawns a subprocess on the same machine.
- The **K8sRunLauncher** creates a Kubernetes Job for each run.
- The **DockerRunLauncher** creates a Docker container for each run.
- The **EcsRunLauncher** launches ECS tasks on AWS.

This architecture provides a clean separation between *orchestration policy* (the run coordinator), *infrastructure provisioning* (the run launcher), and *step execution* (the executor). Each layer is independently configurable and swappable.

#### The Executor Abstraction

Once a run worker process is created, the executor takes over responsibility for executing the steps (individual ops or assets) within the run. The executor is configured per-job (via run config) rather than at the instance level, because different jobs have different execution requirements:

- **in_process_executor**: Executes all steps serially within the run worker process. Used primarily for testing and development.
- **multiprocess_executor** (default): Spawns a child process for each step, enabling parallelism. Configurable with a `max_concurrent` parameter.
- **docker_executor**: Executes each step in its own Docker container, providing strong isolation and the ability to use different container images per step.
- **celery_k8s_executor**: Uses Celery as a task queue with Kubernetes pods as workers, suitable for very high-throughput deployments.
- **custom_executor**: Users can implement their own executor by subclassing `Executor`, giving them full control over how steps are dispatched and monitored.

The executor abstraction is one of Dagster's most powerful architectural features, because it decouples the *definition* of what to execute (the asset graph) from the *mechanism* of how to execute it. The same asset graph can be executed in-process during development, in parallel subprocesses during testing, and in isolated Kubernetes pods in production, all by changing a configuration value.

#### Push vs. Pull Analysis

Dagster's execution model is a hybrid of push and pull patterns, and understanding where each is used reveals both the system's strengths and its inherent latency characteristics.

**Pull patterns** (polling-based) dominate several critical paths:

1. The daemon's schedule and sensor evaluation is purely pull-based. Every 30 seconds (by default), the daemon wakes up and checks whether any schedules need to fire or any sensor conditions are met. This means scheduled runs have a minimum latency of up to 30 seconds from the scheduled time, and sensor-triggered runs can miss events that occur and resolve within a single tick interval.

2. The queued run coordinator uses a pull model. The run queue daemon periodically wakes up, checks the queue, and dequeues runs that meet the current concurrency and priority policies. Under heavy load, runs may sit in the queue for multiple polling cycles.

3. In Dagster+ Hybrid deployments, the agent polls the Dagster+ control plane for new work, introducing an additional layer of polling latency between the cloud backend and the user's infrastructure.

4. Sensor evaluation itself often involves polling external systems — checking an S3 bucket for new files, querying a database for new partitions, or polling a message queue — adding further latency layers.

**Push patterns** exist in areas that are more execution-adjacent:

1. When the `DefaultRunCoordinator` is used, run submission from the UI or GraphQL API is push-based — the coordinator calls the launcher immediately.

2. Once a run worker is launched, step execution within the run is push-based. The executor traverses the execution plan and pushes steps to worker processes as soon as their dependencies are satisfied. There is no polling for step readiness; the executor maintains an in-memory DAG of the plan and fires steps as upstreams complete.

3. In Dagster+ Serverless, the control plane can push work directly to managed compute, eliminating the agent polling layer.

The fundamental architectural question Dagster faces is this: the core of any orchestrator is *deciding when to run what*. In Dagster, that decision is made by polling loops (daemon ticks) that evaluate conditions against database state. This is fundamentally a pull model. The system does not have an event-driven, push-based mechanism for detecting "upstream asset X has been updated, therefore downstream assets Y and Z should now be launched." The declarative automation system (AutomationConditions) tries to make this feel declarative, but under the hood it is still evaluated on a polling tick by the automation condition sensor.

### The Dagster Instance and Storage Layer

The **DagsterInstance** is the central orchestrator object in Dagster. It is a singleton within each process that coordinates all persistence, scheduling, and execution operations. The instance is configured through a `dagster.yaml` file and maintains references to the pluggable storage backends for runs, events, and schedules. Every component in the Dagster ecosystem — the webserver, the daemon, the CLI, the user code servers — interacts with the instance to read and write state.

The instance provides a unified API for:
- Creating and querying runs
- Storing and retrieving execution events (materializations, observations, logs)
- Managing schedule and sensor state (tick history, cursor values)
- Querying asset materialization history
- Managing concurrency limits and run queues

The storage layer is separated into three independently configurable backends, all accessible through the instance:

1. **Run Storage**: Persists `DagsterRun` objects, which track the lifecycle of each pipeline execution — its status (queued, started, success, failure), its configuration, its tags, and its association with schedules or sensors. The run storage supports filtering and pagination, enabling the UI to efficiently query runs by status, tag, or time range.

2. **Event Log Storage**: Persists all events that occur during pipeline execution. This is the most heavily trafficked storage component, as every asset materialization, observation, check result, log message, and lifecycle transition writes an event. The event log table (`event_logs`) is indexed on run ID, asset key, event type, and partition, with composite indices optimized for the most common query patterns (e.g., "get all materialization events for this asset key in this partition range").

3. **Schedule/Sensor Storage**: Persists tick history and cursor state for schedules and sensors. Each tick records whether it resulted in a run request, a skip, or an error, and sensors use cursors to track their position in external streams (e.g., "I have processed all events up to timestamp X").

**Storage backends** are pluggable:
- **SQLite** is the default for local development. It uses per-run database files for event logs to avoid lock contention, which works well for small workloads but becomes a significant bottleneck at scale.
- **PostgreSQL** and **MySQL** are recommended for production, providing consolidated tables with connection pooling, proper indexing, and support for concurrent read/write access.
- Users can implement custom storage backends by subclassing the abstract storage interfaces.

The database schema has evolved considerably over Dagster's history. The `event_logs` table alone has gone through multiple index migrations to optimize for the asset-oriented query patterns that became dominant after the SDA paradigm shift. Early versions of the schema were optimized for run-centric queries ("show me all events for this run"), while later versions added asset-centric indices ("show me all materializations for this asset") and partition-centric indices ("show me the status of each partition of this asset").

#### How Components Interact With the Instance

The webserver queries the instance through the GraphQL layer, which translates GraphQL queries into instance method calls. When a user navigates the asset catalog in the Dagster UI, the webserver issues GraphQL queries that hit the event log storage's asset materialization indices. When a user views a run, the webserver queries both the run storage (for run metadata) and the event log storage (for step-level events and logs).

The daemon interacts with the instance for scheduling and sensing. On each tick, the scheduler daemon queries the schedule storage for active schedules, evaluates their cron expressions, and creates run records in the run storage. The sensor daemon loads sensor cursors from the schedule storage, evaluates the sensor function (which runs in the user code server, not the daemon), and creates run requests based on the result.

User code servers do not directly access the instance. Instead, they interact with the instance indirectly through the execution context passed to op and asset functions. When an asset's compute function logs metadata or records an observation, the execution context routes those calls through the gRPC boundary to the run worker, which writes them to the event log storage.

### I/O Manager Architecture

I/O managers are one of Dagster's most elegant architectural abstractions, and understanding them is crucial for appreciating how Dagster decouples computation from storage. An I/O manager is responsible for two operations: `handle_output` (persisting the output of an op or asset) and `load_input` (retrieving a previously persisted output as input to a downstream op or asset).

The key insight behind I/O managers is that most data pipeline code mixes two concerns: transforming data and managing storage. A typical Airflow task might contain SQL queries, file I/O operations, and transformation logic all intertwined. The I/O manager separates these concerns: the asset's compute function contains *only* the transformation logic (it receives DataFrames as input and returns DataFrames as output), and the I/O manager handles *all* storage details (where to read from, where to write to, what format to use).

This separation has profound implications:

1. **Environment portability**: The same asset code can run against a local DuckDB database in development and a production Snowflake instance by swapping the I/O manager. No code changes to the asset functions are needed.

2. **Testability**: Unit tests can use an `InMemoryIOManager` that never touches disk, making tests fast and deterministic. Integration tests can use a local `FilesystemIOManager`. The compute logic is tested identically in both cases.

3. **Lineage capture**: Because the I/O manager is responsible for reading and writing, Dagster can automatically capture the physical location of each materialized asset (the S3 path, the BigQuery table, the Snowflake schema), enriching the asset lineage with deployment-specific storage details.

4. **Caching and recomputation**: The I/O manager can implement caching logic. If an upstream asset hasn't changed, a smart I/O manager can skip recomputation of downstream assets by checking data versions. This is the foundation of Dagster's incremental computation story.

Dagster ships with a library of built-in I/O managers for common data stores: `FilesystemIOManager` (pickle files on local disk), S3, GCS, Azure ADLS2, Snowflake, BigQuery, DuckDB, and many more. Custom I/O managers are straightforward to implement: subclass `IOManager` and implement `handle_output` and `load_input`. This extensibility means teams can build I/O managers tailored to their specific storage infrastructure (e.g., a custom Parquet-on-S3 manager with specific partitioning and compression settings).

The I/O manager abstraction also neatly solves the problem of asset-to-asset data passing. In Dagster, when asset B declares a dependency on asset A, B's compute function receives the *materialized value* of A as its input — but "materialized value" is defined by the I/O manager. With the `DuckDBPandasIOManager`, B receives a Pandas DataFrame loaded from the DuckDB table that A produced. With the `FilesystemIOManager`, B receives a Python object unpickled from the file that A wrote. The asset's compute function never knows or cares where the data physically lives.

### Resources and Configuration

Dagster's resource system models external dependencies — database connections, API clients, file systems, secret stores — as typed objects that can be injected into assets and ops. A resource is defined by subclassing `ConfigurableResource` and declaring its configuration as Pydantic-typed class attributes. For example, a database resource might declare `host`, `port`, `database`, `username`, and `password` as typed attributes, with validation provided by Pydantic's type system.

Resources are bound to assets and ops through function parameter annotations. When an asset function declares a parameter annotated with a resource type, Dagster's dependency injection framework resolves that resource at execution time and passes it to the function. This pattern is familiar to anyone who has used dependency injection in web frameworks like FastAPI or Spring, and it makes resource usage explicit and auditable.

The **configuration system** has undergone a major evolution. In early Dagster versions, configuration was defined using a custom type system (`Field`, `String`, `Int`, etc.) that required users to learn Dagster-specific config primitives. Starting in Dagster 1.3, the team introduced "Pythonic Config and Resources," which replaces the custom type system with Pydantic models. Config is now defined as a `Config` subclass with standard Python type annotations, validated by Pydantic at launch time. This change dramatically reduced the learning curve for configuration and made it possible to use standard Python tooling (type checkers, IDE autocompletion) with Dagster config.

Run-time configuration is passed through the `run_config` mechanism. When launching a run (from the UI, the CLI, a schedule, or a sensor), the user provides a configuration dictionary that maps to the config schemas defined on the assets, ops, and resources. Dagster validates this configuration against the schemas before launching the run, catching misconfigurations early rather than mid-execution. Configuration values can also be sourced from environment variables (via `EnvVar`), which keeps secrets out of config files and aligns with twelve-factor app principles.

### The Webserver / Dagster UI (Dagit)

The Dagster webserver, historically called Dagit, serves both the web UI and the GraphQL API from the same process. The UI is a React application that communicates exclusively through the GraphQL endpoint at `/graphql`. This single-API-surface architecture means that every interaction in the UI — loading the asset catalog, viewing run details, launching a backfill — translates to one or more GraphQL queries or mutations.

The **GraphQL API** is Dagster's primary and only network API surface. Unlike Airflow, which exposes a REST API (with a GraphQL API added later), Dagster was designed with GraphQL from the start. The schema is extensive, covering runs, assets, schedules, sensors, partitions, resources, code locations, instance configuration, and more. The API supports both queries (for reading state) and mutations (for launching runs, reloading code locations, toggling schedules, etc.), as well as subscriptions for real-time updates (run progress, log streaming).

The GraphQL-first design has significant implications. On the positive side, it provides a single, well-typed, self-documenting API surface. GraphQL's field selection means clients can request exactly the data they need, avoiding over-fetching. The schema serves as both documentation and contract. On the negative side, teams that prefer REST APIs find the GraphQL-only surface frustrating — there is no simple `GET /runs` endpoint for quick curl-based debugging or integration with monitoring tools that expect REST interfaces. The GraphQL schema has also undergone breaking changes between versions, though the team has committed to documenting these in release notes.

The Dagster UI is organized around the asset graph rather than a list of DAGs. The primary navigation paradigm is the **global asset graph**, a visual representation of all assets across all code locations, showing their dependencies, materialization status, freshness, and health. Users can click on any asset to see its full history of materializations, its code definition, its upstream and downstream dependencies, its partition status, and its configured automation conditions. This asset-first navigation is radically different from Airflow's DAG-first approach and reflects Dagster's core philosophy: the user's primary concern is their data products, not the tasks that produce them.

### Schedules and Sensors

Schedules are Dagster's mechanism for time-based execution triggers. A schedule is associated with a job and defines a cron expression that determines when the job should run. The schedule can include a decider function (e.g., "only fire on weekdays" or "skip if a specific condition is met"), and it can provide run configuration that varies based on the scheduled time (e.g., "run with yesterday's date as the partition").

Sensors are Dagster's mechanism for event-driven execution triggers. A sensor defines an evaluation function that Dagster calls on each tick (typically every 30 seconds). The function examines some external state — new files in S3, new records in a database, messages in a Kafka topic — and returns either a set of run requests or a skip. Sensors are more flexible than schedules but also more complex, because the user must manage cursor state (tracking which events have already been processed) and handle idempotency (ensuring the same event doesn't trigger duplicate runs).

The **declarative automation** system, introduced in Dagster 1.5 and significantly enhanced through 1.12, represents a major architectural evolution away from imperative sensors toward declarative conditions. The key API is `AutomationCondition`, which allows users to attach conditions directly to asset definitions using composable primitives:

- `AutomationCondition.eager()` — materialize this asset whenever any of its upstream dependencies are updated
- `AutomationCondition.on_cron("* * * * *")` — materialize on a cron schedule after upstream dependencies have updated
- `AutomationCondition.on_missing()` — materialize if the asset has never been materialized
- Composed conditions using `&` and `|` operators

These conditions are evaluated by an `AutomationConditionSensorDefinition`, which runs on a tick interval (default 30 seconds). On each tick, it evaluates the condition tree for each asset and launches runs for any assets whose conditions are satisfied. The critical advantage of declarative automation over imperative sensors is that the conditions are inspectable. In the Dagster UI, each asset displays its condition tree, showing which sub-conditions are currently true or false, making it possible to understand *why* an asset was or was not materialized on any given tick. With imperative sensors, this reasoning requires reading sensor logs and tracing cursor state.

The declarative automation system also has a key architectural advantage: it is **dependency-aware by default**. The `eager()` condition automatically understands that if upstream asset A has been updated and downstream asset B depends on A, then B should be triggered — no explicit sensor code needed. This represents a shift from *imperative triggering* (write code that detects changes) to *declarative policy* (declare the desired behavior, let the system figure out when to trigger).

### Dagster+ (Cloud)

Dagster+ is the managed cloud platform built on top of Dagster's open-source engine. It comes in two deployment models:

**Serverless** is fully managed: Dagster Labs hosts the control plane and provides the compute environment for executing user code. This is the simplest deployment option, suitable for teams that don't want to manage infrastructure.

**Hybrid** is the more architecturally interesting model. The control plane (webserver, GraphQL API, metadata database, and daemons) runs in Dagster's cloud, but user code execution happens in the customer's own infrastructure (VPC, Kubernetes cluster, etc.). The bridge between the control plane and the user's environment is the **Dagster+ agent**, a long-running process that the customer deploys. The agent polls the Dagster+ API for instructions (start a code server, launch a run, evaluate a sensor) and executes them in the customer's environment. All sensitive data remains in the customer's VPC; only metadata and orchestration state flow back to the Dagster+ control plane.

Dagster+ adds several proprietary features on top of the open-source core:

- **Branch deployments**: Automatically creates ephemeral Dagster deployments for each pull request, allowing teams to preview pipeline changes before merging. This brings CI/CD preview environments to data engineering, a capability that has been standard in web development for years but is historically challenging for data pipelines.

- **Insights**: Historical analytics on pipeline performance, cost, and reliability trends. Answers questions like "why are our pipelines taking longer this month than last month?"

- **Alerts**: Configurable notifications (Slack, PagerDuty, email) for run failures, asset freshness violations, and metric thresholds.

- **Asset Catalog**: An enhanced search and discovery interface with column-level lineage, metadata search, and integration with external catalogs.

- **SSO, RBAC, and audit logs**: Enterprise security and governance features.

### Partitioning and Backfills

Partitioning is a first-class concept in Dagster's asset model. A partitioned asset is one that represents a logical collection of data slices, each identified by a partition key. Dagster supports four partition types:

- **Static partitions**: A fixed set of partition keys defined at code time (e.g., `["us", "eu", "apac"]`).
- **Time-based partitions**: Partitions defined by a time window (e.g., daily from 2020-01-01 to present).
- **Dynamic partitions**: Partitions whose keys are not known at code time but are discovered at runtime (e.g., new regions added to a dataset). Sensors can dynamically add partition keys.
- **Multi-dimensional partitions**: Partitions that span two independent dimensions (e.g., `region x date`), enabling a matrix of data slices.

Partition dependencies are managed through **partition mappings**. When downstream asset B depends on upstream asset A, and both are partitioned, Dagster needs to determine which partitions of A correspond to each partition of B. For assets with the same partitioning scheme, the mapping is one-to-one (each daily partition of B depends on the same day's partition of A). For assets with different schemes (e.g., daily upstream, weekly downstream), Dagster provides built-in partition mappings (`TimeWindowPartitionMapping`, `IdentityPartitionMapping`) and supports custom mappings for complex cases.

**Backfills** are the mechanism for (re)computing historical partitions. By default, a backfill over N partitions launches N separate runs, one per partition. This provides isolation and granular retry, but the run overhead can be significant for large backfills. Dagster supports single-run backfills (configured via `BackfillPolicy.single_run()`) where a single run processes a range of partitions, which is more efficient for parallel-processing engines like Spark or Snowflake.

### Code Locations and Workspaces

The code location architecture is Dagster's mechanism for loading, isolating, and communicating with user code. A code location is an independently loadable Python environment that contains a `Definitions` object — a collection of assets, jobs, schedules, sensors, and resources. Each code location runs in its own process (or container), communicating with Dagster system processes via gRPC.

This architecture provides several critical benefits:

- **Dependency isolation**: Different teams can use different versions of libraries without conflicts. The finance team's code location can use `pandas==1.5` while the ML team's uses `pandas==2.0`.
- **Fault isolation**: A crash in one code location (due to a bug in user code) does not affect other code locations or the Dagster system processes.
- **Independent deployment**: Each code location can be deployed independently, enabling multi-team workflows where each team owns their own deployment pipeline.

The **workspace** is the configuration that tells Dagster which code locations exist and how to load them. It is defined in a `workspace.yaml` file:

```yaml
load_from:
  - python_file:
      relative_path: "sales/assets.py"
      location_name: "sales"
  - python_file:
      relative_path: "marketing/assets.py"
      location_name: "marketing"
  - grpc_server:
      host: "ml-code-server"
      port: 4000
      location_name: "machine_learning"
```

When the webserver starts, it loads each code location from the workspace. For Python file/module entries, Dagster spawns a subprocess to load the code. For gRPC server entries, Dagster connects to the already-running gRPC server. The code location server exposes methods for listing definitions, evaluating schedules and sensors, and launching runs — all communication between system processes and user code flows through this gRPC interface.

Code location reloading is a subtle but important operational concern. In production deployments using `dagster api grpc`, the only way to reload code is to restart the container or pod running the gRPC server — the "Reload" button in the UI effectively does nothing. Dagster 1.3.6 introduced the `dagster code-server start` command as an alternative that runs user code in a subprocess and supports hot reloading from the UI, solving this pain point for teams that need rapid iteration without redeployment.

One notable limitation of the code location architecture is that cross-code-location asset dependencies are restricted. An asset in code location A can declare a dependency on an asset in code location B, but the dependency is "external" — the orchestrator treats it as an opaque boundary. Automated triggering across code locations requires coordination between the automation condition sensors in each location. This limitation is intentional (it enforces clean team boundaries) but can be frustrating for organizations that want a fully seamless global asset graph.

## 3. Version Evolution

Dagster's version history traces an arc from experimental framework to stable platform, with several major architectural pivots along the way. Understanding this evolution illuminates why certain design decisions were made and what lessons were learned.

### The 0.x Era (2018–2022): Finding Product-Market Fit

Dagster's early years were characterized by rapid iteration and frequent API churn. The project launched with a computational model centered on **solids** (the original name for ops), **pipelines** (the original name for jobs), and **modes** (environment-specific configurations for pipelines). This solid/pipeline/model model was Dagster's first attempt at a developer-friendly orchestration API, and it attracted early adopters who were frustrated with Airflow's rigidity.

During this period, the team made several significant API changes. The `solid` API was renamed to `op` and `pipeline` to `job` in 0.13.0, reflecting a desire for more intuitive terminology. The `mode` concept was deprecated and replaced by a more flexible resource system. These changes, while necessary for the product's evolution, were painful for early adopters who had to refactor their codebases. One Hacker News commenter who migrated around 100 pipelines noted that "the biggest caveat was full change of internal APIs in 0.13, which forced the team to execute a fairly complicated refactor."

### The Birth of Software-Defined Assets (0.12–0.15, 2021–2022)

The introduction of software-defined assets was Dagster's most important architectural pivot. The `@asset` decorator was introduced experimentally in Dagster 0.12.12, accompanied by a GitHub discussion that laid out the vision: "Conceptually, software-defined assets invert the typical relationship between assets and computation. Instead of defining a graph of ops and recording which assets those ops end up materializing, you define a set of assets, each of which knows how to compute its contents from upstream assets."

The SDAs stabilized in Dagster 0.15.0 (June 2022), which declared them "fully stable and ready for prime time — we recommend using them whenever your goal using Dagster is to build and maintain data assets." This was a clear statement of product direction: the op/graph/job model was being demoted from primary interface to underlying substrate, and the asset model was becoming the recommended default.

### Dagster 1.0 (August 2022): Stability Declared

Dagster 1.0 was less about new features and more about drawing a line under the API churn. As the release blog post stated: "1.0 doesn't include any seismic changes — rather, it's a marker that indicates we've put the finishing touches on Dagster's core abstractions." The release removed all previously deprecated APIs (solids, pipelines, modes, the old `AssetGroup` API), cleaned up the surface area, and committed to backward compatibility within the 1.x line.

By this point, Dagster had grown from a solo project into a platform with 463 releases, over 200 code contributors, a production-grade scheduler, thirty-three integration libraries, and a web UI. The project had raised venture funding and was building a cloud product (then called Dagster Cloud).

### Dagster 1.1–1.3 (Late 2022–Early 2023): The Asset Layer Matures

The 1.1–1.3 releases focused on making the asset model the default paradigm rather than an alternative to ops. Key developments included:

- **1.1**: Introduction of the `Definitions` API, which replaced the older `@repository` pattern. `Definitions` became the single entry point for declaring all Dagster objects (assets, jobs, schedules, sensors, resources) in a code location.
- **1.2**: `AutoMaterializePolicy` (precursor to `AutomationCondition`) and `FreshnessPolicy`, introducing declarative ways to specify when assets should be recomputed and when they should be considered stale.
- **1.3**: Pythonic Config and Resources graduated from experimental to stable, replacing the custom config type system with Pydantic. The release blog post described it as making Dagster "easier to learn and feel more natural for modern Python users."

### Dagster 1.4–1.7 (2023–2024): Declarative Automation and Scale

The 1.4–1.7 releases expanded the declarative automation system and addressed scale concerns:

- **1.4**: "Material Girl" — Improved asset backfill monitoring, clearer freshness indicators in the UI.
- **1.5**: "How Will I Know?" — `AutomationCondition` was introduced as the successor to `AutoMaterializePolicy`, providing composable, inspectable conditions.
- **1.6**: "Back to Black" — Configurable concurrency limits, improved step-level retry behavior, and the `AssetSpec` API for defining asset metadata without compute functions.
- **1.7**: "Love Plus One" — Enhanced asset selection syntax, improved partition performance, and expanded integration libraries.

### Dagster 1.8–1.10 (2024): Components and Developer Experience

These releases introduced a major new abstraction layer: **Components**. Components are configurable, reusable building blocks that generate Dagster definitions from YAML or Python. The `dg` CLI was introduced as a companion tool for scaffolding projects, managing components, and validating configurations.

- **1.8**: "Call Me Maybe" — Initial Components preview, `dg` CLI preview.
- **1.9**: "Spooky" — Expanded Components library, improved integration marketplace.
- **1.10**: "Mambo No 5" — Unified concurrency pools, FreshnessPolicy API overhaul.

### Dagster 1.11–1.13 (2025): Stabilization and AI

- **1.11**: "Build Me Up Buttercup" — Components and `dg` CLI reached Release Candidate status. `create-dagster` command for one-shot project scaffolding. Partial retries for multi-asset steps.
- **1.12**: "Monster Mash" — Components and `dg` CLI declared GA. FreshnessPolicies stabilized. FreshnessDaemon enabled by default. UI refresh with collapsible sidebar.
- **1.13**: "Octopus's Garden" — AI Skills for AI-assisted coding, partitioned asset checks, virtual assets (preview), state-backed components enabled by default, 20+ new integration components.

### Key Architectural Pivots and Lessons Learned

Several themes emerge from Dagster's evolution:

1. **The asset pivot was both necessary and costly.** Moving from a task-oriented model (solids/ops) to an asset-oriented model required rethinking the entire API surface. The op layer remained as a substrate, but the recommended path shifted to assets. This created a period where documentation, tutorials, and community knowledge were split between the old and new paradigms, contributing to a steeper learning curve for newcomers.

2. **Configuration complexity required multiple attempts.** The original config type system (`Field`, `String`, etc.) was powerful but unfamiliar to Python developers. The Pythonic config system (Pydantic-based) was the right fix, but it meant teams that had invested in the old system had to migrate. The lesson: use standard language idioms for configuration rather than inventing domain-specific type systems.

3. **Declarative automation is harder than it looks.** The progression from manual sensors to `AutoMaterializePolicy` to `AutomationCondition` shows the difficulty of getting the abstraction right. Each iteration added composability and inspectability, but the underlying mechanism (polling-based sensor evaluation) remained unchanged. Truly push-based execution — where materializations cascade automatically without polling — remains an unsolved challenge.

4. **Components represent a bet on platformization.** The Components framework and `dg` CLI shift Dagster from a library-you-write-code-against to a platform-you-configure. This is a significant architectural bet that aims to make Dagster accessible to less Python-proficient practitioners (analytics engineers, data analysts) while preserving the full power of the code-based API for engineers.

## 4. Known Pain Points & Complaints

### The Learning Curve

Dagster's dual mental model (assets and ops) is the single most cited onboarding challenge. New users encounter both paradigms — the tutorials emphasize assets, but the docs and community forums still reference ops, graphs, and jobs extensively. Many users on Reddit have reported finding Dagster's learning curve steeper than Prefect's, particularly because the terminology is less intuitive and the conceptual leap from "what tasks should I run" to "what data assets do I have" requires a mental shift.

The asset-versus-ops bifurcation has practical consequences. A user who starts with ops (because they're migrating from Airflow and thinking in tasks) will produce a codebase that looks fundamentally different from a user who starts with assets. The official recommendation to "use assets whenever your goal is to build and maintain data assets" is clear, but it doesn't fully resolve the ambiguity for users whose work straddles asset production and operational tasks.

### GraphQL-Only API Surface

Dagster's GraphQL-only API is simultaneously one of its best-designed features and one of its most criticized. The GraphQL API is well-typed, self-documenting, and powerful. The interactive GraphQL Playground at `/graphql` is an excellent exploration tool. However, teams that are accustomed to REST APIs find the GraphQL-only approach restrictive. Simple debugging with `curl` becomes verbose, integration with monitoring tools that expect REST endpoints requires GraphQL clients, and the learning curve for GraphQL syntax adds yet another technology to learn.

The GraphQL schema has also undergone breaking changes between versions, though the team documents these in release notes. The statement in the official docs that "the GraphQL API is still evolving and is subject to breaking changes" has been present for years and contributes to a perception of API instability.

### Code Location Loading and Cold Start

Code location loading time is a persistent pain point at scale. Users with large numbers of assets (tens of thousands, as in the case of teams using dlt to load many source tables) report loading times exceeding 10 minutes. The default timeout of 180 seconds is routinely exceeded, requiring configuration changes to increase the timeout. This problem is architectural: loading a code location involves importing all user modules, constructing all asset definitions, resolving all dependencies, and building the internal execution plan snapshots — all before any actual work begins.

The cold start problem is particularly acute in development environments where users are iterating rapidly. Each code change requires a reload, and if the reload takes several minutes, the development feedback loop becomes painful. The `dagster code-server start` command (introduced in 1.3.6) partially addresses this by supporting hot reload without process restart, but it doesn't solve the underlying loading time for teams with very large asset graphs.

### Configuration System Complexity

While the Pythonic config system introduced in 1.3 simplified configuration considerably, the overall configuration landscape remains complex. Users must understand:
- `dagster.yaml` (instance-level configuration: storage backends, run launcher, daemon settings)
- `workspace.yaml` (code location configuration)
- Run config (per-execution configuration: asset parameters, resource credentials)
- Resource configuration (how external systems are parameterized)
- Environment variable configuration (via `EnvVar`)

In Dagster+ deployments, additional configuration layers exist: deployment settings, agent configuration, and CI/CD integration. The path from "I want to run this locally" to "I want to run this in production" involves touching multiple configuration files across different systems, and the debugging surface is large.

### Testing

Dagster's testing story is widely praised as superior to Airflow's. The `execute_in_process` method allows running assets and jobs in-process with full mock support, and the I/O manager abstraction enables swapping out real storage for in-memory alternatives. However, testing at scale introduces friction. Testing an asset that depends on five other assets requires either materializing all five upstreams (which can be slow and requires realistic test data) or mocking all five dependencies (which requires understanding the internal materialization mechanics). Testing sensors requires understanding tick semantics and cursor state. Testing declarative automation conditions requires understanding the condition evaluation lifecycle.

### Multi-Repo and Monorepo Challenges

Code locations enable team isolation, but cross-code-location orchestration is limited. An asset in one code location can declare a dependency on an asset in another code location, but automated triggering across locations requires coordination between the automation sensors in each location. Cross-code-location backfills are not supported. The recommended pattern — use sensors that monitor asset materializations across locations — reintroduces the imperative complexity that declarative automation was supposed to eliminate.

### Sensor Tick Latency and Polling Overhead

The 30-second default tick interval for sensors and automation conditions imposes a floor on trigger latency. A sensor that detects a new file in S3 won't trigger a run until the next tick — up to 30 seconds later. For high-frequency data pipelines processing near-real-time data, this latency can be unacceptable. Users can reduce the tick interval, but this increases load on the database (each tick queries sensor state and event tables) and, as reported in GitHub issues, can cause significant performance degradation with SQLite backends. One user reported that sensors running at 10-second intervals caused the Automation page to take 3–6 minutes to load after several months of operation.

### Performance at Scale

Dagster's database usage has been a recurring source of complaints from large-scale users. GitHub issues document several related problems:

- **Connection exhaustion**: Under concurrent load, Dagster can consume hundreds or even thousands of PostgreSQL connections. One user reported 1,000 connections for just 12 concurrent runs. The issue appears related to connection management in the `dagster/concurrency_key` handling and step execution.
- **Webserver database load**: The webserver can issue queries that drive Postgres CPU to near 100% for sustained periods, seemingly related to a specific commit introduced in 1.7.x.
- **Large asset graph resolution**: Teams with more than 20,000 assets report code location loading times measured in minutes rather than seconds, with default timeouts being exceeded.

These issues appear to stem from architectural choices that work well at moderate scale but degrade under high concurrency or very large asset graphs. The team has addressed some of these with index migrations and query optimization, but the underlying patterns (many small database writes during execution, polling-based state evaluation) create inherent scalability challenges.

### The I/O Manager Abstraction

While elegant in principle, the I/O manager abstraction can introduce confusion in practice. Users wonder why their asset function receives a DataFrame when they wrote it to a database — the answer (the I/O manager loaded it) is not immediately obvious. The indirection between "what my code returns" and "where the data actually lives" can make debugging difficult. Custom I/O managers require understanding Dagster's internal loading and storage protocols, and errors in I/O managers manifest as confusing "asset not found" or "type mismatch" errors.

### Migration from Airflow

Teams migrating from Airflow face a conceptual gap. Airflow DAGs are explicit execution graphs — the DAG file describes exactly what runs when. Dagster assets describe data products and their dependencies — the execution graph is inferred. This inversion requires unlearning Airflow patterns, and the migration process (even with dagster-airlift's task-by-task approach) requires restructuring code to separate orchestration from business logic in ways that Airflow DAGs didn't enforce.

### Dagster+ Pricing and Feature Gating

Some features that feel essential for production use — branch deployments, SSO, RBAC, audit logs — are gated behind Dagster+ pricing tiers. The open-source version is fully functional for orchestration, but teams running at scale will likely need the cloud product's operational features. The credit-based pricing model adds a layer of cost unpredictability.

## 5. Asset-Oriented Architecture Analysis

### What Asset-Orientation Provides

The asset-oriented model's primary value is **alignment between the mental model of the practitioner and the representation in the tool**. Data engineers think about tables, models, dashboards, and ML artifacts — these are the "nouns" of the data platform. Task-oriented orchestrators force practitioners to translate these nouns into verbs (tasks), then manually reconstruct the noun relationships from the task graph. Asset-oriented orchestrators let practitioners work directly with the nouns: define the assets, declare their dependencies, and let the tool figure out the verbs.

This alignment cascades into several concrete benefits:

- **Observability**: Because the system knows that "this table was produced by this asset definition," it can answer questions that are difficult in a task-oriented system: Is this asset up to date? What upstream assets would need to be recomputed if this one changed? What is the blast radius if this source data is delayed?

- **Lineage**: Asset lineage is automatic rather than manually constructed. When every asset declaration includes its upstream dependencies, the global asset graph is a side effect of the code, not a separate documentation artifact that must be maintained.

- **Reusability and composability**: Assets are self-contained declarations that can be composed into different jobs without modifying the asset code. The same asset can be part of a daily backfill job, an ad-hoc reprocessing job, and a CI/CD test job — all without changing the asset definition.

- **Developer experience**: Asset-oriented development maps naturally to the way developers work iteratively. You can develop and test a single asset in isolation, running just its compute function with mocked upstreams, then gradually compose it into larger pipelines.

### What Asset-Orientation Costs

The asset-oriented model introduces complexity that task-oriented systems avoid:

- **Mental model overhead**: Practitioners must learn to think in terms of data products rather than execution steps. For operational tasks that don't produce persistent data (sending an email, triggering a webhook, cleaning up temporary files), the asset model is an awkward fit — these are naturally modeled as ops, not assets.

- **Implicit execution ordering**: The execution graph is inferred from the asset graph, which means the ordering is not always obvious. A task-oriented DAG makes execution order explicit; an asset graph requires the user to reason about which assets will be computed in which order.

- **State management**: The asset model requires tracking materialization state (what was last computed, when, with what code version). This state is powerful for observability but adds database load and query complexity.

- **Abstraction leak**: Assets are compiled into ops, and understanding the interaction between the two layers is necessary for debugging and optimization. The abstraction is not perfectly sealed.

### WASM and Asset Materialization

A WASM-based runtime could complement asset materialization in interesting ways. WASM modules are self-contained, portable compute units with well-defined inputs and outputs — conceptually similar to how Dagster assets are self-contained declarations with well-defined inputs (upstream assets) and outputs (the materialized asset). A WASM-based orchestrator could treat each asset's compute function as a WASM module, achieving stronger isolation than process-based executors while maintaining the portability benefits that Dagster currently achieves through the I/O manager abstraction.

## 6. Execution Model: Push vs Pull Analysis

### Where Dagster Uses Pull

Pull-based polling is Dagster's dominant pattern for orchestration decisions:

1. **Schedule evaluation**: The scheduler daemon polls on a fixed interval, checking whether any schedule's cron expression has been satisfied since the last tick.
2. **Sensor evaluation**: The sensor daemon polls on a fixed interval, calling each sensor's evaluation function.
3. **Automation condition evaluation**: The automation condition sensor polls on a fixed interval, evaluating condition trees for all assets.
4. **Run queue processing**: The run queue daemon polls the database for queued runs.
5. **Agent polling** (Dagster+ Hybrid): The agent polls the Dagster+ API for new work.

### Where Dagster Uses Push

Push-based patterns exist primarily in the execution layer:

1. **Direct run submission**: When using the `DefaultRunCoordinator`, the webserver pushes runs directly to the launcher.
2. **Step execution**: Within a run worker, the executor pushes steps to worker processes as their dependencies are satisfied.
3. **Event streaming**: The run worker pushes events (materializations, logs) to the event log storage as they occur.

### Latency Characteristics

The polling-based orchestration model imposes a fundamental latency floor. Every run triggered by a schedule, sensor, or automation condition experiences additional latency equal to the time between the triggering event and the next polling tick. At the default 30-second tick interval, this means every triggered run has 0–30 seconds of polling latency. Reducing the tick interval reduces latency but increases database load.

For workloads that require sub-second triggering, Dagster's polling model is architecturally unsuitable unless augmented with external push mechanisms (e.g., a custom sensor that listens on a WebSocket rather than polling, or an external system that submits runs directly via the GraphQL API).

### What a Fully Push-Based Dagster Would Look Like

A push-based Dagster would replace the polling daemon with an event-driven architecture:

- Asset materializations would emit events to a message bus (Kafka, NATS, Redis streams).
- A set of subscribers would react to these events: the scheduler subscriber would enqueue scheduled runs, the dependency subscriber would trigger downstream assets, the monitoring subscriber would update freshness status.
- The core loop would be: materialization event → subscriber evaluation → run submission — all without polling.

The architectural challenge is maintaining consistency and correctness in an event-driven system. Dagster's current polling model has the advantage of simplicity: every tick sees a consistent snapshot of database state. An event-driven model would need to handle out-of-order events, duplicate events, and partial failures — problems that message queue systems solve but that add significant complexity to the orchestrator.

## 7. Relevance to Conductor

### What Dagster Got Right

Several of Dagster's architectural decisions are instructive for any new orchestrator:

- **Asset lineage as automatic side effect**: Making dependency declaration part of the asset definition rather than a separate DAG construction step eliminates an entire class of errors where the declared dependencies diverge from the actual dependencies.
- **I/O manager abstraction**: The separation of computation from storage is a genuinely elegant design that enables environment portability and testability. A WASM-based orchestrator could adopt a similar abstraction, treating WASM modules as pure compute functions and externalizing all I/O.
- **Code location architecture**: Process isolation between user code and system code, with gRPC as the communication protocol, is a sound model for multi-tenant deployments. The ability to independently deploy and version code locations is valuable for team autonomy.
- **Configuration validation at launch time**: Validating configuration against a typed schema before execution prevents mid-run failures. This principle applies regardless of whether the runtime is Python or WASM.

### What Dagster Got Wrong or Still Struggles With

- **Polling-based orchestration**: The fundamental latency and scalability limitations of a polling-based scheduler are a direct argument for a push-based alternative.
- **Database load at scale**: Dagster's heavy database usage (many small writes per step, polling queries, connection management issues) creates operational challenges at scale. A push-based system with message queuing could distribute load differently.
- **Cold start and code loading**: The time required to load and resolve large asset graphs is an architectural limitation. A WASM-based system, where modules are pre-compiled and can be loaded lazily, could potentially address this by avoiding the need to import and introspect Python modules at startup.
- **Single-API-surface (GraphQL)**: While GraphQL has advantages, a multi-protocol API (supporting both GraphQL and REST, and potentially gRPC) would serve a broader range of integration scenarios.

### Implications for Push-Based Architecture

Dagster's architecture confirms that the core challenge of an orchestrator is *deciding when to run what*. Dagster solves this with polling loops. A push-based orchestrator would solve it with event processing. The key architectural questions for Conductor:

1. **How are triggering events produced?** In Dagster, events are database writes that are discovered by polling. In a push-based system, events would be messages published to a bus.
2. **How is consistency maintained?** Polling gives you a consistent point-in-time view. Event processing requires handling out-of-order events and ensuring exactly-once processing.
3. **What is the failure mode?** If the polling daemon crashes, runs are delayed. If a message bus partition becomes unavailable, some events may be undelivered. These failure modes have different recovery characteristics.

### Implications for WASM-Based Runtime

A WASM runtime offers several advantages that complement an asset-oriented or task-oriented orchestrator:

- **Strong isolation by default**: WASM modules are sandboxed at the runtime level, not the OS process level. This provides finer-grained isolation than Dagster's process-based executors with lower overhead than Docker-based executors.
- **Polyglot compute**: WASM modules can be compiled from many languages (Rust, Go, C/C++, even Python via Pyodide). This enables a truly polyglot orchestrator where users write compute logic in their preferred language.
- **Deterministic execution**: WASM runtimes can provide deterministic execution guarantees (given the same inputs and WASM binary, the same output is produced), which is valuable for caching and incremental recomputation.
- **Reduced cold start**: WASM modules can be pre-compiled and cached, potentially solving the code loading latency that plagues Python-based orchestrators.

### Asset-Oriented Thinking for a Task-Oriented Tool

Even if Conductor is task-oriented at its core, Dagster's asset model offers valuable design principles:

1. **Make outputs first-class**: Every task should know what data products it produces. Even if the system doesn't automatically infer the execution graph from the asset graph, tracking outputs as named entities enables lineage, observability, and caching that are impossible in a pure-task model.

2. **Separate computation from storage**: Adopt an I/O-manager-like abstraction that decouples task logic from data location. This enables environment portability without changing task code.

3. **Declarative dependencies**: Allow tasks to declare their input and output dependencies declaratively, even if the execution graph is constructed separately. This enables automatic lineage capture.

4. **Materialization as a concept**: Track when each task's output was last produced, with what inputs and what code version. This enables incremental computation and freshness monitoring regardless of the execution model.
