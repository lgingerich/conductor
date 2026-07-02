# Apache Airflow: Architectural Deep Dive & Competitive Analysis

This document is a comprehensive architectural analysis of Apache Airflow, written to inform the design of Conductor — a next-generation data orchestration tool using a push-based execution model and WASM container runtimes. It covers Airflow's architecture in exhaustive detail, traces its evolution across major versions, catalogues known pain points and community complaints, and draws specific lessons for what Conductor should borrow, avoid, or reimagine.

---

## 1. Overview & Positioning

Apache Airflow is an open-source workflow orchestration platform originally created at Airbnb in 2014 by Maxime Beauchemin. It was accepted into the Apache Incubator in 2016 and graduated as a top-level Apache project in 2019. Today, Airflow is maintained by a large community of contributors and commercial backers, the most prominent being Astronomer, which provides a managed Airflow service called Astro. Other managed Airflow offerings include Google Cloud Composer, Amazon MWAA (Managed Workflows for Apache Airflow), and several smaller providers.

Airflow's core design philosophy is "workflows as code." DAGs (Directed Acyclic Graphs) are defined in Python files, where developers use Airflow's operator library to declare tasks and their dependencies. Airflow parses these Python files at runtime to build representations of the workflows, schedules them according to cron-like expressions, and executes tasks by dispatching them to workers. The platform's design emphasizes flexibility — any Python code can be a DAG, any function can be an operator — along with extensibility through a plugin system and a vast library of pre-built operators (over 2,000 provider integrations as of 2026).

The primary use case for Airflow is batch data pipeline orchestration: ETL/ELT jobs, data warehouse transformations, machine learning model training workflows, report generation, and infrastructure automation. Its users are predominantly data engineers and data platform teams. Major deployments exist at Uber (450,000 pipeline runs daily across 1,000 teams), Stripe (150,000 tasks per day), and LinkedIn (10,000+ parallel DAGs). Airflow holds roughly 70% market share in the workflow orchestration space, making it the de facto standard and the primary competitor for any new entrant.

---

## 2. Architecture Deep Dive

### 2.1 Scheduler Architecture

The scheduler is the brain of Airflow. Its primary responsibility is to monitor all DAGs and task instances, determine which tasks are ready to run, and dispatch them to an executor for execution. The scheduler operates as a long-running process that executes a continuous loop — this is the "pull-based" heart of Airflow's architecture.

The scheduler loop, implemented in `SchedulerJobRunner._run_scheduler_loop`, executes in a fixed sequence on every iteration. First, it harvests DAG parsing results from the DagFileProcessorManager — this means it checks whether the DAG file processor has finished parsing any DAG files since the last loop. The parsed DAGs are serialized into the metadata database's `serialized_dag` table. Second, the scheduler examines the database for DAG runs that need to be created or updated, and for task instances whose dependencies are now met. Third, it transitions task instance states in the database — marking tasks as queued, checking for deadlocks, applying retry logic, and detecting zombie tasks. Fourth, it queues executable tasks into the executor's task queue. Fifth, it performs an executor heartbeat — calling into the executor to check on the status of running tasks and synchronizing state back to the metadata database. Finally, it runs periodic maintenance operations such as cleaning up old database records and checking for SLA misses.

This entire loop runs continuously with a configurable sleep interval between iterations (controlled by `scheduler_idle_sleep_time`). The loop speed is critical: if the loop takes too long — because there are many DAGs to examine, many task instances to evaluate, or database queries are slow — scheduling latency increases for all DAGs. The scheduling throughput under ideal conditions is roughly 500–1,000 tasks per second, but this degrades rapidly under real-world conditions when many tasks are blocked or when mapped tasks create large fan-outs.

**DAG Parsing and the DagFileProcessor.** DAG parsing is one of Airflow's most architecturally significant subsystems. Every DAG definition is a Python file. The scheduler (or a standalone DAG processor service, in Airflow 3+) must parse these files to extract DAG objects, serialize them, and store them in the metadata database. This parsing happens continuously — by default, every DAG file is re-parsed every 30 seconds (controlled by `min_file_process_interval`).

The `DagFileProcessorManager` runs a dedicated loop that discovers, sorts, and dispatches DAG files for parsing. It spawns `DagFileProcessorProcess` subprocesses (the number controlled by `parsing_processes`, defaulting to 2) that each parse one file at a time. Each parsing subprocess has a strict timeout (`dag_file_processor_timeout`, defaulting to 180 seconds) after which it is killed. The parsing process imports the Python file as a module, finds DAG objects within it, serializes them, and returns the results to the manager. The DAG parsing loop is a significant source of CPU usage and can become a bottleneck at scale — environments with hundreds or thousands of DAGs routinely spend more CPU cycles on parsing than on actual task execution.

A key architectural detail of Airflow 2.x is that the DAG processor and the scheduler communicate over a Linux pipe with a 64KB buffer capacity. If the scheduler produces callbacks (SLA callbacks, task failure notifications) faster than the DAG processor can consume them, the pipe fills up and the scheduler blocks, unable to perform heartbeats or schedule new tasks. This was a known bug (Issue #41869) that could cause the scheduler to be repeatedly killed by Kubernetes liveness probes if deployed in a containerized environment.

**The DagBag Concept.** The term "DagBag" refers to the collection of all DAGs known to a running Airflow instance. When the DAG processor parses a file, it produces a DagBag containing the DAGs defined in that file. These are serialized into the `serialized_dag` table. The scheduler then constructs its own in-memory DagBag by reading from this table, not by re-parsing the files. This separation — parsing happens once, scheduling reads from the database — is a critical performance optimization introduced in Airflow 1.10 via DAG serialization.

**DAG Serialization.** Before Airflow 1.10.0, the scheduler, webserver, and workers each independently parsed every DAG file. This meant that DAG parsing overhead was multiplied by the number of components accessing the DAGs. DAG serialization (AIP-20) changed this: the DAG processor now parses each file once, serializes the resulting DAG objects (Python's pickle-like serialization, stored as JSON in the `serialized_dag` table), and all other components read the serialized form from the database. This eliminated redundant parsing and removed the requirement that all Airflow components have access to the DAG files and identical Python environments — important for scaling to distributed worker architectures.

However, DAG serialization introduced its own problems. The serialized representation can diverge from the actual DAG if there are bugs in the serialization/deserialization code. The serialization format has evolved across versions, sometimes breaking backward compatibility. And the `serialized_dag` table itself grows over time, especially in Airflow 3 with DAG versioning, where multiple versions of each DAG are retained.

### 2.2 Executor Architecture

Executors are the pluggable abstraction that determines how Airflow task instances are actually run. The executor is configured per Airflow deployment and is initialized as part of the scheduler process. The scheduler calls `executor.queue_task_instance()` to submit a task, `executor.heartbeat()` to synchronize state, and `executor.sync()` to check on completions. The executor manages its own internal task queue and communicates with the metadata database to update task states.

Airflow ships with several executors, each representing a different point on the isolation-versus-latency spectrum:

**SequentialExecutor** (removed in Airflow 3): The simplest executor, it runs one task at a time in a subprocess of the scheduler. Only useful for development and testing; incapable of parallelism. Was tied to SQLite as the metadata database backend. Replaced by `LocalExecutor` in Airflow 3, which can also work with SQLite for local development.

**LocalExecutor**: Runs tasks in subprocesses on the same machine as the scheduler, using Python's `multiprocessing` module. Tasks run with parallelism limited by `parallelism` config. Suitable for single-machine deployments with modest workload. No separate worker processes needed — the scheduler itself spawns task subprocesses. State tracking happens in-process via a multiprocessing queue.

**CeleryExecutor**: The most widely used production executor in Airflow 2.x. It introduces a message broker (RabbitMQ or Redis) as an intermediary between the scheduler and workers. The scheduler publishes task execution commands to Celery queues. Independent Celery worker processes, which can run on separate machines, poll these queues and execute tasks. The result backend (typically the same Redis or RabbitMQ instance) stores task return states. This architecture enables horizontal scaling — add more Celery workers to increase parallelism — at the cost of operating an additional infrastructure component (the message broker) and dealing with Celery's own complexity and failure modes.

The Celery model is explicitly pull-based: each worker process continuously polls its designated queues for new tasks. Workers can be configured to listen to specific queues, enabling task routing (e.g., send GPU tasks to GPU-equipped workers). The `CeleryExecutor`'s `start()` method is a no-op — all state is managed during the `heartbeat()` call where the scheduler publishes tasks and checks results.

**KubernetesExecutor**: Introduced in Airflow 1.10.0 and significantly re-architected in 2.0. Instead of maintaining persistent worker processes, the KubernetesExecutor creates a dedicated Kubernetes Pod for every task instance. The scheduler interacts with the Kubernetes API directly to create, monitor, and terminate pods. Each task pod runs a single task and terminates upon completion (or failure). This provides strong task isolation and per-task resource specification, and scales down to zero resource usage when idle. The trade-off is pod startup latency — typically 10–60 seconds per task — which makes it unsuitable for workloads with many short-duration tasks.

The KubernetesExecutor's initialization is substantially more complex than the CeleryExecutor's. It instantiates a task queue, a result queue, an `AirflowKubernetesScheduler` (which manages the lifecycle of Kubernetes watchers), a Kubernetes client, and an event scheduler for managing pod state transitions.

**CeleryKubernetesExecutor** (removed in Airflow 3): Allowed simultaneous use of both executors, with task-level routing to choose which executor handled a given task. Replaced in Airflow 3 by the multiple executor configuration feature, which generalizes this concept.

**Task State Transitions.** Regardless of executor, task instances follow a defined state machine. A task begins in the `scheduled` state. The scheduler transitions it to `queued` when it is ready to run and the executor has accepted it. The executor assigns a workload token (in Airflow 3, used for API authentication). The task becomes `running` when a worker picks it up. It then transitions to `success`, `failed`, `skipped`, or `up_for_retry` depending on the outcome. The scheduler handles retry logic — if a task fails and has retries remaining, it transitions back to `scheduled` after a delay.

**Heartbeats and Zombie Detection.** While a task is running, its worker process sends periodic heartbeats to the metadata database (Airflow 2) or to the API server (Airflow 3). The scheduler periodically scans for tasks in the `running` state whose heartbeat timestamp exceeds a configurable threshold (`scheduler_zombie_task_threshold`). These are "zombie tasks" — tasks that the metadata database thinks are running but whose actual worker process has died (due to OOM kill, network partition, node failure, etc.). Zombie tasks are marked as failed and, if retries remain, rescheduled. The undead detection works in the reverse direction: if the scheduler finds a task running that shouldn't be (e.g., because its DAG run was marked failed), it kills the task.

### 2.3 Metadata Database

The metadata database is the central state store for all of Airflow. Every component — scheduler, executor, workers, webserver, and triggerer — reads from and writes to this database. It is the single source of truth for DAG definitions, DAG runs, task instances, connections, variables, users, roles, permissions, XComs, logs, and operational state.

Airflow uses SQLAlchemy as its ORM layer, theoretically supporting any SQLAlchemy-compatible backend. In practice, PostgreSQL is the strongly recommended backend for production; MySQL is supported but historically has had more issues. SQLite is used for development but is unsuitable for any production workload due to its lack of concurrent write support.

**Schema and Key Tables.** The database schema is large and has grown significantly across versions. The most important tables are:

- `dag`: Metadata about each DAG (owner, schedule, description, whether it's paused).
- `serialized_dag`: The serialized representation of each DAG, including all tasks and their dependencies. This is what the scheduler, webserver, and API server read instead of parsing DAG files. In Airflow 3 with DAG versioning, this table stores multiple versions per DAG.
- `dag_run`: One row per execution of a DAG, tracking the run ID, state, execution date, and associated DAG version.
- `task_instance`: One row per execution of a task, tracking state, timestamps, retry count, the executor that ran it, and (in Airflow 3) the `dag_version_id` foreign key. This is the largest and most heavily queried table in a production deployment.
- `dag_code`: Stores the actual Python source code of each DAG file.
- `dag_version`: In Airflow 3, tracks structural versions of each DAG.
- `xcom`: Cross-communication between tasks, storing small serialized data payloads (pickle or JSON). Can grow very large if tasks exchange significant data.
- `connection`: Credentials and connection strings for external systems.
- `variable`: Key-value configuration store, accessible from DAG code.
- `log`: Task execution logs (when stored in the database, which is uncommon in production).
- Alembic migration tracking tables.

**Scalability Concerns.** The database-as-central-state-store model is Airflow's most significant architectural limitation. Several problems converge:

First, connection pressure. Airflow components open many database connections — the scheduler opens connections for each scheduling loop iteration, each DAG file processor opens connections, each worker opens connections for heartbeat and state updates, the webserver and API server open connections for each HTTP request. Production deployments routinely exceed PostgreSQL's default connection limit (100), requiring PgBouncer as a connection pooler. Even with PgBouncer, the connection overhead is substantial.

Second, query volume and lock contention. The scheduler executes complex queries on every loop iteration — scanning the `task_instance` table for runnable tasks, locking rows for state transitions, checking dependencies (the `TaskInstance.are_dependencies_met` method is a known CPU-intensive query). When many task instances exist (hundreds of thousands or millions), even indexed queries become slow. GitHub issue #54283 documents a case where a 500-DAG deployment with 5-minute schedules degraded from sub-minute task execution to 10+ minute delays after just 12 hours of running, because the `dag_run` and `task_instance` tables had accumulated 100,000 and 3 million rows respectively, and the scheduler's full-table scans became too expensive. The fix was running `airflow db clean` to trim old records.

Third, the database is a single point of failure. If the database becomes unavailable or slow, all of Airflow stops: scheduling halts, running tasks cannot update their state, the UI becomes unresponsive. High-availability database setups (replicas, failover) mitigate this but add operational complexity.

Fourth, the database is also a security boundary concern. In Airflow 2.x, tasks had direct database access — a DAG author could write arbitrary SQL against the metadata database from within a task. This was both a security risk and an architectural leakage. Airflow 3 addresses this by routing all task interactions through the API server and prohibiting direct database access from task code.

Fifth, missing indexes have been a recurring source of production incidents. As recently as Airflow 3.3.0, a migration was added to create indexes on `serialized_dag(dag_id, last_updated)`, `dag_code(dag_id)`, and `task_instance(dag_version_id)` — columns that had been in production for months without proper indexing, causing full table scans on the `task_instance` table when querying by DAG version. The `task_instance` table in particular is a persistent source of schema drift and missing-index performance regressions.

### 2.4 Webserver & API

**Airflow 2.x: Flask-AppBuilder Monolith.** The Airflow 2 webserver was built on Flask and Flask-AppBuilder (FAB). It served a server-rendered HTML UI alongside a REST API (`/api/v1`). FAB provided authentication (LDAP, OAuth, database-backed users), role-based access control (RBAC), and CRUD views for Airflow models. The webserver queried the metadata database directly for every page load — rendering the DAG tree view, graph view, task instance lists, and other screens required multiple complex database queries whose performance degraded with scale. The webserver was monolithic: UI rendering, REST API, authentication, and static file serving all ran in a single Flask process.

The FAB integration, while powerful, created significant coupling. FAB's permission model — which mapped every endpoint to a permission-view pair in the database — made it difficult to add or change UI routes. DAG-level access control required creating permission-view mappings for every endpoint per DAG, a combinatorial explosion that FB's model handled poorly. The Airflow 2.x UI also aged poorly compared to modern React-based interfaces, with slow page loads, limited interactivity, and a graph view that became unusable for DAGs with more than a few hundred tasks.

**Airflow 3.x: FastAPI API Server + React UI.** Airflow 3 completely rewrites this layer. The `airflow webserver` command is replaced by `airflow api-server`, which runs a FastAPI-based server. This server serves three applications: the v2 REST API (a stable, versioned API replacing the v1 Flask API), an internal API for the React-based UI that hosts static JavaScript assets, and the Task Execution API (AIP-72) that workers use to interact with Airflow during task execution.

The React UI is a ground-up rewrite that provides faster navigation, better DAG visualization (the graph view now handles large DAGs), and a more responsive user experience. Authentication is decoupled from the webserver — Airflow 3 defaults to SimpleAuthManager (a basic authentication manager suitable for development) but supports the FAB auth manager as a separate provider package for production RBAC needs. This separation means the API server can be scaled independently of auth concerns.

Critically, the API server is now the sole access point to the metadata database for tasks and workers. In Airflow 2.x, every worker had a direct database connection. In Airflow 3, the worker communicates exclusively with the API server over HTTP/gRPC, which proxies all database operations. This is a significant architectural improvement for security and operational control.

### 2.5 Worker Model & Task Execution Lifecycle

When the scheduler identifies a task ready to run, it queues it with the executor. The executor's responsibility is to get the task running on a worker. The exact mechanism depends on the executor:

- **Celery Workers**: Each Celery worker process runs `airflow celery worker`, which starts a Celery consumer that polls the configured queues (RabbitMQ or Redis). When a task message arrives, the worker spawns a `LocalTaskJobProcess` — a subprocess that handles the actual task execution. The `LocalTaskJob` is the unified execution harness: it sets up the task instance context (variables, connections, XCom access), runs operator-specific logic through the `TaskRunner`, monitors for completion or timeout, handles retries, and reports the final state back to the metadata database.

- **Kubernetes Pods**: For each task, the KubernetesExecutor creates a pod running the Airflow worker image. The pod's entrypoint runs the task in a subprocess, mimicking the `LocalTaskJob` flow but in an isolated container.

- **Airflow 3 Task Execution Model**: The architecture changes significantly. The worker's Supervisor process spawns an isolated Task Runner subprocess for each task. The Supervisor and Task Runner communicate via STDIN/STDOUT pipes. When the task code needs an Airflow resource (a variable, a connection, an XCom value), it writes a structured request to STDOUT. The Supervisor intercepts this, calls the Task Execution API on the API server, and writes the response to the Task Runner's STDIN. State transitions and heartbeats flow through the same channel. This model eliminates direct database access from task code entirely, creating a clean security boundary where the API server is the gatekeeper for all Airflow state.

**The Task Lifecycle** proceeds through well-defined stages: The scheduler marks the task as `scheduled`, the executor assigns a workload token and transitions it to `queued`, a worker picks up the task and transitions it to `running`, the worker's Supervisor spawns the Task Runner and monitors its heartbeats, and upon completion (or failure), the Supervisor reports the final state via the API server, which updates the metadata database.

**Logging Architecture.** Task logs are captured from the Task Runner's STDOUT and STDERR. In Airflow 2.x, logs are written to the local filesystem on the worker by default, then optionally uploaded to remote storage (S3, GCS, Azure Blob, CloudWatch) upon task completion. The webserver can serve remote logs by fetching them from the configured remote storage backend. In Airflow 3, the logging architecture is similar but the logs flow through the Supervisor and API server rather than directly from the worker to storage. Real-time log streaming during task execution is served by an HTTP server on the worker that the UI connects to directly.

---

## 3. Version Evolution

### Airflow 1.x Era (2014–2020): The Monolith

The original Airflow architecture was comparatively simple. A single `airflow webserver` process served the UI. A single `airflow scheduler` process ran the scheduling loop and spawned task subprocesses locally (via `LocalExecutor`). All components shared a single SQLite or MySQL/Postgres database. The DAG folder was the source of truth — every component independently parsed every DAG file.

This architecture worked for modest deployments but had fundamental scaling limits. The scheduler was a single point of both failure and throughput bottleneck. DAG parsing overhead multiplied by component count. Memory usage grew linearly with the number of DAGs because every component kept parsed DAG objects in memory. There was no high-availability story — if the scheduler died, all scheduling stopped.

Key milestones in the 1.x series: The `CeleryExecutor` was introduced to enable horizontal worker scaling. The `KubernetesExecutor` arrived in 1.10.0. DAG serialization (AIP-20) landed in 1.10.0–1.10.3, fundamentally changing how DAGs were shared among components. Flask-AppBuilder RBAC was introduced in 1.10.0 as a replacement for the earlier, simpler authentication system.

### Airflow 2.0 (December 2020): The Great Rewrite

Airflow 2.0 was the most significant release in the project's history until 3.0, representing roughly two years of development. The release notes spanned over 3,000 lines. The primary architectural motivations were: eliminating the scheduler as a single point of failure (HA scheduling), modernizing the codebase after years of organic growth, and fixing deeply embedded design issues that had accumulated since the original Airbnb prototype.

Major architectural changes in 2.0 included:

**HA Scheduler**: Airflow 2.0 introduced support for running multiple scheduler instances simultaneously (typically two for active-standby). The schedulers use database-level locking to coordinate — only one scheduler holds the "active" lock at a time, and the other runs in standby, failing over automatically if the active scheduler dies. This was a pragmatic approach that avoided introducing a consensus protocol or distributed coordination service, but it meant the database became even more critical — it is now also the coordinator between schedulers.

**Fully REST API**: The experimental REST API from 1.10 was promoted to stable status in 2.0, providing a programmatic interface for triggering DAG runs, querying task states, and managing connections. This was essential for integrating Airflow into CI/CD pipelines and external systems.

**Provider Package Split**: Airflow was decomposed into a `apache-airflow` core package and over 60 provider packages (one per external service or protocol: `apache-airflow-providers-google`, `apache-airflow-providers-amazon`, etc.). This architectural decision decoupled operator release cycles from core Airflow release cycles, allowing providers to evolve independently. It also reduced the installation footprint — you only install the providers you need.

**TaskFlow API**: The `@task` decorator and functional DAG authoring pattern was introduced, allowing DAGs to be written using Python functions with implicit XCom-based data passing, rather than explicitly wiring operators together with `>>` operators and manually managing XCom push/pull. This was primarily a developer experience improvement, not an architectural change, but it influenced how DAGs are structured and has become the recommended authoring style.

**Simplified KubernetesExecutor**: The KubernetesExecutor was re-architected, removing over 3,000 lines of code. The `executor_config` dictionary was replaced with a `pod_override` parameter accepting a Kubernetes V1Pod object, giving users full access to the Kubernetes API for pod specification.

**Configuration Rationalization**: The sprawling `airflow.cfg` was reorganized into distinct sections. Deprecated configuration options were removed. Component-specific configuration (e.g., pod templates for Kubernetes) was moved to dedicated files.

### Airflow 2.2–2.5: Deferred Execution and Dynamic Mapping

This era introduced two features that materially changed Airflow's execution model:

**Deferrable Operators (AIP-40, Airflow 2.2)**: The Triggerer component was introduced as a new long-running process (alongside the scheduler and webserver). It runs an asyncio event loop capable of efficiently managing thousands of concurrent triggers — small, async workloads that poll external systems for events. When an operator calls `self.defer(trigger, method_name, kwargs)`, the worker releases its slot, the task enters a `deferred` state, and the specified trigger begins running in the triggerer. When the trigger fires (e.g., a sensor detects a file in S3, a Databricks job completes), the scheduler re-queues the task for execution. This dramatically improved resource efficiency for I/O-bound workflows — instead of a worker sitting idle while a sensor polls, the triggerer handles thousands of poll cycles concurrently in a single process.

The trade-off is increased architectural complexity. Deferred tasks create additional database state transitions (deferred -> scheduled -> queued -> running), additional scheduler overhead for re-scheduling, and a new failure domain (the triggerer). There is also a latency cost: when a trigger fires, the task must go through the full scheduling cycle again, which can add several seconds of delay.

**Dynamic Task Mapping (AIP-42, Airflow 2.3)**: This allows a single task definition to expand into multiple parallel task instances at runtime, based on the output of an upstream task. The `.partial()` method specifies static parameters, and `.expand()` specifies dynamic parameters that are resolved at runtime. The scheduler creates `n` copies of the task (one per input element), each with a unique `map_index`. This replaced a common anti-pattern of generating tasks in loops at DAG parse time, which made the DAG structure dependent on data that was only available at runtime.

Dynamic task mapping introduced its own performance characteristics. The `DagRun.create_task_instances` method inserts all mapped task instance rows into the database at once. The scheduler's `are_dependencies_met` check for mapped tasks exhibits O(n²) behavior in certain cases (looping over all finished task instances for each schedulable task instance, as documented in GitHub issue #45991). A 500-way mapped task can take minutes to schedule, not seconds, due to this evaluation overhead.

**Airflow 2.6–2.10**: These releases focused on incremental improvements rather than architectural shifts. Notable additions included the `dag.test()` method (2.5) for running DAGs in a single serialized Python process without a scheduler, the dataset-based scheduling feature (precursor to Assets in 3.0), and the multiple executor configuration feature (2.10.0) that replaced the CeleryKubernetesExecutor with a more general mechanism.

### Airflow 3.0 (2025): Service-Oriented Architecture

Airflow 3.0 is the most significant release in the project's history. It addresses deep architectural limitations that had been accumulating since the original design, while preserving (mostly) backward compatibility for DAG code. The core architectural changes are:

**Service-Oriented Decomposition**: The monolithic webserver is split into separate services. The `airflow api-server` is a FastAPI-based service that serves the REST API (v2), hosts the React UI, and provides the Task Execution API. The DAG processor is now a mandatory standalone service — in Airflow 2, it could be embedded in the scheduler, but in Airflow 3, the scheduler does not parse DAGs at all; it exclusively reads the `serialized_dag` table. The triggerer remains a separate service. Each service can be scaled independently, and the scheduler no longer competes for CPU with DAG parsing.

**Task Execution Interface (AIP-72)**: This is the most fundamental architectural change. Workers no longer connect directly to the metadata database. All runtime interactions — state transitions, heartbeats, XCom operations, variable and connection lookups — flow through the API server via the Task Execution API. The task receives a scoped JWT token at startup. The Supervisor process proxies all API calls, authenticating with the token and receiving refreshed tokens in heartbeat responses. This creates a proper security boundary where task code cannot access the database, cannot impersonate other tasks, and can only access resources it is explicitly authorized to use.

**Task SDK (`airflow.sdk`)**: A stable, forward-compatible namespace for DAG authoring. All DAG primitives (`airflow.sdk.DAG`, `@dag`, `@task`) are imported from this namespace rather than from internal Airflow modules. This provides API stability guarantees — DAGs written against the Task SDK will work across future Airflow versions — and opens the door for multi-language Task SDKs (Go, Rust, etc.) that compile to the same execution interface.

**DAG Versioning (AIP-65, AIP-66)**: Multiple versions of a DAG can coexist in the `serialized_dag` table. DAG runs are tied to a specific version, and the running DAG run uses the version it was started with, even if a newer version is deployed. The UI can display historical DAG structures, enabling inspection of "what did this DAG look like when this run executed?" This is foundational for safer backfills, audit trails, and DAG evolution over time.

**DAG Bundles**: DAGs can be sourced from locations beyond the local filesystem — Git repositories, NFS mounts, object storage — through a DAG bundle abstraction. This improves the deployment model for teams that manage DAGs in version control.

**Removed Features**: SubDAGs (replaced by TaskGroups and asset-aware scheduling), SequentialExecutor (replaced by LocalExecutor), CeleryKubernetesExecutor (replaced by multiple executor config), SLAs (replaced by Deadline Alerts, not yet implemented as of 3.0), and the v1 REST API (replaced by v2 FastAPI API) were all removed. The `catchup` default changed from `True` to `False`, and the `schedule_interval` and `timetable` parameters were unified under a single `schedule` field.

**Migration Experience**: Upgrading to Airflow 3 is a significant effort. Airflow 2.11+ is a required intermediate step. The Ruff linter with AIR rules automates some import path migrations. However, real-world migration experiences report CPU spikes from JWT key divergence in multi-worker API server setups, tasks failing with `Connection refused` because service URLs changed, health checks failing because ports were removed, and user creation silently no-oping because FAB auth manager isn't the default. The `airflow.sdk` import chain triggers a connection to the API server at import time — if the API server is unavailable, the DAG processor hangs on imports and gets SIGKILL'd by its own parse timeout.

**Post-3.0 Developments**: Airflow 3.2 introduced native Python async task support, allowing `async def` functions to be used directly in `@task` decorated functions and `PythonOperator`, using an asyncio event loop on the worker to multiplex concurrent I/O without deferring. Airflow 3.x continues to ship performance fixes, with 3.3 adding long-missing database indexes to combat full table scans.

---

## 4. Known Pain Points & Complaints

### The Pull-Based Scheduler Loop and Scheduling Latency

The scheduler's polling loop is Airflow's most fundamental architectural constraint. Every scheduling decision requires a full pass through the loop: query the database for DAG states, evaluate every pending task's dependencies, queue runnable tasks, heartbeat the executor. If the loop takes 15 seconds and a new DAG run is triggered, that DAG's tasks wait at least 15 seconds before being scheduled — and potentially much longer if the loop processes DAGs in a non-priority order. The situation compound with scale: as the number of DAGs, DAG runs, and task instances grows, each loop iteration takes longer, increasing scheduling latency for all DAGs simultaneously. A single DAG with 5,000 mapped tasks can slow scheduling for the entire deployment because the scheduler evaluates all pending task dependencies every loop cycle, not just the ones that have changed state.

Airflow 3 partially mitigates this by separating DAG parsing from scheduling (the scheduler loop no longer includes parsing overhead), but the fundamental polling architecture remains. The scheduler still scans for work; it does not receive push notifications when work is ready. The `scheduler_idle_sleep_time` parameter controls the minimum loop interval, but setting it too low drives up CPU and database load without proportional benefits.

### DAG Parsing Performance at Scale

At scale — hundreds to thousands of DAGs — DAG parsing becomes the dominant operational concern. Every DAG file is a Python module that must be imported. Top-level code in that module executes on every parse. If a DAG file makes an API call, queries a database, or loads a large library at the top level (outside of task functions), that operation runs every 30 seconds per DAG file, per parser process. The community's best practices — avoid top-level imports, use local imports inside tasks, cache `Variable.get()`, use Jinja templates instead of database calls — all exist because the architecture forces repeated re-execution of DAG file code. As one Hacker News commenter put it, "We've had SO many headaches operating airflow over the years... scheduling overhead in airflow, random race conditions deep in the airflow code."

The `dag_file_processor_timeout` is both a safety mechanism and a source of nondeterminism. If a DAG's parse time exceeds the timeout (180 seconds by default), the parser process is killed. If the parsing of one file affects another (through shared state or resource contention), failures cascade. The DAG processor sorts files for parsing based on `file_parsing_sort_mode`, but large file counts combined with default alphabetical sorting can cause the same subset of files to be repeatedly parsed while files deeper in the sort order starve. Google Cloud Composer's monitoring dashboard flags total DAG parse times exceeding 10 seconds as indicating scheduler overload.

### The Metadata Database as Bottleneck and Single Point of Failure

The metadata database is both the central coordination point and the most common source of production incidents. As documented earlier: connection pressure requires PgBouncer, query volume causes lock contention, full table scans on the `task_instance` table degrade scheduling performance, and missing indexes have been a recurring problem even in recent releases. The database is the sole communication channel between components — the scheduler and workers don't communicate directly, they communicate by writing to and reading from the database. This means every state transition is a database write, every dependency check is a database read, and every heartbeat from a running task is a database update. At scale, the write volume alone can overwhelm even well-tuned PostgreSQL instances.

The "database as single point of failure" problem is particularly acute because it's not just a failure mode — it's a performance degradation mode. A slow database doesn't crash Airflow, it degrades it: scheduling latency increases, timeouts fire, tasks are falsely marked as zombies, retries compound the database load, and the system enters a death spiral. Running `airflow db clean` to purge old records is a routine maintenance task that many teams automate as a cron job, which is an indicator that the architecture doesn't handle data accumulation gracefully.

### Cold Start / DAG Import Time

DAG cold start refers to the time between deploying a new DAG file and when Airflow begins scheduling it. This is determined by two intervals: `dag_dir_list_interval` (how often Airflow scans the DAG folder for new files) and `min_file_process_interval` (how often Airflow re-parses existing DAGs). In default configurations, a new DAG may not appear in the UI or be scheduled for up to 5 minutes after deployment. For CI/CD-driven workflows where DAGs are deployed as part of a release pipeline, this delay is a friction point — you can't immediately verify that the deployed DAG is correct and being scheduled.

### Backfill Semantics and Complexity

Airflow's backfill model has evolved through multiple iterations and remains a source of confusion. The original model was catchup-based: if a DAG's `start_date` is in the past and `catchup=True`, Airflow creates DAG runs for every missed interval between `start_date` and now, executing them sequentially. In Airflow 2.x, `catchup` defaulted to `True`. In Airflow 3, it defaults to `False`, acknowledging that most users don't want catchup behavior. But the backfill use case (reprocessing historical data with the latest DAG code) is different from catchup (catching up missed scheduled runs), and the same mechanisms serve both poorly. The `airflow dags backfill` CLI command creates DAG runs with historical execution dates but uses the *current* DAG code, which may not match the code that was in place at the historical execution time. Airflow 3's DAG versioning is supposed to address this — backfilling with a specific DAG version — but the implementation has been incremental.

### Testing and Local Development Friction

Testing Airflow DAGs has historically been difficult. The recommended workflow for years was to deploy to a staging environment and manually trigger runs, because running Airflow locally required setting up a full scheduler, database, and workers. The `dag.test()` method (introduced in Airflow 2.5) improved this significantly by allowing DAGs to be executed in a single serialized Python process with no scheduler required. However, `dag.test()` still requires a running Airflow database (even if only SQLite), has limitations with certain operators (particularly those that depend on the scheduler or triggerer), and doesn't replicate the distributed execution environment where most bugs surface.

More fundamentally, the minimum viable Airflow development environment is still heavy. The project's own developer documentation describes `Breeze`, a Docker-based development environment with separate images for different Python and Airflow versions, each taking around 3GB. Breeze requires Docker Desktop with at least 32GB of RAM to run integration tests. For a tool whose primary artifact is a Python script (a DAG), the development loop is disproportionately slow and resource-intensive.

### Executor Abstraction Leaking

The executor abstraction is supposed to be a pluggable "how tasks are run" interface, but in practice, different executors create different behavioral contracts. A DAG that works perfectly with the `LocalExecutor` may fail or behave differently with the `CeleryExecutor` or `KubernetesExecutor` because of differences in environment, serialization requirements, state reporting semantics, and task isolation. The `CeleryExecutor` requires that operator arguments be serializable (picklable), which is not a requirement of the `LocalExecutor`. The `KubernetesExecutor` requires that each task be independently runnable in a fresh container, which breaks assumptions about shared filesystem state that work with the `LocalExecutor`. The community's advice is to test DAGs with the same executor that will be used in production, which defeats the purpose of the abstraction.

### Task Dependency Management and Cross-DAG Dependencies

Airflow's dependency model is purely local to a DAG: `task_a >> task_b`. There is no first-class mechanism for expressing dependencies between DAGs. The workarounds — `ExternalTaskSensor` (which polls for a task in another DAG to complete), `TriggerDagRunOperator` (which triggers another DAG from within a task), and the Asset-based scheduling in Airflow 3 — are all imperfect. `ExternalTaskSensor` is a polling mechanism that consumes a worker slot while waiting. Asset-based scheduling is a significant improvement (trigger a DAG when a named asset is updated) but is still fundamentally polling under the hood — the triggerer polls external systems for asset state changes.

### Time Zone and Scheduling Complexities

Airflow internally stores all times in UTC. The UI displays timestamps in UTC by default. DST transitions, time zone-aware datetime objects, and `pendulum`'s datetime handling have all been sources of confusion and bugs. The `schedule_interval` parameter had inconsistent semantics between cron expressions and timedelta objects. Airflow 3 simplifies this by unifying scheduling under a single `schedule` field and using `CronTriggerTimetable` by default, but the legacy of time zone complexity remains a pain point for teams migrating from older versions.

### Serialization/Deserialization Overhead

DAG serialization (saving DAGs to the `serialized_dag` table) and deserialization (reading them back) is a computationally significant operation at scale. The serialized form must be complete enough to reconstruct the DAG's structure for scheduling decisions, but the serialization code has had bugs where certain operator configurations don't round-trip correctly. Serialization format changes across versions have occasionally required database migrations or even DAG rewrites. The `serialized_dag` table grows unboundedly — in Airflow 3 with versioning, each DAG deployment creates a new serialized version, so teams with frequent deployments accumulate large amounts of serialized DAG data.

### Version Upgrade Pain

Upgrading between major Airflow versions has been consistently cited as one of the worst aspects of operating Airflow. A Hacker News commenter from a company running ~5,000 DAGs and 100,000+ daily task executions described it bluntly: "Upgrades have been an absolute nightmare and so disruptive... We've since tried multiple times to upgrade past the 2.0 release and hit issues every time, so we are just done with it. We'll stay at 2.0 until we eventually move off airflow altogether." The Airflow 2 to 3 migration, while better documented than previous upgrades, still requires running tools like `ruff` with AIR rules to auto-fix imports, and even then, a blog post from a data engineer doing the migration documented CPU spikes to 600%, silently failing tasks, health check failures, and user creation that silently did nothing.

---

## 5. Push vs. Pull Model Analysis

### Where "Pull" Happens in Airflow

Airflow's pull-based nature manifests in three distinct layers:

**Layer 1: Scheduler Polling.** The scheduler continuously loops through all DAGs and task instances, querying the database to find work that needs to be done. In each loop iteration, it checks: Which DAG runs need to be created? Which task instances have all their dependencies met? Which tasks are now ready to be queued? The scheduler initiates nothing in response to external events — it discovers work by scanning state. The latency between when a task becomes runnable and when the scheduler discovers it is a function of the scheduler loop duration.

**Layer 2: Worker Polling.** Celery workers continuously poll their configured queues (RabbitMQ or Redis) for new task messages. The workers don't receive push notifications — they ask the broker "do you have work for me?" on a polling interval. Kubernetes workers follow a different pattern (the scheduler creates pods directly) but the result is similar: the worker infrastructure waits to be told what to do, it doesn't proactively accept work.

**Layer 3: Sensor Polling.** This is the most expensive form of polling. A sensor operator (e.g., `S3KeySensor`, `ExternalTaskSensor`) sits in a worker slot and repeatedly queries an external system — "does this file exist yet?", "has that task finished?" — until the condition is met or the sensor times out. Each poll is a worker resource occupied, a database connection held, and, in the worst case, a request to an external system. Deferrable operators move this polling to the triggerer, which is more efficient (one asyncio event loop can handle thousands of concurrent polls) but is still fundamentally polling — the triggerer asks the external system "are you ready?" on a loop, it doesn't receive a push notification.

Even Airflow 3's event-driven scheduling (AIP-82), despite its name, is poll-based under the hood. The AIP-82 design document explicitly acknowledges this: "Only the poll based event-driven scheduling is considered as part of this AIP. Some investigation has been done on the push based event-driven scheduling without leading to a satisfying solution." The `AssetWatcher` and `BaseEventTrigger` classes poll external message queues (Kafka, SQS) for events — they don't accept push notifications from those systems. The "push" in push-based event-driven scheduling was deferred to a future AIP, which as of mid-2026 has not been implemented.

### Latency Characteristics

In a pull-based model, scheduling latency is bounded below by the polling interval and bounded above by the polling interval plus processing time. If the scheduler loop takes 8 seconds and a task becomes runnable immediately after the loop starts, that task waits 7+ seconds before being discovered. If the database is under load and the loop takes 20 seconds, all scheduling decisions are delayed by up to 20 seconds. This is acceptable for batch workloads with minute-scale SLAs but becomes problematic for near-real-time use cases. The task execution latency compounds: scheduler lag plus worker pickup lag plus container/Pod startup time plus task execution time.

### What a Push Model Would Look Like

A push-based execution model inverts this relationship. Instead of the scheduler polling for state changes, state changes would push notifications to the scheduling system. When a task completes, it notifies the scheduler directly (or an event bus that feeds the scheduler). The scheduler, instead of scanning all task instances to find which ones have newly satisfied dependencies, maintains an in-memory dependency graph and processes a stream of completion events. Each completion triggers a localized dependency check for the tasks immediately downstream of the completed task, not a global scan.

The benefits of a push model for orchestration are:

- **Reduced latency**: Task scheduling is event-driven, not interval-driven. Downstream tasks can be queued within milliseconds of their upstream dependencies completing, not seconds or tens of seconds later.
- **Reduced database load**: The scheduler doesn't execute large scans of the `task_instance` table on every loop iteration. It receives targeted events and performs targeted queries.
- **Better CPU utilization**: The scheduler isn't burning cycles re-evaluating task dependencies that haven't changed. It only does work when work arrives.
- **Natural fit for event-driven architectures**: Push models compose better with event-driven infrastructure (webhooks, message queues, event buses) because the orchestration system is itself event-driven.

The challenges of a push model include:

- **State consistency**: The scheduler must maintain an accurate in-memory representation of the DAG graph, including all running tasks, their states, and their dependencies. If the scheduler restarts, it must reconstruct this state from the database, which requires a startup scan — temporarily regressing to pull behavior.
- **At-least-once delivery**: Push notifications can be lost (network failure, process crash). The system needs a reconciliation mechanism — periodic full-state scans — to detect and recover from missed events, essentially reintroducing a slow pull loop for fault tolerance.
- **Backpressure**: A burst of completions (e.g., a 1,000-way mapped task finishing) can overwhelm the scheduler with events. The system needs flow control.
- **Complexity**: A push-based scheduler is more complex than a polling loop, with event queues, consumer groups, and reconciliation logic.

### Existing Discussions in the Airflow Community

The Airflow community has discussed push-based scheduling in the context of AIP-82 (external event-driven scheduling) and the Common Message Queue proposal. The discussion acknowledges that a push model would improve responsiveness but has not moved forward primarily because the metadata database as central state store is deeply embedded in Airflow's architecture. A push model would require a significant re-architecture — the scheduler would need to consume from an event bus rather than querying database tables, the database would become a backup state store rather than the primary coordination mechanism, and tasks would need to publish completion events rather than simply updating their state in the database.

The architectural conservatism is understandable: Airflow's pull model has scaled to Uber's 450,000 daily pipeline runs, and the community's energy has been focused on Airflow 3's service-oriented decomposition and API-first architecture. Push-based scheduling remains an acknowledged future direction but not an immediate priority.

---

## 6. Relevance to Conductor

### What Airflow Got Right (Good Ideas to Borrow)

**Workflows as Code**: Airflow's DAG-as-Python model is its most important design choice and the primary reason for its dominance. Users can express arbitrary logic in their DAG definitions, use any Python library, and leverage their existing Python tooling. Any orchestration tool that aims to compete with Airflow must support code-defined workflows. The stable Task SDK concept in Airflow 3 — a versioned, forward-compatible API for DAG authoring — is a pattern worth adopting directly: provide a clean, stable SDK that insulates users from internal implementation changes.

**Pluggable Executor Model**: The idea that a single orchestration system can dispatch tasks to different execution environments through a pluggable interface is sound. The abstraction leaks in Airflow's implementation, but the concept is correct. Conductor can improve on this by making the executor interface truly uniform — a WASM runtime is inherently more portable and consistent than a mix of subprocess, Celery, and Kubernetes executors.

**Provider Ecosystem**: Airflow's provider package model decouples connector development from core development. Conductor should consider a similar model from the start, rather than bolting it on after years of monolithic growth. A WASM-based runtime makes this even more natural: connectors can be compiled to WASM modules with well-defined interfaces, independent of the core orchestration engine.

**DAG Versioning**: The ability to inspect historical DAG structures and execute DAG runs against the DAG version that was current when the run started is genuinely valuable for debugging, auditing, and safe backfills. Conductor should implement versioning from day one.

**Deferrable/Async Execution Model**: The idea of releasing execution resources while waiting for external events is necessary for efficient orchestration at scale. Conductor's push-based model can improve on this: instead of deferring and poll-re-scheduling, the system can register interest in an external event and be notified when it occurs, with no intermediary polling.

**Rich Operator Library**: The breadth of Airflow's operator ecosystem (2,000+ provider integrations) is a major switching cost for users. Conductor needs a strategy for this — either an operator compatibility layer, an operator generation tool, or a clear migration path.

### What Airflow Got Wrong (Things to Avoid)

**The Metadata Database as Central Coordination Point**: This is the cardinal architectural sin that cascades into most of Airflow's scaling problems. Using a relational database as the communication bus between components means every state change is a database write, every coordination decision is a database read, and the database's performance limits the entire system's throughput. Conductor should use the database for durable storage (DAG definitions, historical run data, audit logs) but should use a message bus or event stream for real-time coordination between components. The scheduler should react to events, not query tables.

**The Pull-Based Scheduler Loop**: The scheduler shouldn't poll for work. It should be event-driven — task completions publish events, the scheduler consumes them, evaluates dependencies for affected downstream tasks, and dispatches runnable tasks. A periodic reconciliation scan (e.g., every 60 seconds) can serve as a safety net for missed events, but the primary scheduling path should be push-based.

**DAG Parsing as a Continuous Re-Import Problem**: Repeatedly re-executing Python DAG files to extract workflow definitions is a fundamentally wasteful approach. Conductor should adopt a declarative workflow definition format (which could still be generated from Python code) that is parsed once when deployed, versioned, and stored in a structured form that doesn't require code execution to understand. The WASM runtime provides an additional path: DAG definitions could be compiled to WASM modules at deploy time, combining the flexibility of code-defined workflows with the efficiency of compiled artifacts.

**The Executor Abstraction Leaking**: Don't create an abstraction if different implementations aren't truly interchangeable. If some executors support cross-task state sharing and others don't, that difference should be explicit in the API, not a runtime surprise. Conductor's WASM-based execution model naturally provides strong isolation guarantees that are uniform across all execution — there is no "local" versus "remote" executor distinction when every task runs in a WASM sandbox.

**Exploding State Tracking**: Airflow's `task_instance` table grows unboundedly, and the system degrades as it grows. Conductor should design its data model for efficient pruning from the start — partition by time, automatically expire old run data, provide configurable retention policies rather than requiring users to clean up manually.

**Version Upgrade Fragility**: Airflow's upgrade pain is partly a consequence of its monolithic origins — the core, the UI, the API, the DAG format, and the database schema are all tightly coupled and version-locked. Conductor should define explicit, versioned interfaces between components so that components can evolve independently. The SDK that users write DAGs against should have a different version lifecycle than the scheduler or the execution engine.

### Specific Implications for Push-Based Architecture

A push-based orchestration model fundamentally changes the scheduler's role. Instead of being the active component that discovers work, the scheduler becomes a reactive component that processes a stream of events. This has several implications for Conductor's design:

**Event Bus as Infrastructure**: Conductor needs a reliable, persistent event bus (Kafka, NATS, or a similar technology) as a core infrastructure component. This is where task completion events, DAG deployment events, and external trigger events flow. The scheduler consumes from this bus.

**Dependency Graph in Memory**: The scheduler should maintain the DAG dependency graph in memory (with durable backup to the database) so that when a task completion event arrives, it can immediately identify which downstream tasks have become runnable, without querying the database. This is critical for the latency benefits of push-based scheduling.

**Reconciliation Loop**: Even in a push-based model, a slow reconciliation loop (e.g., every 60 seconds) should scan the database for tasks that appear stuck — tasks in a `running` state whose heartbeats have expired, tasks whose completion events may have been lost. This provides fault tolerance without requiring the scheduler to poll at the frequency of Airflow's loop.

**Exactly-Once vs. At-Least-Once Semantics**: A push-based model must decide its delivery guarantees. At-least-once is easier to implement (re-deliver events if no acknowledgment is received) but requires idempotent scheduling (queuing the same task twice is harmless). Exactly-once is harder but prevents duplicate task execution. Conductor should aim for at-least-once delivery with idempotent task queuing as a pragmatic first approach.

### Specific Implications for WASM-Based Runtime

Using WASM as the container runtime for task execution is a significant architectural differentiator with both advantages and challenges:

**Advantages**:

- **Near-instant Startup**: WASM modules can start in microseconds to single-digit milliseconds, compared to 10–60 seconds for Kubernetes pod startup or hundreds of milliseconds for Celery worker subprocess creation. This is transformative for workloads with many short-duration tasks — the kind of workloads where Airflow's KubernetesExecutor is impractical and where even the CeleryExecutor's subprocess overhead adds up.

- **Strong Isolation by Default**: WASM runtimes provide capability-based security. A WASM module has no access to the host system unless explicitly granted (file system, network, environment variables). This is a cleaner security model than container-based isolation, which typically grants ambient authority unless explicitly restricted. In Airflow 3, prohibiting direct database access from tasks was a major effort; in a WASM-based system, it's the default.

- **Cross-Language Execution**: WASM modules can be compiled from Rust, Go, C/C++, Python (via Pyodide or similar), and increasingly many other languages. This enables truly polyglot task execution — a DAG can have tasks in different languages — without requiring each language runtime to be installed on the worker. The WASM runtime is the only runtime needed.

- **Deterministic Resource Limits**: WASM runtimes can enforce strict memory limits, CPU instruction budgets, and execution timeouts at the module level. This provides fine-grained resource control that container runtimes struggle with.

- **Portability**: A WASM module runs identically on any platform with a WASM runtime — local development, CI, cloud, edge. This eliminates the "works on my machine" problem for task execution and simplifies testing.

**Challenges and Design Requirements**:

- **Python Ecosystem Compatibility**: Airflow's dominance is inseparable from Python's dominance. Many of Conductor's target users will have existing Python DAGs and Python operators. WASM-based Python execution (via Pyodide, CPython compiled to WASM, or similar) is maturing but is not yet seamless. Conductor needs a clear story for Python task execution — either a high-quality Python-to-WASM path, or a hybrid model where Python tasks run in traditional containers while WASM-native tasks run in WASM.

- **I/O Model**: WASM modules have restricted I/O capabilities by design. Tasks that need to interact with external systems (databases, APIs, object storage) need well-defined interfaces for those interactions. WASI (WebAssembly System Interface) provides standards for filesystem, networking, and sockets, but the coverage is not yet complete. Conductor will need to provide SDK-level abstractions for common I/O patterns that work within WASM's capability model.

- **State and Caching**: WASM modules are stateless by default (a module is instantiated, runs, and terminates). This is ideal for task isolation but means that any state sharing between tasks (analogous to Airflow's XCom) must be explicit through the orchestration layer, not implicit through shared memory or filesystem. This is architecturally cleaner but requires clear SDK design.

- **Module Distribution**: WASM modules need to be distributed to workers before execution, similar to how container images are pulled. WASM modules are typically much smaller than container images (kilobytes to megabytes vs. hundreds of megabytes to gigabytes), which makes distribution faster and cheaper. However, the distribution mechanism (module registry, caching, versioning) needs to be built or adopted (e.g., from the wasmCloud or Docker WASM ecosystem).

- **Observability**: Debugging a task running in a WASM sandbox is different from debugging a subprocess or container. Log capture, error reporting, stack traces, and profiling all need WASM-aware tooling. The Conductor worker runtime should expose standard observability interfaces (OpenTelemetry, structured logging) that work uniformly whether the task is WASM or traditional.

### Migration Concerns for Airflow Users

For Conductor to attract Airflow users, several migration barriers need to be lowered:

**DAG Conversion**: The largest switching cost is converting existing DAGs. Conductor should provide a conversion tool that can ingest an Airflow DAG Python file and produce an equivalent Conductor workflow definition. 100% fidelity is not realistic (deferrable operators, sensors, and Airflow-specific features will require manual migration), but 80% automated conversion significantly reduces the migration barrier.

**Operator Parity**: Users will ask "does Conductor have an operator for X?" for every X they currently use. A compatibility layer that can wrap existing Airflow operators and execute them in Conductor (perhaps in a traditional container sidecar until native WASM operators are available) would smooth the transition.

**Execution Model Differences**: The push-based execution model and WASM runtime are fundamentally different from Airflow's pull-based scheduler and process/container model. Users will experience different performance characteristics (tasks start faster but may have different I/O patterns), different failure modes (WASM sandbox violations vs. OOM kills), and different operational rhythms. Documentation, runbooks, and migration guides need to make these differences explicit.

**Integration Points**: Airflow integrates deeply with enterprise infrastructure: authentication systems (LDAP, OAuth, SAML), monitoring systems (statsd, Prometheus, Datadog), logging infrastructure (Elasticsearch, CloudWatch), and secret managers (Vault, AWS Secrets Manager). Conductor needs equivalent integration points from launch, or clear guidance on how to bridge the gap.

**Community and Ecosystem**: Airflow's community — the Slack workspace with thousands of members, the Airflow Summit, the countless blog posts and tutorials — is a moat. Conductor should invest in documentation, examples, and community building from day one. Every common Airflow use case should have a documented Conductor equivalent.

---

*This document was compiled from official Apache Airflow documentation, Airflow Improvement Proposals (AIPs), Airflow Summit conference materials, Astronomer documentation, GitHub issues and discussions, Hacker News and Reddit threads, and technical blog posts from the Airflow community. It reflects the state of Airflow as of mid-2026, including Airflow 3.0–3.3.*
