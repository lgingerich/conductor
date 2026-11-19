# Architecture

Project-specific invariants live here; universal Rust workflow lives in `AGENTS.md`. Design detail: `docs/core-primitives.md` (model and decisions), `docs/automatic-artifact-dependencies.md` (proposed, not built), and the competitor notes in `docs/` (`airflow.md`, `dagster.md`, `prefect.md`, `temporal.md`).

## Purpose
An experimental, library-only data orchestration core. It models work as `Task`s (the only runnable nodes), `Artifact`s (named identities of durable data products), and `Pipeline`s (named sets of tasks), compiles a pipeline into a validated dependency DAG (`TaskGraph`), and tracks run state (`PipelineRun`, `TaskRun`, `ReadyTracker`). It does not execute tasks, schedule, persist, or talk to a network: the committed code is the data model and state machines only (the scheduler, WASM runtime and persistence in `README.md` are ideas, not code). `examples/explore_pipeline.rs` shows the API.

## Module Boundaries
All modules are private in `src/lib.rs`; the public API is the re-exports there.
- `src/artifact.rs`: `Artifact`, identity by slug only. Depends on nothing.
- `src/errors.rs`: `GraphError`, `ReadyError`, `TransitionError` (`thiserror`). Depends on `graph` and `task` only for the types named in messages.
- `src/task.rs`: `Task` (declared `inputs`, `outputs`, `after`), `TaskState`, `TaskRun`. Depends on `artifact`, `errors`.
- `src/graph.rs`: `TaskGraph` compilation (`from_pipeline`), `EdgeKind`, `GraphEdge`, `ReadyTracker`. Depends on `artifact`, `errors`, `pipeline`, `task`.
- `src/pipeline.rs`: `Pipeline`, `PipelineRun`, name/id newtypes. Depends on `errors`, `graph`, `task`. `Pipeline::plan` is the only public way to build a `TaskGraph`.

## Core Invariants
- Tasks are the only graph nodes. Artifacts are ports on tasks, never nodes, and are not runnable.
- Identity is the human-readable name (`TaskName`, `PipelineName`, `Artifact` slug); the graph indexes by name and keeps a name-to-index map.
- Edges are either `EdgeKind::Data` (producer to consumer of a shared artifact) or `EdgeKind::Control` (explicit `after`). `plan()` rejects duplicate task names, missing `after` targets, duplicate edges, and cycles with a `GraphError`.
- A `TaskGraph` is a pure function of its `Pipeline`, pipeline-local, and immutable once built. A `PipelineRun` is created only from a validated graph, shared via `Arc`, and its task run ids are `{run_id}/{task_name}`.
- `TaskState` transitions go through `TaskRun::start/complete/fail/skip`; illegal transitions return `TransitionError`, never panic. `Skipped` is terminal and means not attempted (upstream failed), distinct from `Failed`.
- The caller owns the clock: transition methods take the instant as a parameter, and timing lives on `TaskRun`, not in `TaskState`.

## Forbidden Patterns / Dependencies
- No `unsafe` (`unsafe_code = "deny"`); no `unwrap`/`expect` outside tests (clippy).
- Do not make `Artifact` a runnable or graph node, or add execution, I/O, or clock reads to the model types.
- `jiff` and `thiserror` are the only runtime dependencies (`ascii-dag` is dev-only, for the example); do not add one without asking.
