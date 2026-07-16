# Automatic Artifact Dependency Discovery

Design direction for making Conductor's artifact lineage automatic while
preserving the core model in [`core-primitives.md`](core-primitives.md):

- **Tasks are the only runnable nodes.**
- **Artifacts are global identities for durable data products.**
- **Pipelines package and schedule Tasks.**
- **Effects describe non-data mutation without creating fake Artifacts.**

Status: proposed design. The current Rust API still uses explicit
`Task::with_inputs` and `Task::with_outputs`; those methods are the low-level
intermediate representation and the manual escape hatch described here.

## North star

A user should normally:

1. Define the Artifacts their code reads, writes, or observes.
2. Write Tasks using Conductor's artifact I/O API.
3. Put those Tasks in Pipelines.

Conductor should discover Task inputs and outputs, build data edges, populate
global lineage, and enforce the resulting contract. Users should not maintain
a second list of `inputs`, `outputs`, or artifact dependencies that can drift
away from the code that performs the I/O.

```python
raw_users = Artifact.external(
    "users/raw",
    location=BigQuery("vendor", "users"),
)
clean_users = Artifact(
    "users/clean",
    location=Iceberg("lake.analytics.clean_users"),
)
users_export = Artifact(
    "users/export",
    location=GCS("analytics-exports", "users/"),
)

@task
def clean(ctx):
    source = ctx.read(raw_users)
    sink = ctx.write(clean_users)
    return ctx.component("jobs.clean_users", source=source, sink=sink)

@task
def export(ctx):
    source = ctx.read(clean_users)
    sink = ctx.write(users_export)
    return ctx.component("jobs.export_users", source=source, sink=sink)

pipeline = Pipeline("users", tasks=[clean, export])
plan = pipeline.plan()
```

The author never repeats:

```python
clean.inputs = [raw_users]
clean.outputs = [clean_users]
export.inputs = [clean_users]
export.outputs = [users_export]
```

`plan()` derives those ports from `ctx.read` and `ctx.write`, then lowers them
to the same Task graph Conductor already builds.

## Refined decision

Automatic discovery is a good default, but it cannot mean "guess what
arbitrary code might access."

The design is:

> Artifact identities are declared explicitly. Task ports are derived from
> artifact access through a mediated planning API, merged with explicit
> annotations when necessary, persisted as a Task access manifest, compiled
> into a Task graph, and enforced during execution.

This distinction matters:

- **Good:** `ctx.read(users)` is the source of truth for an input.
- **Good:** `ctx.write(report)` is the source of truth for an output.
- **Good:** the planner records those calls instead of asking for a duplicate
  dependency list.
- **Not credible:** inspecting arbitrary Python, SQL strings, shell scripts,
  or raw cloud SDK calls and always discovering every possible dependency.

Automatic dependency discovery therefore depends on mediated I/O. A Task that
bypasses Conductor with `boto3`, a raw BigQuery client, or an arbitrary network
call cannot receive the same lineage guarantee.

## Why this is worth doing

### One source of truth

The operation that accesses data also defines the dependency. Code and
lineage cannot silently diverge because someone forgot to update a decorator
or DAG definition.

### Better authoring

Fan-out and fan-in emerge from normal artifact usage:

```python
master = Artifact.external("video/master", location=GCS("raw", "ep42.mov"))
mezzanine = Artifact("video/mezzanine", location=GCS("work", "ep42.mov"))
video_1080 = Artifact("video/1080p", location=GCS("renditions", "1080.mp4"))
video_720 = Artifact("video/720p", location=GCS("renditions", "720.mp4"))
audio = Artifact("video/audio", location=GCS("renditions", "audio.m4a"))
package = Artifact("video/hls", location=GCS("packages", "ep42/hls/"))

@task
def validate(ctx):
    source = ctx.read(master)
    sink = ctx.write(mezzanine)
    return ctx.component("video.validate", source=source, sink=sink)

@task
def encode_1080(ctx):
    source = ctx.read(mezzanine)
    sink = ctx.write(video_1080)
    return ctx.component("video.encode", profile="1080p", source=source, sink=sink)

@task
def encode_720(ctx):
    source = ctx.read(mezzanine)
    sink = ctx.write(video_720)
    return ctx.component("video.encode", profile="720p", source=source, sink=sink)

@task
def extract_audio(ctx):
    source = ctx.read(mezzanine)
    sink = ctx.write(audio)
    return ctx.component("video.audio", source=source, sink=sink)

@task
def package_hls(ctx):
    inputs = [
        ctx.read(video_1080),
        ctx.read(video_720),
        ctx.read(audio),
    ]
    sink = ctx.write(package)
    return ctx.component("video.package_hls", inputs=inputs, sink=sink)
```

The three encoders automatically fan out from `mezzanine`; `package_hls`
automatically waits for all three outputs.

### Better enforcement boundary

The same mediated API can provide:

- lineage capture,
- least-privilege credentials,
- storage adapters,
- audit events,
- usage metering,
- runtime validation, and
- future WASM host capabilities.

Automatic lineage and sandbox security reinforce each other: a WASM Task
cannot access a store that the host did not expose.

### Task-centric execution remains intact

Discovery fills Task ports. It does not make Artifacts runnable and does not
create a second execution graph:

```text
Task body + PlanContext
        |
        v
TaskAccessManifest { reads, writes, effects }
        |
        v
Task { inputs, outputs, after }       <- low-level IR
        |
        v
TaskGraph                            <- the runnable graph
```

The global Artifact catalog is a derived producer/consumer index. It is used
for lineage, freshness, and cascading recompute, but execution still resolves
to Tasks.

## Artifact identity and storage

An Artifact is a logical, globally addressable data product. Storage type and
physical address are metadata, not different primitives:

```python
Artifact("events/raw", location=GCS("raw-events", "dt={date}/"))
Artifact("events/clean", location=BigQuery("analytics", "clean_events"))
Artifact("orders", location=Iceberg("lake.marts.orders"))
Artifact("users", location=Postgres("app", "users"))
Artifact("churn/model", location=ModelRegistry("churn", channel="production"))
```

The logical key should remain stable when a bucket, project, schema, or
physical layout changes. Connectors use `location`; lineage uses the Artifact
identity.

Artifact granularity should match the unit users can reason about, observe, and
rematerialize. A whole GCS bucket is an Artifact only when the bucket itself is
the product; normally a dataset prefix or logical collection is the better
identity. A BigQuery or Iceberg table is usually one logical Artifact, while
partitions and snapshots are materialization slices discussed below.

An Artifact's storage technology does not determine whether it is external.
A BigQuery table can be produced by Conductor or owned by another system.

### Managed, external, and unresolved

Workspace compilation classifies an Artifact:

- **Managed:** at least one registered Task writes it.
- **External:** explicitly declared as externally owned and no Conductor Task
  is expected to produce it.
- **Unresolved:** read by a Task, but neither written in the workspace nor
  declared external.

Unresolved Artifacts should be errors, not silently treated as roots. This
catches misspelled keys and missing pipeline registration. External Artifacts
are intentional roots whose freshness comes from observations or events, not
from a producer Task.

```python
vendor_feed = Artifact.external(
    "vendor/orders",
    location=GCS("partner-drop", "orders/"),
)
```

## Planning model

Planning has two related scopes.

### Pipeline plan

`Pipeline.plan()` discovers or loads each Task's access manifest, builds the
pipeline-local Task DAG, validates it, and determines ready/topological order.
An input produced outside the Pipeline is a boundary, not an in-pipeline edge.

### Workspace compile

A workspace/catalog pass combines registered Pipelines:

```text
Artifact -> producer Tasks
Artifact -> consumer Tasks
Task     -> Pipeline
```

This is what connects teams and Pipelines through Artifact identities. It
should not merge every Task into one giant runnable DAG. Materialization
events and rematerialization requests resolve through the catalog to the
relevant Task and Pipeline runs.

## The mediated plan pass

`plan()` must not run real data work. It invokes a Task with a restricted
`PlanContext`:

- `read(artifact)` records an input and returns a symbolic source handle.
- `write(artifact)` records an output and returns a symbolic sink handle.
- `effect(artifact, kind)` records a non-materializing mutation.
- execution components are described or bound, but not started;
- real credentials, network access, and writes are unavailable.

The result is persisted:

```python
TaskAccessManifest(
    task="clean",
    reads={raw_users},
    writes={clean_users},
    effects=set(),
    plan_key=PlanKey(
        code_digest="...",
        config_digest="...",
        partition="2026-07-15",
    ),
)
```

The plan key matters because accesses may depend on configuration, environment,
or partition. Changing a value that can affect artifact selection invalidates
the manifest and requires replanning.

### Do not dry-run arbitrary computation

Returning fake rows from `ctx.read()` and executing an entire Python function
would be fragile: transformations may inspect values, branch on data, call
libraries, or perform side effects. The preferred API separates artifact
binding from actual component execution, as in the examples above.

Future WASM components should expose the same model through host imports and
component metadata. Opaque native processes can use the explicit escape hatch.

## Runtime enforcement

Discovery is only trustworthy if execution obeys the plan.

At runtime, every artifact read and write is checked against the persisted
manifest:

- an unplanned access fails before credentials or handles are granted;
- a missing required write makes the Task incomplete or failed;
- a write emits an Artifact materialization event;
- an external read may emit an observation event;
- effects emit effect/run events, not materializations.

This converts the plan from best-effort documentation into an enforceable
capability contract.

## Dynamic and conditional access

This is the main limitation of automatic discovery:

```python
@task
def report(ctx, region):
    source = ctx.read(eu_users if region == "eu" else us_users)
    sink = ctx.write(report_output)
    return ctx.component("reports.build", source=source, sink=sink)
```

This is safe when `region` is part of `PlanContext`; each plan has a concrete
manifest.

Data-dependent selection is different:

```python
flags = ctx.read(feature_flags)
if flags.use_history:          # depends on runtime data
    history = ctx.read(history)
```

A planning pass cannot know the branch without reading real data. Conductor
should not guess. Strict planning should report unresolved dynamic access and
require one of:

1. restructure the Task so artifact selection is based on plan parameters;
2. split the branches into separate Tasks/Pipelines; or
3. annotate the set of possible accesses.

The resulting graph may intentionally over-approximate possible dependencies,
but it must not omit a possible dependency.

## Manual escape hatch

Explicit annotations remain first-class for dynamic branches, opaque
executables, legacy code, SQL engines that bypass artifact-bound APIs, and
advanced partition mappings.

The default mode merges observed and annotated access:

```python
@task(
    possible_reads=[current_users, historical_users],
    writes=[report],
)
def report(ctx):
    ...
```

For an entirely opaque Task:

```python
@task(
    discovery="explicit",
    reads=[raw_events],
    writes=[clean_events],
)
def legacy_spark_job(ctx):
    return ctx.process(["spark-submit", "legacy.py"])
```

Recommended merge rules:

1. Observed accesses and annotations form a union.
2. A `possible_reads` annotation adds a conservative upstream dependency; it
   does not require that every read occur in every run.
3. Declared and unconditionally observed writes are required outputs.
4. Optional or mutually exclusive outputs are deferred initially; split them
   into separate Tasks rather than weakening materialization semantics.
5. An annotation never hides an observed access.
6. A runtime access must be in the merged manifest.
7. Contradictory direction or storage metadata is a planning error.
8. `discovery="explicit"` is visible in the catalog so consumers know lineage
   is author-asserted rather than mediated.

The current `with_inputs` and `with_outputs` methods can remain the low-level
Rust equivalent of explicit mode.

## Effects and control ordering

Vacuum, index creation, CDN purge, notifications, and API calls should not be
modeled as Artifact outputs.

```python
@task
def vacuum_users(ctx):
    target = ctx.effect(users, kind="postgres.vacuum")
    return ctx.component("postgres.vacuum", target=target)
```

An effect targeted at an Artifact can automatically depend on that Artifact's
producer without claiming to materialize a new data product. Effects can form
same-Pipeline control tails used during cascading recompute.

Ordering unrelated to an Artifact remains explicit:

```python
@task(after=[send_invoice])
def notify_customer(ctx):
    ...
```

Data plus control declarations for the same Task pair should be rejected as
redundant, as the current graph planner already does.

## Multiple writers

Automatic discovery makes accidental multiple writers easier to expose. The
default workspace policy should reject more than one producer for the same
logical Artifact.

Explicit policies can later support:

- partitioned writers whose partitions do not overlap;
- append-only writers;
- a canonical producer plus backfill/migration producers; or
- versioned outputs with distinct Artifact identities.

Until those semantics exist, connecting every writer to every consumer is too
ambiguous for reliable rematerialization.

## Partitions, snapshots, and streams

Automatic discovery identifies logical Artifacts. It cannot infer all physical
slice semantics.

Recommended initial policy:

- unpartitioned Artifacts need no mapping;
- matching partition keys use a default one-to-one mapping;
- non-trivial partition mappings are explicit;
- Iceberg snapshot IDs and model versions are materialization metadata;
- stream offsets/windows require a separate stream execution model and are
  out of scope for the first implementation.

The planner must not claim partition-level cascade until partition identity
and mapping are represented explicitly.

## Honest guarantees

Conductor can guarantee:

1. All lineage-relevant accesses made through the mediated API are visible.
2. For a fixed PlanContext, the manifest contains all observed and annotated
   possible accesses.
3. Runtime access cannot exceed the persisted manifest in strict mode.
4. Effects never create Artifact materializations.
5. Workspace compilation can connect registered producers and consumers by
   global Artifact identity.

Conductor cannot guarantee:

1. complete inference from arbitrary Python, shell, SQL text, or cloud SDKs;
2. that one plan covers every possible runtime configuration;
3. external freshness from planning alone;
4. partition mappings that were never modeled;
5. an authoritative producer when multiple-writer policy is absent; or
6. cross-Pipeline connectivity when a Pipeline is not registered in the same
   workspace/catalog.

## Relationship to Dagster and Airflow

Dagster avoids explicit task wiring by making asset dependencies primary, but
upstream assets are still declared through function arguments, `deps`, or
asset specs. Airflow generally requires task ordering and manually declared
asset/dataset outlets. Neither reliably infers arbitrary storage access from
task bodies.

Conductor's intended distinction is:

- Dagster-like global data identity and lineage;
- Airflow-like first-class non-data Tasks;
- no duplicate input/output lists for mediated Tasks;
- Task-only execution after discovery lowers accesses to ports; and
- future WASM capabilities enforce the same contract used for lineage.

## Implementation sequence

### 1. Access manifest and merge rules

Introduce internal read/write/effect access records and compile them into the
existing Task port representation. Keep explicit ports working.

### 2. Restricted PlanContext

Add symbolic `read`, `write`, and `effect` bindings. Planning produces no real
I/O and persists a manifest keyed by code/config/partition inputs.

### 3. Runtime enforcement

Use the manifest to grant capabilities and validate actual reads, writes, and
materializations.

### 4. Artifact ownership and workspace catalog

Distinguish managed, external, and unresolved Artifacts; reject ambiguous
writers; build global producer/consumer indexes across registered Pipelines.

### 5. Materialization and observation events

Drive freshness, cross-Pipeline triggers, and cascading recompute from Artifact
events rather than Pipeline completion.

### 6. WASM integration

Expose the mediated API as host capabilities and derive manifests from
component bindings. Opaque/native Tasks continue to use explicit mode.

## Summary

The goal is not zero declarations. Users still declare stable Artifact
identities and write Tasks that intentionally read, write, or affect them. The
goal is **zero duplicate dependency declarations in the normal path**.

Task bodies express artifact usage once. Planning discovers and persists that
usage. The existing graph compiler turns it into runnable ordering. Runtime
enforces it. Explicit annotations remain available where automatic discovery
cannot be complete.
