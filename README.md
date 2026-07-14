# Conductor

An experimental data orchestration engine exploring what a next-generation
system might look like.

Core primitives (Task, Artifact, Pipeline) are documented in
[`docs/core-primitives.md`](docs/core-primitives.md).

## Try it

Define the sample load pipeline and inspect the current API:

```bash
cargo run --example explore_pipeline
```

## Core Ideas

- **Push-based execution** — the scheduler reacts to events rather than
  polling for work, eliminating scheduling latency.
- **WASM container runtime** — tasks run in WebAssembly sandboxes for
  near-instant startup, strong isolation, and polyglot execution.
- **Single binary** — embedded persistence keeps the operational footprint
  minimal during exploration.
