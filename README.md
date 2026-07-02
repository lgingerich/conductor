# Conductor

An experimental data orchestration engine exploring what a next-generation
system might look like.

## Core Ideas

- **Push-based execution** — the scheduler reacts to events rather than
  polling for work, eliminating scheduling latency.
- **WASM container runtime** — tasks run in WebAssembly sandboxes for
  near-instant startup, strong isolation, and polyglot execution.
- **Single binary** — embedded persistence keeps the operational footprint
  minimal during exploration.
