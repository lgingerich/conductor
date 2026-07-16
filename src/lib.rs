//! Conductor — experimental next-generation data orchestration.
//!
//! Core primitives are [`Task`] (runnable), [`Artifact`] (data identity), and
//! [`Pipeline`] (composition). Define them with human-readable names; the
//! compiled [`TaskGraph`] keeps those names as the identity. See
//! `docs/core-primitives.md` for the design.

mod artifact;
mod errors;
mod graph;
mod pipeline;
mod task;

pub use artifact::Artifact;
pub use errors::{GraphError, TransitionError};
pub use graph::{EdgeKind, GraphEdge, TaskGraph};
pub use pipeline::{Pipeline, PipelineName, PipelineRun, PipelineRunId};
pub use task::{Task, TaskName, TaskRun, TaskRunId, TaskState};
