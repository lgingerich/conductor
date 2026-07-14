//! Conductor — experimental next-generation data orchestration.
//!
//! Core primitives are [`Task`] (runnable), [`Artifact`] (data identity), and
//! [`Pipeline`] (composition). Define them with human-readable names; dense
//! id interning stays internal for planners/runners. See
//! `docs/core-primitives.md` for the design.

mod artifact;
mod intern;
mod pipeline;
mod task;

pub use artifact::Artifact;
pub use pipeline::{Pipeline, PipelineRun, PipelineRunId};
pub use task::{Task, TaskRun, TaskRunId, TaskState};

/// Returns the crate name embedded by Cargo.
#[must_use]
pub fn crate_name() -> &'static str {
    env!("CARGO_PKG_NAME")
}

#[cfg(test)]
mod tests {
    use super::crate_name;

    #[test]
    fn crate_name_is_available() {
        assert_eq!(crate_name(), "conductor");
    }
}
