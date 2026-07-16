//! Shared error types for Conductor.

use thiserror::Error;

use crate::graph::EdgeKind;
use crate::task::{TaskName, TaskState};

/// Errors from compiling a [`Pipeline`](crate::Pipeline) into a [`TaskGraph`](crate::TaskGraph).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GraphError {
    /// Two tasks in the pipeline share the same name.
    #[error("duplicate task name `{name}` in pipeline")]
    DuplicateTask {
        /// The duplicated task name.
        name: TaskName,
    },
    /// An `after` dependency names a task not in the pipeline.
    #[error("task `{task}` declares after=`{missing}`, but that task is not in the pipeline")]
    UnknownAfter {
        /// Task that declared the missing dependency.
        task: TaskName,
        /// Name that could not be resolved.
        missing: TaskName,
    },
    /// An edge conflicts with an existing edge between the same tasks.
    #[error("duplicate edge {from} -> {to} [{kind}]")]
    DuplicateEdge {
        /// Upstream task name.
        from: TaskName,
        /// Downstream task name.
        to: TaskName,
        /// Kind of the edge that could not be inserted.
        kind: EdgeKind,
    },
    /// The dependency graph contains a cycle.
    #[error("cycle in task graph: {}", format_cycle_path(.path))]
    Cycle {
        /// Task names participating in (or left in) the cycle.
        path: Vec<TaskName>,
    },
}

fn format_cycle_path(path: &[TaskName]) -> String {
    let mut out = String::new();
    for (i, name) in path.iter().enumerate() {
        if i > 0 {
            out.push_str(" -> ");
        }
        out.push_str(name.as_str());
    }
    out
}

/// Errors from driving a [`TaskRun`](crate::TaskRun) through its state machine.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum TransitionError {
    /// The task was not in a state that allows the attempted transition.
    #[error("illegal transition: cannot {attempted} a task in the {from} state")]
    IllegalTransition {
        /// The state the task was in when the transition was attempted.
        from: TaskState,
        /// The transition that was attempted (`"start"`, `"complete"`, etc.).
        attempted: &'static str,
    },
}
