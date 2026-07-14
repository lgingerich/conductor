use std::fmt;
use std::time::Instant;

use crate::intern::{ArtifactId, TaskId};

/// A task-scoped run identifier.
///
/// Construct with [`TaskRunId::from`] — callers always supply the run name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskRunId(String);

impl fmt::Display for TaskRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for TaskRunId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

/// A runnable unit of work.
///
/// Tasks are the sole execution primitive. They may optionally declare
/// artifact ports (`inputs` / `outputs`) for lineage and control
/// dependencies (`after`) for ordering without a data product.
///
/// Ports reference [`ArtifactId`] (and [`TaskId`] for `after`). Resolve slugs
/// through an [`crate::Interner`]; the [`crate::Artifact`] type is the catalog
/// record for a data product.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    id: TaskId,
    inputs: Vec<ArtifactId>,
    outputs: Vec<ArtifactId>,
    after: Vec<TaskId>,
}

impl Task {
    /// Creates a task with the given id and no ports or control deps.
    #[must_use]
    pub fn new(id: TaskId) -> Self {
        Self {
            id,
            inputs: Vec::new(),
            outputs: Vec::new(),
            after: Vec::new(),
        }
    }

    /// Sets the artifact inputs for this task.
    #[must_use]
    pub fn with_inputs(mut self, artifacts: Vec<ArtifactId>) -> Self {
        self.inputs = artifacts;
        self
    }

    /// Sets the artifact outputs for this task.
    #[must_use]
    pub fn with_outputs(mut self, artifacts: Vec<ArtifactId>) -> Self {
        self.outputs = artifacts;
        self
    }

    /// Sets control dependencies: tasks that must complete before this one.
    #[must_use]
    pub fn with_after(mut self, tasks: Vec<TaskId>) -> Self {
        self.after = tasks;
        self
    }

    /// Returns this task's id.
    #[must_use]
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Returns this task's artifact inputs.
    #[must_use]
    pub fn inputs(&self) -> &[ArtifactId] {
        &self.inputs
    }

    /// Returns this task's artifact outputs.
    #[must_use]
    pub fn outputs(&self) -> &[ArtifactId] {
        &self.outputs
    }

    /// Returns control-dependency task ids.
    #[must_use]
    pub fn after(&self) -> &[TaskId] {
        &self.after
    }
}

/// The execution state of a task within a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    /// The task has not yet started.
    Pending,
    /// The task is currently running, started at the given instant.
    Running {
        /// Instant when the task entered the running state.
        at: Instant,
    },
    /// The task completed successfully at the given instant.
    Completed {
        /// Instant when the task completed.
        at: Instant,
    },
    /// The task failed at the given instant with an error message.
    Failed {
        /// Instant when the task failed.
        at: Instant,
        /// Error message describing the failure.
        error: String,
    },
}

/// A record of one task's execution within a specific run.
#[derive(Debug, Clone)]
pub struct TaskRun {
    task: TaskId,
    run_id: TaskRunId,
    state: TaskState,
}

impl TaskRun {
    /// Creates a new task run in the [`TaskState::Pending`] state.
    #[must_use]
    pub fn new(task: TaskId, run_id: TaskRunId) -> Self {
        Self {
            task,
            run_id,
            state: TaskState::Pending,
        }
    }

    /// Returns the task id for this run.
    #[must_use]
    pub fn task(&self) -> TaskId {
        self.task
    }

    /// Returns this run's identifier.
    #[must_use]
    pub fn run_id(&self) -> &TaskRunId {
        &self.run_id
    }

    /// Returns the current execution state.
    #[must_use]
    pub fn state(&self) -> &TaskState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::{Task, TaskRun, TaskRunId, TaskState};
    use crate::Interner;

    #[test]
    fn task_ports_and_control_deps() {
        let mut names = Interner::new();
        let gcs = names.artifact("gcs/users.parquet");
        let pg = names.artifact("postgres/app/users");
        let load = names.task("gcs_to_postgres");
        let index = names.task("create_indexes");

        let task = Task::new(index)
            .with_inputs(vec![pg])
            .with_after(vec![load]);

        assert_eq!(task.id(), index);
        assert_eq!(names.task_name(task.id()), Some("create_indexes"));
        assert!(task.outputs().is_empty());
        assert_eq!(task.inputs(), &[pg]);
        assert_eq!(task.after(), &[load]);

        let load_task = Task::new(load)
            .with_inputs(vec![gcs])
            .with_outputs(vec![pg]);
        assert_eq!(load_task.outputs().len(), 1);
        assert_eq!(load_task.inputs().len(), 1);
    }

    #[test]
    fn task_run_starts_pending() {
        let mut names = Interner::new();
        let vacuum = names.task("vacuum");
        let run = TaskRun::new(vacuum, TaskRunId::from("vacuum-test"));
        assert_eq!(run.task(), vacuum);
        assert_eq!(run.run_id().to_string(), "vacuum-test");
        assert_eq!(run.state(), &TaskState::Pending);
    }
}
