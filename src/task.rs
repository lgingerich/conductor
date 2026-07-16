use std::fmt;
use std::time::Instant;

use crate::artifact::Artifact;

/// Stable human-readable identity for a [`Task`].
///
/// Used at definition time, in errors, and as the identity stored in the
/// compiled [`TaskGraph`](crate::TaskGraph).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskName(String);

impl TaskName {
    /// Creates a task name from a human-readable slug.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Returns this name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for TaskName {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

/// A task-scoped run identifier.
///
/// Construct with [`TaskRunId::new`] or [`TaskRunId::from`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskRunId(String);

impl TaskRunId {
    /// Creates a task run id from the given identifier.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns this id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for TaskRunId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

/// A runnable unit of work.
///
/// Tasks are the sole execution primitive. They may optionally declare
/// artifact ports (`inputs` / `outputs`) for lineage and control
/// dependencies (`after`) for ordering without a data product.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    name: TaskName,
    inputs: Vec<Artifact>,
    outputs: Vec<Artifact>,
    after: Vec<TaskName>,
}

impl Task {
    /// Creates a task with the given human-readable name and no ports or deps.
    #[must_use]
    pub fn new(name: impl Into<TaskName>) -> Self {
        Self {
            name: name.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            after: Vec::new(),
        }
    }

    /// Sets the artifact inputs for this task.
    #[must_use]
    pub fn with_inputs(mut self, artifacts: impl IntoIterator<Item = Artifact>) -> Self {
        self.inputs = artifacts.into_iter().collect();
        self
    }

    /// Sets the artifact outputs for this task.
    #[must_use]
    pub fn with_outputs(mut self, artifacts: impl IntoIterator<Item = Artifact>) -> Self {
        self.outputs = artifacts.into_iter().collect();
        self
    }

    /// Sets control dependencies from other tasks (by name).
    #[must_use]
    pub fn with_after<'a>(mut self, tasks: impl IntoIterator<Item = &'a Task>) -> Self {
        self.after = tasks.into_iter().map(|task| task.name.clone()).collect();
        self
    }

    /// Returns this task's name.
    #[must_use]
    pub fn name(&self) -> &TaskName {
        &self.name
    }

    /// Returns this task's artifact inputs.
    #[must_use]
    pub fn inputs(&self) -> &[Artifact] {
        &self.inputs
    }

    /// Returns this task's artifact outputs.
    #[must_use]
    pub fn outputs(&self) -> &[Artifact] {
        &self.outputs
    }

    /// Returns control-dependency task names.
    #[must_use]
    pub fn after(&self) -> &[TaskName] {
        &self.after
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)
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
    task: TaskName,
    run_id: TaskRunId,
    state: TaskState,
}

impl TaskRun {
    /// Creates a new task run in the [`TaskState::Pending`] state.
    #[must_use]
    pub fn new(task: impl Into<TaskName>, run_id: TaskRunId) -> Self {
        Self {
            task: task.into(),
            run_id,
            state: TaskState::Pending,
        }
    }

    /// Returns the task name for this run.
    #[must_use]
    pub fn task(&self) -> &TaskName {
        &self.task
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
    use super::{Task, TaskName, TaskRun, TaskRunId, TaskState};
    use crate::artifact::Artifact;

    #[test]
    fn task_ports_and_control_deps() {
        let gcs = Artifact::new("gcs/users.parquet");
        let pg = Artifact::new("postgres/app/users");
        let load = Task::new("gcs_to_postgres")
            .with_inputs([gcs])
            .with_outputs([pg]);
        let index = Task::new("create_indexes").with_after([&load]);

        assert_eq!(index.name(), &TaskName::from("create_indexes"));
        assert!(index.outputs().is_empty());
        assert!(index.inputs().is_empty());
        assert_eq!(index.after(), &[TaskName::from("gcs_to_postgres")]);
        assert_eq!(load.outputs().len(), 1);
        assert_eq!(load.inputs().len(), 1);
    }

    #[test]
    fn task_run_starts_pending() {
        let run = TaskRun::new("vacuum", TaskRunId::from("vacuum-test"));
        assert_eq!(run.task(), &TaskName::from("vacuum"));
        assert_eq!(run.run_id().to_string(), "vacuum-test");
        assert_eq!(run.state(), &TaskState::Pending);
    }
}
