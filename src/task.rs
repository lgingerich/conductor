use std::fmt;
use std::time::Instant;

use crate::artifact::Artifact;
use crate::errors::TransitionError;

/// Stable human-readable identity for a [`Task`].
///
/// Used at definition time, in errors, and as the identity stored in the
/// compiled [`TaskGraph`](crate::TaskGraph).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskName(String);

impl TaskName {
    /// Creates a task name from a human-readable slug.
    ///
    /// # Examples
    ///
    /// ```
    /// use conductor::TaskName;
    ///
    /// let name = TaskName::new("load_users");
    /// assert_eq!(name.as_str(), "load_users");
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use conductor::TaskRunId;
    ///
    /// let id = TaskRunId::new("load-1");
    /// assert_eq!(id.as_str(), "load-1");
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use conductor::Task;
    ///
    /// let task = Task::new("load_users");
    /// assert_eq!(task.name().as_str(), "load_users");
    /// assert!(task.inputs().is_empty());
    /// assert!(task.outputs().is_empty());
    /// assert!(task.after().is_empty());
    /// ```
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

/// The lifecycle state of a task within a run.
///
/// Pure state — no timing data. When each transition happened is recorded as
/// `started_at` / `finished_at` on [`TaskRun`], not inside this enum. See
/// `docs/core-primitives.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    /// The task has not yet started.
    Pending,
    /// The task is currently running.
    Running,
    /// The task completed successfully.
    Completed,
    /// The task failed with an error message.
    Failed {
        /// Error message describing the failure.
        error: String,
    },
    /// The task was not run because an upstream task failed (cascade-skip).
    ///
    /// Terminal, like [`Completed`](Self::Completed) /
    /// [`Failed`](Self::Failed), but distinct: the task was deliberately not
    /// attempted, not run-and-broken.
    Skipped,
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Running => f.write_str("running"),
            Self::Completed => f.write_str("completed"),
            Self::Failed { .. } => f.write_str("failed"),
            Self::Skipped => f.write_str("skipped"),
        }
    }
}

/// A record of one task's execution within a specific run.
///
/// Owns the task's [`TaskState`] (the lifecycle) and the timing of when it
/// started/finished. Transitions are driven by [`start`](Self::start),
/// [`complete`](Self::complete), [`fail`](Self::fail), and
/// [`skip`](Self::skip); each takes the transition instant as a parameter (the
/// caller owns the clock) and returns `Result` so illegal transitions are
/// typed errors, not panics.
#[derive(Debug, Clone)]
pub struct TaskRun {
    task: TaskName,
    run_id: TaskRunId,
    state: TaskState,
    /// When the task entered the running state (`None` until `start`).
    started_at: Option<Instant>,
    /// When the task reached a terminal state (`None` until complete/fail/skip).
    finished_at: Option<Instant>,
}

impl TaskRun {
    /// Creates a new task run in the [`TaskState::Pending`] state.
    ///
    /// # Examples
    ///
    /// ```
    /// use conductor::{TaskRun, TaskRunId, TaskState};
    ///
    /// let run = TaskRun::new("load_users", TaskRunId::new("load-1"));
    /// assert_eq!(run.task().as_str(), "load_users");
    /// assert_eq!(run.run_id().as_str(), "load-1");
    /// assert_eq!(run.state(), &TaskState::Pending);
    /// assert!(run.started_at().is_none());
    /// assert!(run.finished_at().is_none());
    /// ```
    #[must_use]
    pub fn new(task: impl Into<TaskName>, run_id: TaskRunId) -> Self {
        Self {
            task: task.into(),
            run_id,
            state: TaskState::Pending,
            started_at: None,
            finished_at: None,
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

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> &TaskState {
        &self.state
    }

    /// Returns when the task entered the running state, if it has started.
    #[must_use]
    pub fn started_at(&self) -> Option<Instant> {
        self.started_at
    }

    /// Returns when the task reached a terminal state, if it has finished.
    #[must_use]
    pub fn finished_at(&self) -> Option<Instant> {
        self.finished_at
    }

    /// Transitions `Pending → Running`, recording `at` as the start time.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] if the task is not `Pending`.
    pub fn start(&mut self, at: Instant) -> Result<(), TransitionError> {
        self.apply(Transition::Start, at)
    }

    /// Transitions `Running → Completed`, recording `at` as the finish time.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] if the task is not `Running`.
    pub fn complete(&mut self, at: Instant) -> Result<(), TransitionError> {
        self.apply(Transition::Complete, at)
    }

    /// Transitions `Running → Failed`, recording `at` as the finish time.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] if the task is not `Running`.
    pub fn fail(&mut self, at: Instant, error: String) -> Result<(), TransitionError> {
        self.apply(Transition::Fail { error }, at)
    }

    /// Transitions `Pending → Skipped` (cascade-skip), recording `at` as the
    /// finish time. The task never started, so `started_at` stays `None`.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] if the task is not `Pending`.
    pub fn skip(&mut self, at: Instant) -> Result<(), TransitionError> {
        self.apply(Transition::Skip, at)
    }

    /// Single source of truth for the legal-transition table.
    ///
    /// Returns the state the task would move to, or an error if the transition
    /// is illegal from the current state. Does not mutate.
    fn next_state(&self, transition: Transition) -> Result<TaskState, TransitionError> {
        match (&self.state, transition) {
            (TaskState::Pending, Transition::Start) => Ok(TaskState::Running),
            (TaskState::Running, Transition::Complete) => Ok(TaskState::Completed),
            (TaskState::Running, Transition::Fail { error }) => Ok(TaskState::Failed { error }),
            (TaskState::Pending, Transition::Skip) => Ok(TaskState::Skipped),
            (from, t) => Err(TransitionError::IllegalTransition {
                from: from.clone(),
                attempted: t.label(),
            }),
        }
    }

    /// Applies a transition: validates it, updates state, and records timing.
    fn apply(&mut self, transition: Transition, at: Instant) -> Result<(), TransitionError> {
        // Start sets started_at; every other transition sets finished_at. Capture
        // before `transition` is moved into `next_state`.
        let starts = matches!(transition, Transition::Start);
        self.state = self.next_state(transition)?;
        if starts {
            self.started_at = Some(at);
        } else {
            self.finished_at = Some(at);
        }
        Ok(())
    }
}

/// A state-machine transition the caller wants to perform.
///
/// Internal to [`TaskRun`]'s state machine; callers use the named methods
/// ([`TaskRun::start`] etc.) rather than this enum directly.
enum Transition {
    Start,
    Complete,
    Fail { error: String },
    Skip,
}

impl Transition {
    /// Returns the label used in [`TransitionError`] messages.
    fn label(&self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Complete => "complete",
            Self::Fail { .. } => "fail",
            Self::Skip => "skip",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::{Task, TaskName, TaskRun, TaskRunId, TaskState};
    use crate::artifact::Artifact;
    use crate::errors::TransitionError;

    fn transition_ok(r: Result<(), TransitionError>, msg: &str) {
        if let Err(err) = r {
            panic!("expected {msg}: {err}");
        }
    }

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
        assert_eq!(run.run_id().as_str(), "vacuum-test");
        assert_eq!(run.state(), &TaskState::Pending);
        assert!(run.started_at().is_none());
        assert!(run.finished_at().is_none());
    }

    #[test]
    fn pending_to_running_to_completed() {
        let mut run = TaskRun::new("load", TaskRunId::from("load-1"));
        let started = Instant::now();
        transition_ok(run.start(started), "Pending -> Running is legal");
        assert_eq!(run.state(), &TaskState::Running);
        assert_eq!(run.started_at(), Some(started));
        assert!(run.finished_at().is_none());

        let finished = Instant::now();
        transition_ok(run.complete(finished), "Running -> Completed is legal");
        assert_eq!(run.state(), &TaskState::Completed);
        assert_eq!(run.started_at(), Some(started));
        assert_eq!(run.finished_at(), Some(finished));
    }

    #[test]
    fn pending_to_running_to_failed() {
        let mut run = TaskRun::new("load", TaskRunId::from("load-1"));
        transition_ok(run.start(Instant::now()), "start");
        let finished = Instant::now();
        transition_ok(
            run.fail(finished, "boom".to_owned()),
            "Running -> Failed is legal",
        );
        assert_eq!(
            run.state(),
            &TaskState::Failed {
                error: "boom".to_owned()
            }
        );
        assert_eq!(run.finished_at(), Some(finished));
    }

    #[test]
    fn pending_to_skipped() {
        let mut run = TaskRun::new("index", TaskRunId::from("index-1"));
        let skipped_at = Instant::now();
        transition_ok(run.skip(skipped_at), "Pending -> Skipped is legal");
        assert_eq!(run.state(), &TaskState::Skipped);
        // Skipped never started, so started_at stays None.
        assert!(run.started_at().is_none());
        assert_eq!(run.finished_at(), Some(skipped_at));
    }

    #[test]
    fn illegal_transitions_return_typed_error() {
        // complete on a Pending task (never started)
        let mut run = TaskRun::new("load", TaskRunId::from("load-1"));
        assert_eq!(
            run.complete(Instant::now()),
            Err(TransitionError::IllegalTransition {
                from: TaskState::Pending,
                attempted: "complete",
            })
        );

        // start on an already-Running task
        transition_ok(run.start(Instant::now()), "first start is legal");
        assert_eq!(
            run.start(Instant::now()),
            Err(TransitionError::IllegalTransition {
                from: TaskState::Running,
                attempted: "start",
            })
        );

        // skip on a Running task (must be Pending to skip)
        assert_eq!(
            run.skip(Instant::now()),
            Err(TransitionError::IllegalTransition {
                from: TaskState::Running,
                attempted: "skip",
            })
        );
    }

    #[test]
    fn terminal_states_reject_all_transitions() {
        let mut run = TaskRun::new("load", TaskRunId::from("load-1"));
        transition_ok(run.start(Instant::now()), "start");
        transition_ok(run.complete(Instant::now()), "complete");

        // Completed is terminal: every transition is illegal.
        assert_eq!(
            run.start(Instant::now()),
            Err(TransitionError::IllegalTransition {
                from: TaskState::Completed,
                attempted: "start",
            })
        );
        assert_eq!(
            run.complete(Instant::now()),
            Err(TransitionError::IllegalTransition {
                from: TaskState::Completed,
                attempted: "complete",
            })
        );
        assert_eq!(
            run.skip(Instant::now()),
            Err(TransitionError::IllegalTransition {
                from: TaskState::Completed,
                attempted: "skip",
            })
        );
    }

    /// Exhaustive check of the transition table: every (state, transition) pair
    /// is either a known-legal edge or an `IllegalTransition`. Locks the table
    /// so adding a state/transition without updating `next_state` fails loudly.
    #[test]
    fn transition_table_is_exhaustive_and_consistent() {
        let states = [
            TaskState::Pending,
            TaskState::Running,
            TaskState::Completed,
            TaskState::Failed {
                error: String::new(),
            },
            TaskState::Skipped,
        ];

        for from in states {
            // start: legal only Pending -> Running
            let mut run = TaskRun::new("t", TaskRunId::from("r"));
            run.state = from.clone();
            let res = run.start(Instant::now());
            if from == TaskState::Pending {
                assert_eq!(res, Ok(()));
                assert_eq!(run.state(), &TaskState::Running);
            } else {
                assert_eq!(
                    res,
                    Err(TransitionError::IllegalTransition {
                        from: from.clone(),
                        attempted: "start",
                    })
                );
            }

            // complete: legal only Running -> Completed
            let mut run = TaskRun::new("t", TaskRunId::from("r"));
            run.state = from.clone();
            let res = run.complete(Instant::now());
            if from == TaskState::Running {
                assert_eq!(res, Ok(()));
                assert_eq!(run.state(), &TaskState::Completed);
            } else {
                assert_eq!(
                    res,
                    Err(TransitionError::IllegalTransition {
                        from: from.clone(),
                        attempted: "complete",
                    })
                );
            }

            // fail: legal only Running -> Failed
            let mut run = TaskRun::new("t", TaskRunId::from("r"));
            run.state = from.clone();
            let res = run.fail(Instant::now(), "e".to_owned());
            if from == TaskState::Running {
                assert_eq!(res, Ok(()));
                assert_eq!(
                    run.state(),
                    &TaskState::Failed {
                        error: "e".to_owned()
                    }
                );
            } else {
                assert_eq!(
                    res,
                    Err(TransitionError::IllegalTransition {
                        from: from.clone(),
                        attempted: "fail",
                    })
                );
            }

            // skip: legal only Pending -> Skipped
            let mut run = TaskRun::new("t", TaskRunId::from("r"));
            run.state = from.clone();
            let res = run.skip(Instant::now());
            if from == TaskState::Pending {
                assert_eq!(res, Ok(()));
                assert_eq!(run.state(), &TaskState::Skipped);
            } else {
                assert_eq!(
                    res,
                    Err(TransitionError::IllegalTransition {
                        from: from.clone(),
                        attempted: "skip",
                    })
                );
            }
        }
    }
}
