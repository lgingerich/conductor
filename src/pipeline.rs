use std::fmt;
use std::sync::Arc;

use crate::errors::GraphError;
use crate::graph::TaskGraph;
use crate::task::{Task, TaskName, TaskRun, TaskRunId};

/// Stable human-readable identity for a [`Pipeline`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipelineName(String);

impl PipelineName {
    /// Creates a pipeline name from a human-readable slug.
    ///
    /// # Examples
    ///
    /// ```
    /// use conductor::PipelineName;
    ///
    /// let name = PipelineName::new("nightly_load");
    /// assert_eq!(name.as_str(), "nightly_load");
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

impl fmt::Display for PipelineName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for PipelineName {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

/// A pipeline-scoped run identifier.
///
/// Construct with [`PipelineRunId::new`] or [`PipelineRunId::from`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipelineRunId(String);

impl PipelineRunId {
    /// Creates a pipeline run id from the given identifier.
    ///
    /// # Examples
    ///
    /// ```
    /// use conductor::PipelineRunId;
    ///
    /// let id = PipelineRunId::new("nightly-2026-07-16");
    /// assert_eq!(id.as_str(), "nightly-2026-07-16");
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

impl fmt::Display for PipelineRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for PipelineRunId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

/// A pipeline that orders execution of its constituent [`Task`]s.
///
/// Tasks run according to the task graph: data edges from artifact
/// input/output declarations, and control edges from `after`
/// dependencies. A task starts only after its dependencies are satisfied.
#[derive(Debug, Clone)]
pub struct Pipeline {
    name: PipelineName,
    tasks: Vec<Task>,
}

impl Pipeline {
    /// Creates a pipeline with the given human-readable name and tasks.
    ///
    /// # Examples
    ///
    /// ```
    /// use conductor::{Artifact, Pipeline, Task};
    ///
    /// let source = Artifact::new("gcs/users.parquet");
    /// let load = Task::new("load_users").with_inputs([source]);
    /// let pipeline = Pipeline::new("nightly_load", [load]);
    ///
    /// assert_eq!(pipeline.name().as_str(), "nightly_load");
    /// assert_eq!(pipeline.tasks().len(), 1);
    /// ```
    #[must_use]
    pub fn new(name: impl Into<PipelineName>, tasks: impl IntoIterator<Item = Task>) -> Self {
        Self {
            name: name.into(),
            tasks: tasks.into_iter().collect(),
        }
    }

    /// Returns this pipeline's name.
    #[must_use]
    pub fn name(&self) -> &PipelineName {
        &self.name
    }

    /// Returns the tasks in this pipeline.
    #[must_use]
    pub fn tasks(&self) -> &[Task] {
        &self.tasks
    }

    /// Compiles this pipeline into a validated task dependency graph.
    ///
    /// Derives data edges from artifact input/output ports and control edges
    /// from `after` dependencies.
    ///
    /// # Errors
    ///
    /// Returns [`GraphError`] when task names collide, an `after` target is
    /// missing, edges conflict, or the dependency graph contains a cycle.
    pub fn plan(&self) -> Result<TaskGraph, GraphError> {
        TaskGraph::from_pipeline(self)
    }
}

impl fmt::Display for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)
    }
}

/// A record of one pipeline execution.
///
/// A run is always created from a validated [`TaskGraph`] (the plan), which it
/// references via an [`Arc`]. Multiple runs can share the same plan. Tracks the
/// [`PipelineRunId`] and the per-task [`TaskRun`] outcomes.
#[derive(Debug, Clone)]
pub struct PipelineRun {
    /// The validated plan this run executes. Shared across runs of the same plan.
    graph: Arc<TaskGraph>,
    run_id: PipelineRunId,
    tasks: Vec<TaskRun>,
}

impl PipelineRun {
    /// Creates a pending run of `graph`, seeding one [`TaskRun`] per task.
    ///
    /// Task run ids default to each task's name (unique within a single run).
    /// The run references the plan via [`Arc`], so multiple runs can share one
    /// graph cheaply. Does not execute anything yet — that needs the in-process
    /// runner from the roadmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use conductor::{Pipeline, PipelineRun, PipelineRunId, TaskState};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let pipeline = Pipeline::new("load", [
    ///     conductor::Task::new("load_users"),
    ///     conductor::Task::new("create_indexes"),
    /// ]);
    /// let graph = Arc::new(pipeline.plan()?);
    /// let run = PipelineRun::new(graph, PipelineRunId::new("load-1"));
    ///
    /// assert_eq!(run.pipeline().as_str(), "load");
    /// assert_eq!(run.run_id().as_str(), "load-1");
    /// assert_eq!(run.tasks().len(), 2);
    /// assert!(run.tasks().iter().all(|t| t.state() == &TaskState::Pending));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn new(graph: Arc<TaskGraph>, run_id: PipelineRunId) -> Self {
        let tasks = graph
            .tasks()
            .iter()
            .map(|task| TaskRun::new(task.name().clone(), TaskRunId::from(task.name().as_str())))
            .collect();

        Self {
            graph,
            run_id,
            tasks,
        }
    }

    /// Returns the validated plan this run executes.
    #[must_use]
    pub fn graph(&self) -> &TaskGraph {
        &self.graph
    }

    /// Returns the pipeline name for this run.
    #[must_use]
    pub fn pipeline(&self) -> &PipelineName {
        self.graph.pipeline()
    }

    /// Returns this run's identifier.
    #[must_use]
    pub fn run_id(&self) -> &PipelineRunId {
        &self.run_id
    }

    /// Returns the per-task run records.
    #[must_use]
    pub fn tasks(&self) -> &[TaskRun] {
        &self.tasks
    }

    /// Looks up a task run by task name for mutation (e.g. to drive its state
    /// machine). Returns `None` if no task in the run has that name.
    #[must_use]
    pub fn task_mut(&mut self, name: &TaskName) -> Option<&mut TaskRun> {
        self.tasks
            .iter_mut()
            .find(|task_run| task_run.task() == name)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{Pipeline, PipelineName, PipelineRun, PipelineRunId};
    use crate::artifact::Artifact;
    use crate::graph::TaskGraph;
    use crate::task::{Task, TaskState};

    fn load_pipeline() -> Pipeline {
        let gcs = Artifact::new("gcs/users.parquet");
        let pg = Artifact::new("postgres/app/users");
        let load = Task::new("gcs_to_postgres")
            .with_inputs([gcs])
            .with_outputs([pg]);
        let index = Task::new("create_indexes").with_after([&load]);
        Pipeline::new("load", [load, index])
    }

    fn plan(pipeline: &Pipeline) -> TaskGraph {
        match pipeline.plan() {
            Ok(graph) => graph,
            Err(err) => panic!("expected plan to succeed: {err}"),
        }
    }

    #[test]
    fn run_seeds_one_pending_run_per_task() {
        let pipeline = load_pipeline();
        let graph = Arc::new(plan(&pipeline));
        let run = PipelineRun::new(graph, PipelineRunId::from("load-test"));

        assert_eq!(run.pipeline(), &PipelineName::from("load"));
        assert_eq!(run.run_id().to_string(), "load-test");
        assert_eq!(run.tasks().len(), 2);
        assert_eq!(run.tasks()[0].task().as_str(), "gcs_to_postgres");
        assert_eq!(run.tasks()[1].task().as_str(), "create_indexes");
        assert_eq!(run.tasks()[0].run_id().to_string(), "gcs_to_postgres");
        assert_eq!(run.tasks()[1].run_id().to_string(), "create_indexes");
        assert!(
            run.tasks()
                .iter()
                .all(|task_run| task_run.state() == &TaskState::Pending)
        );
    }

    #[test]
    fn run_references_validated_graph() {
        let pipeline = load_pipeline();
        let graph = Arc::new(plan(&pipeline));
        let run = PipelineRun::new(graph, PipelineRunId::from("load-test"));

        assert_eq!(run.graph().pipeline(), &PipelineName::from("load"));
        assert_eq!(run.graph().tasks().len(), 2);
        let order: Vec<&str> = run
            .graph()
            .topological_order()
            .into_iter()
            .map(|task| task.name().as_str())
            .collect();
        assert_eq!(order, ["gcs_to_postgres", "create_indexes"]);
    }

    #[test]
    fn multiple_runs_share_one_graph() {
        let pipeline = load_pipeline();
        let graph = Arc::new(plan(&pipeline));
        let run1 = PipelineRun::new(Arc::clone(&graph), PipelineRunId::from("run-1"));
        let run2 = PipelineRun::new(graph, PipelineRunId::from("run-2"));

        // Both runs reference the same plan (same pipeline, same task set).
        assert_eq!(run1.graph().pipeline(), run2.graph().pipeline());
        assert_eq!(run1.graph().tasks().len(), run2.graph().tasks().len());
        assert_ne!(run1.run_id(), run2.run_id());
    }

    #[test]
    fn empty_pipeline_run_has_no_tasks() {
        let pipeline = Pipeline::new("empty", []);
        let graph = Arc::new(plan(&pipeline));
        let run = PipelineRun::new(graph, PipelineRunId::from("empty-run"));
        assert!(run.tasks().is_empty());
        assert_eq!(run.run_id().to_string(), "empty-run");
    }
}
