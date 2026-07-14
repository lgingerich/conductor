use std::fmt;

use crate::errors::GraphError;
use crate::graph::TaskGraph;
use crate::task::{Task, TaskRun, TaskRunId};

/// Stable human-readable identity for a [`Pipeline`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipelineName(String);

impl PipelineName {
    /// Creates a pipeline name from a human-readable slug.
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
/// Construct with [`PipelineRunId::from`] — callers always supply the run name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipelineRunId(String);

impl fmt::Display for PipelineRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for PipelineRunId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
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

    /// Creates a pending run of this pipeline.
    ///
    /// Seeds one [`TaskRun`] per task. Task run ids default to each task's
    /// name (unique within a single pipeline run). Does not execute anything
    /// yet — that needs the in-process runner from the roadmap.
    #[must_use]
    pub fn run(&self, run_id: impl AsRef<str>) -> PipelineRun {
        let tasks = self
            .tasks
            .iter()
            .map(|task| TaskRun::new(task.name().clone(), TaskRunId::from(task.name().as_str())))
            .collect();

        PipelineRun {
            pipeline: self.name.clone(),
            run_id: PipelineRunId::from(run_id.as_ref()),
            tasks,
        }
    }
}

impl fmt::Display for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)
    }
}

/// A record of one pipeline execution.
///
/// Tracks which pipeline was run, under which [`PipelineRunId`], and the
/// per-task [`TaskRun`] outcomes.
#[derive(Debug, Clone)]
pub struct PipelineRun {
    pipeline: PipelineName,
    run_id: PipelineRunId,
    tasks: Vec<TaskRun>,
}

impl PipelineRun {
    /// Creates an empty pipeline run with no task runs seeded.
    #[must_use]
    pub fn new(pipeline: impl Into<PipelineName>, run_id: PipelineRunId) -> Self {
        Self {
            pipeline: pipeline.into(),
            run_id,
            tasks: Vec::new(),
        }
    }

    /// Creates a pipeline run seeded with one pending [`TaskRun`] per task.
    ///
    /// Prefer [`Pipeline::run`] when task run ids can default to each task's
    /// name. Use this when you need caller-supplied task run ids.
    ///
    /// `task_run_ids` must have the same length as `pipeline.tasks()`.
    #[must_use]
    pub fn from_pipeline(
        pipeline: &Pipeline,
        run_id: PipelineRunId,
        task_run_ids: impl IntoIterator<Item = TaskRunId>,
    ) -> Option<Self> {
        let task_run_ids: Vec<TaskRunId> = task_run_ids.into_iter().collect();
        if task_run_ids.len() != pipeline.tasks().len() {
            return None;
        }

        let tasks = pipeline
            .tasks()
            .iter()
            .zip(task_run_ids)
            .map(|(task, task_run_id)| TaskRun::new(task.name().clone(), task_run_id))
            .collect();

        Some(Self {
            pipeline: pipeline.name().clone(),
            run_id,
            tasks,
        })
    }

    /// Returns the pipeline name for this run.
    #[must_use]
    pub fn pipeline(&self) -> &PipelineName {
        &self.pipeline
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
}

#[cfg(test)]
mod tests {
    use super::{Pipeline, PipelineName, PipelineRun, PipelineRunId};
    use crate::artifact::Artifact;
    use crate::task::{Task, TaskName, TaskRunId, TaskState};

    #[test]
    fn run_seeds_one_pending_run_per_task() {
        let gcs = Artifact::new("gcs/users.parquet");
        let pg = Artifact::new("postgres/app/users");
        let load = Task::new("gcs_to_postgres")
            .with_inputs([gcs])
            .with_outputs([pg]);
        let index = Task::new("create_indexes").with_after([&load]);

        let pipeline = Pipeline::new("load", [load, index]);
        let run = pipeline.run("load-test");

        assert_eq!(run.pipeline(), &PipelineName::from("load"));
        assert_eq!(run.run_id().to_string(), "load-test");
        assert_eq!(run.tasks().len(), 2);
        assert_eq!(run.tasks()[0].task(), &TaskName::from("gcs_to_postgres"));
        assert_eq!(run.tasks()[1].task(), &TaskName::from("create_indexes"));
        assert_eq!(run.tasks()[0].run_id().to_string(), "gcs_to_postgres");
        assert_eq!(run.tasks()[1].run_id().to_string(), "create_indexes");
        assert!(
            run.tasks()
                .iter()
                .all(|task_run| task_run.state() == &TaskState::Pending)
        );
    }

    #[test]
    fn from_pipeline_seeds_custom_task_run_ids() {
        let pipeline = Pipeline::new("load", [Task::new("only")]);
        let Some(run) = PipelineRun::from_pipeline(
            &pipeline,
            PipelineRunId::from("load-test"),
            [TaskRunId::from("only-custom")],
        ) else {
            panic!("task run id count should match")
        };
        assert_eq!(run.tasks()[0].run_id().to_string(), "only-custom");
    }

    #[test]
    fn from_pipeline_rejects_mismatched_task_run_id_count() {
        let pipeline = Pipeline::new("load", [Task::new("only")]);
        assert!(
            PipelineRun::from_pipeline(&pipeline, PipelineRunId::from("load-test"), []).is_none()
        );
    }

    #[test]
    fn empty_pipeline_run_has_no_tasks() {
        let pipeline = Pipeline::new("empty", []);
        let run = pipeline.run("empty-run");
        assert!(run.tasks().is_empty());
        assert_eq!(run.run_id().to_string(), "empty-run");
    }

    #[test]
    fn new_pipeline_run_starts_empty() {
        let run = PipelineRun::new("load", PipelineRunId::from("empty-seed"));
        assert!(run.tasks().is_empty());
    }
}
