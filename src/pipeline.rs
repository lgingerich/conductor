use std::fmt;

use crate::intern::PipelineId;
use crate::task::{Task, TaskRun, TaskRunId};

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
    id: PipelineId,
    tasks: Vec<Task>,
}

impl Pipeline {
    /// Creates a pipeline with the given id and tasks.
    #[must_use]
    pub fn new(id: PipelineId, tasks: Vec<Task>) -> Self {
        Self { id, tasks }
    }

    /// Returns this pipeline's id.
    #[must_use]
    pub fn id(&self) -> PipelineId {
        self.id
    }

    /// Returns the tasks in this pipeline.
    #[must_use]
    pub fn tasks(&self) -> &[Task] {
        &self.tasks
    }
}

/// A record of one pipeline execution.
///
/// Tracks which pipeline was run, under which [`PipelineRunId`], and the
/// per-task [`TaskRun`] outcomes.
#[derive(Debug, Clone)]
pub struct PipelineRun {
    pipeline: PipelineId,
    run_id: PipelineRunId,
    tasks: Vec<TaskRun>,
}

impl PipelineRun {
    /// Creates an empty pipeline run with no task runs seeded.
    #[must_use]
    pub fn new(pipeline: PipelineId, run_id: PipelineRunId) -> Self {
        Self {
            pipeline,
            run_id,
            tasks: Vec::new(),
        }
    }

    /// Creates a pipeline run seeded with one pending [`TaskRun`] per task.
    ///
    /// `task_run_ids` must have the same length as `pipeline.tasks()`; each
    /// id is caller-supplied (no generated defaults).
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
            .map(|(task, task_run_id)| TaskRun::new(task.id(), task_run_id))
            .collect();

        Some(Self {
            pipeline: pipeline.id(),
            run_id,
            tasks,
        })
    }

    /// Returns the pipeline id for this run.
    #[must_use]
    pub fn pipeline(&self) -> PipelineId {
        self.pipeline
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
    use super::{Pipeline, PipelineRun, PipelineRunId};
    use crate::Interner;
    use crate::task::{Task, TaskRunId, TaskState};

    #[test]
    fn from_pipeline_seeds_one_pending_run_per_task() {
        let mut names = Interner::new();
        let gcs = names.artifact("gcs/users.parquet");
        let pg = names.artifact("postgres/app/users");
        let load_id = names.task("gcs_to_postgres");
        let index_id = names.task("create_indexes");
        let pipeline_id = names.pipeline("load");

        let load = Task::new(load_id)
            .with_inputs(vec![gcs])
            .with_outputs(vec![pg]);
        let index = Task::new(index_id)
            .with_inputs(vec![pg])
            .with_after(vec![load_id]);

        let pipeline = Pipeline::new(pipeline_id, vec![load, index]);
        let Some(run) = PipelineRun::from_pipeline(
            &pipeline,
            PipelineRunId::from("load-test"),
            [
                TaskRunId::from("gcs_to_postgres-run"),
                TaskRunId::from("create_indexes-run"),
            ],
        ) else {
            panic!("task run id count should match")
        };

        assert_eq!(run.pipeline(), pipeline_id);
        assert_eq!(names.pipeline_name(run.pipeline()), Some("load"));
        assert_eq!(run.run_id().to_string(), "load-test");
        assert_eq!(run.tasks().len(), 2);
        assert_eq!(run.tasks()[0].task(), load_id);
        assert_eq!(run.tasks()[1].task(), index_id);
        assert_eq!(run.tasks()[0].run_id().to_string(), "gcs_to_postgres-run");
        assert!(
            run.tasks()
                .iter()
                .all(|task_run| task_run.state() == &TaskState::Pending)
        );
    }

    #[test]
    fn from_pipeline_rejects_mismatched_task_run_id_count() {
        let mut names = Interner::new();
        let pipeline_id = names.pipeline("load");
        let task_id = names.task("only");
        let pipeline = Pipeline::new(pipeline_id, vec![Task::new(task_id)]);
        assert!(
            PipelineRun::from_pipeline(&pipeline, PipelineRunId::from("load-test"), []).is_none()
        );
    }

    #[test]
    fn empty_pipeline_run_has_no_tasks() {
        let mut names = Interner::new();
        let pipeline_id = names.pipeline("empty");
        let pipeline = Pipeline::new(pipeline_id, vec![]);
        let Some(run) = PipelineRun::from_pipeline(&pipeline, PipelineRunId::from("empty-run"), [])
        else {
            panic!("empty pipeline should accept empty task run ids")
        };
        assert!(run.tasks().is_empty());
        assert_eq!(run.run_id().to_string(), "empty-run");
    }

    #[test]
    fn new_pipeline_run_starts_empty() {
        let mut names = Interner::new();
        let pipeline_id = names.pipeline("load");
        let run = PipelineRun::new(pipeline_id, PipelineRunId::from("empty-seed"));
        assert!(run.tasks().is_empty());
    }
}
