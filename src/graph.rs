//! Pipeline-local task dependency graph.
//!
//! Compiles [`Task`](crate::Task) input/output ports and `after` deps into a
//! validated DAG. See `docs/core-primitives.md`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

use crate::artifact::Artifact;
use crate::errors::GraphError;
use crate::intern::{ArtifactId, Interner, TaskId};
use crate::pipeline::{Pipeline, PipelineName};
use crate::task::{Task, TaskName};

/// Why one task must precede another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeKind {
    /// Ordering derived from a shared artifact (producer → consumer).
    Data {
        /// The artifact that links the two tasks.
        artifact: Artifact,
    },
    /// Explicit control dependency from [`Task::with_after`](crate::Task::with_after).
    Control,
}

impl fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data { artifact } => write!(f, "data({artifact})"),
            Self::Control => f.write_str("control"),
        }
    }
}

/// A directed dependency edge between two tasks in a [`TaskGraph`].
///
/// Endpoints are dense process-local task ids. Resolve them through the owning
/// graph with [`TaskGraph::edge_from`] / [`TaskGraph::edge_to`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphEdge {
    from: TaskId,
    to: TaskId,
    kind: EdgeKind,
}

impl GraphEdge {
    /// Returns why this edge exists.
    #[must_use]
    pub fn kind(&self) -> &EdgeKind {
        &self.kind
    }
}

/// Validated, pipeline-local task dependency DAG.
#[derive(Debug, Clone)]
pub struct TaskGraph {
    pipeline: PipelineName,
    /// Tasks in declaration order (index == dense [`TaskId`]).
    tasks: Vec<Task>,
    edges: Vec<GraphEdge>,
    /// Unique upstream task ids per task (for ready-set).
    upstream: Vec<Vec<TaskId>>,
    /// Unique downstream task ids per task.
    downstream: Vec<Vec<TaskId>>,
    topological_order: Vec<TaskId>,
}

impl TaskGraph {
    /// Compiles a pipeline into a validated task dependency graph.
    pub(crate) fn from_pipeline(pipeline: &Pipeline) -> Result<Self, GraphError> {
        let mut interner = Interner::new();
        let tasks = Self::collect_tasks(pipeline, &mut interner)?;
        let name_to_id = Self::index_task_names(&tasks);

        Self::validate_after_targets(&tasks, &name_to_id)?;
        let edges = Self::collect_edges(&tasks, &name_to_id, &mut interner)?;
        let (upstream, downstream, indegree) = Self::build_adjacency(&edges, tasks.len());
        let topological_order = Self::topological_sort(&tasks, &downstream, indegree)?;

        Ok(Self {
            pipeline: pipeline.name().clone(),
            tasks,
            edges,
            upstream,
            downstream,
            topological_order,
        })
    }

    /// Returns the pipeline name this graph was compiled from.
    #[must_use]
    pub fn pipeline(&self) -> &PipelineName {
        &self.pipeline
    }

    /// Returns tasks in declaration order.
    #[must_use]
    pub fn tasks(&self) -> &[Task] {
        &self.tasks
    }

    /// Returns all dependency edges (endpoints as dense ids).
    #[must_use]
    pub fn edges(&self) -> &[GraphEdge] {
        &self.edges
    }

    /// Resolves an edge's upstream task.
    #[must_use]
    pub fn edge_from(&self, edge: &GraphEdge) -> &Task {
        &self.tasks[edge.from.as_usize()]
    }

    /// Resolves an edge's downstream task.
    #[must_use]
    pub fn edge_to(&self, edge: &GraphEdge) -> &Task {
        &self.tasks[edge.to.as_usize()]
    }

    /// Returns tasks in topological order (declaration order among ties).
    #[must_use]
    pub fn topological_order(&self) -> Vec<&Task> {
        self.topological_order
            .iter()
            .map(|id| &self.tasks[id.as_usize()])
            .collect()
    }

    /// Returns tasks with no upstream dependencies.
    #[must_use]
    pub fn roots(&self) -> Vec<&Task> {
        self.tasks
            .iter()
            .enumerate()
            .filter(|(id, _)| self.upstream[*id].is_empty())
            .map(|(_, task)| task)
            .collect()
    }

    /// Looks up a task by name.
    #[must_use]
    pub fn get(&self, name: &TaskName) -> Option<&Task> {
        self.task_id(name).map(|id| &self.tasks[id.as_usize()])
    }

    /// Returns unique upstream tasks for `task`, or `None` if unknown.
    #[must_use]
    pub fn upstream(&self, task: &TaskName) -> Option<Vec<&Task>> {
        let id = self.task_id(task)?;
        Some(
            self.upstream[id.as_usize()]
                .iter()
                .map(|up| &self.tasks[up.as_usize()])
                .collect(),
        )
    }

    /// Returns unique downstream tasks for `task`, or `None` if unknown.
    #[must_use]
    pub fn downstream(&self, task: &TaskName) -> Option<Vec<&Task>> {
        let id = self.task_id(task)?;
        Some(
            self.downstream[id.as_usize()]
                .iter()
                .map(|down| &self.tasks[down.as_usize()])
                .collect(),
        )
    }

    /// Returns tasks not in `completed` whose every upstream task is completed.
    #[must_use]
    pub fn ready(&self, completed: &HashSet<TaskName>) -> Vec<&Task> {
        self.tasks
            .iter()
            .enumerate()
            .filter(|(id, task)| {
                if completed.contains(task.name()) {
                    return false;
                }
                self.upstream[*id]
                    .iter()
                    .all(|up| completed.contains(self.tasks[up.as_usize()].name()))
            })
            .map(|(_, task)| task)
            .collect()
    }

    fn task_id(&self, name: &TaskName) -> Option<TaskId> {
        self.tasks
            .iter()
            .position(|task| task.name() == name)
            .map(TaskId::from_usize)
    }

    fn index_task_names(tasks: &[Task]) -> HashMap<&TaskName, TaskId> {
        tasks
            .iter()
            .enumerate()
            .map(|(i, task)| (task.name(), TaskId::from_usize(i)))
            .collect()
    }

    fn collect_tasks(
        pipeline: &Pipeline,
        interner: &mut Interner,
    ) -> Result<Vec<Task>, GraphError> {
        let mut tasks = Vec::with_capacity(pipeline.tasks().len());
        let mut seen_names: HashSet<&TaskName> = HashSet::new();

        let pipeline_id = interner.pipeline(pipeline.name().as_str());
        debug_assert_eq!(
            interner.pipeline_name(pipeline_id),
            Some(pipeline.name().as_str())
        );

        for task in pipeline.tasks() {
            if !seen_names.insert(task.name()) {
                return Err(GraphError::DuplicateTask {
                    name: task.name().clone(),
                });
            }
            let id = interner.task(task.name().as_str());
            debug_assert_eq!(id.as_usize(), tasks.len());
            debug_assert_eq!(interner.task_name(id), Some(task.name().as_str()));
            tasks.push(task.clone());
        }
        Ok(tasks)
    }

    fn validate_after_targets(
        tasks: &[Task],
        name_to_id: &HashMap<&TaskName, TaskId>,
    ) -> Result<(), GraphError> {
        for task in tasks {
            for after in task.after() {
                if !name_to_id.contains_key(after) {
                    return Err(GraphError::UnknownAfter {
                        task: task.name().clone(),
                        missing: after.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn collect_edges(
        tasks: &[Task],
        name_to_id: &HashMap<&TaskName, TaskId>,
        interner: &mut Interner,
    ) -> Result<Vec<GraphEdge>, GraphError> {
        let mut producers: HashMap<ArtifactId, Vec<TaskId>> = HashMap::new();
        for task in tasks {
            let id = name_to_id[task.name()];
            for artifact in task.outputs() {
                let art_id = interner.artifact(artifact.slug());
                debug_assert_eq!(interner.artifact_name(art_id), Some(artifact.slug()));
                producers.entry(art_id).or_default().push(id);
            }
        }

        let mut edges = Vec::new();
        let mut index = EdgeIndex::default();

        for task in tasks {
            let to = name_to_id[task.name()];
            for artifact in task.inputs() {
                let art_id = interner.artifact(artifact.slug());
                let Some(from_ids) = producers.get(&art_id) else {
                    continue;
                };
                for &from in from_ids {
                    index.insert_data(&mut edges, tasks, from, to, art_id, artifact.clone())?;
                }
            }
            for after in task.after() {
                let from = name_to_id[after];
                index.insert_control(&mut edges, tasks, from, to)?;
            }
        }
        Ok(edges)
    }

    fn build_adjacency(
        edges: &[GraphEdge],
        n: usize,
    ) -> (Vec<Vec<TaskId>>, Vec<Vec<TaskId>>, Vec<usize>) {
        let mut upstream = vec![Vec::new(); n];
        let mut downstream = vec![Vec::new(); n];
        let mut indegree = vec![0usize; n];

        for edge in edges {
            let from_u = edge.from.as_usize();
            let to_u = edge.to.as_usize();
            if !upstream[to_u].contains(&edge.from) {
                upstream[to_u].push(edge.from);
                indegree[to_u] += 1;
            }
            if !downstream[from_u].contains(&edge.to) {
                downstream[from_u].push(edge.to);
            }
        }
        (upstream, downstream, indegree)
    }

    fn topological_sort(
        tasks: &[Task],
        downstream: &[Vec<TaskId>],
        mut indegree: Vec<usize>,
    ) -> Result<Vec<TaskId>, GraphError> {
        let n = tasks.len();
        let mut ready: VecDeque<TaskId> = (0..n)
            .filter(|&i| indegree[i] == 0)
            .map(TaskId::from_usize)
            .collect();

        let mut topological_order = Vec::with_capacity(n);

        while let Some(id) = ready.pop_front() {
            topological_order.push(id);
            let mut newly_ready = Vec::new();
            for &down in &downstream[id.as_usize()] {
                let d = down.as_usize();
                indegree[d] -= 1;
                if indegree[d] == 0 {
                    newly_ready.push(down);
                }
            }
            newly_ready.sort_by_key(|t| t.as_usize());
            for t in newly_ready {
                ready.push_back(t);
            }
        }

        if topological_order.len() != n {
            let ordered: HashSet<TaskId> = topological_order.iter().copied().collect();
            let leftover: HashSet<TaskId> = (0..n)
                .map(TaskId::from_usize)
                .filter(|id| !ordered.contains(id))
                .collect();
            return Err(GraphError::Cycle {
                path: Self::cycle_path(tasks, downstream, &leftover),
            });
        }

        Ok(topological_order)
    }

    fn cycle_path(
        tasks: &[Task],
        downstream: &[Vec<TaskId>],
        leftover: &HashSet<TaskId>,
    ) -> Vec<TaskName> {
        let start = leftover.iter().copied().min_by_key(|id| id.as_usize());
        let Some(start) = start else {
            return Vec::new();
        };

        let mut stack = Vec::new();
        let mut on_stack = HashSet::new();
        let mut visited = HashSet::new();

        if let Some(path) = Self::find_cycle(
            start,
            tasks,
            downstream,
            leftover,
            &mut stack,
            &mut on_stack,
            &mut visited,
        ) {
            return path;
        }

        let mut names: Vec<TaskName> = leftover
            .iter()
            .map(|id| tasks[id.as_usize()].name().clone())
            .collect();
        names.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        names
    }

    fn find_cycle(
        id: TaskId,
        tasks: &[Task],
        downstream: &[Vec<TaskId>],
        leftover: &HashSet<TaskId>,
        stack: &mut Vec<TaskId>,
        on_stack: &mut HashSet<TaskId>,
        visited: &mut HashSet<TaskId>,
    ) -> Option<Vec<TaskName>> {
        stack.push(id);
        on_stack.insert(id);
        visited.insert(id);

        for &next in &downstream[id.as_usize()] {
            if !leftover.contains(&next) {
                continue;
            }
            if on_stack.contains(&next) {
                let start_idx = stack.iter().position(|&x| x == next).unwrap_or(0);
                let mut path: Vec<TaskName> = stack[start_idx..]
                    .iter()
                    .map(|t| tasks[t.as_usize()].name().clone())
                    .collect();
                path.push(tasks[next.as_usize()].name().clone());
                return Some(path);
            }
            if !visited.contains(&next)
                && let Some(path) =
                    Self::find_cycle(next, tasks, downstream, leftover, stack, on_stack, visited)
            {
                return Some(path);
            }
        }

        on_stack.remove(&id);
        stack.pop();
        None
    }
}

#[derive(Default)]
struct EdgeIndex {
    control_pairs: HashSet<(TaskId, TaskId)>,
    data_keys: HashSet<(TaskId, TaskId, ArtifactId)>,
    data_pairs: HashSet<(TaskId, TaskId)>,
}

impl EdgeIndex {
    fn insert_control(
        &mut self,
        edges: &mut Vec<GraphEdge>,
        tasks: &[Task],
        from: TaskId,
        to: TaskId,
    ) -> Result<(), GraphError> {
        if self.control_pairs.contains(&(from, to)) || self.data_pairs.contains(&(from, to)) {
            return Err(GraphError::DuplicateEdge {
                from: tasks[from.as_usize()].name().clone(),
                to: tasks[to.as_usize()].name().clone(),
                kind: EdgeKind::Control,
            });
        }
        self.control_pairs.insert((from, to));
        edges.push(GraphEdge {
            from,
            to,
            kind: EdgeKind::Control,
        });
        Ok(())
    }

    fn insert_data(
        &mut self,
        edges: &mut Vec<GraphEdge>,
        tasks: &[Task],
        from: TaskId,
        to: TaskId,
        artifact_id: ArtifactId,
        artifact: Artifact,
    ) -> Result<(), GraphError> {
        let key = (from, to, artifact_id);
        if self.data_keys.contains(&key) || self.control_pairs.contains(&(from, to)) {
            return Err(GraphError::DuplicateEdge {
                from: tasks[from.as_usize()].name().clone(),
                to: tasks[to.as_usize()].name().clone(),
                kind: EdgeKind::Data { artifact },
            });
        }
        self.data_keys.insert(key);
        self.data_pairs.insert((from, to));
        edges.push(GraphEdge {
            from,
            to,
            kind: EdgeKind::Data { artifact },
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{EdgeKind, TaskGraph};
    use crate::artifact::Artifact;
    use crate::errors::GraphError;
    use crate::pipeline::{Pipeline, PipelineName};
    use crate::task::{Task, TaskName};
    use std::collections::HashSet;

    fn explore_shaped_pipeline() -> Pipeline {
        let bq = Artifact::new("bigquery/analytics/users");
        let gcs = Artifact::new("gcs/analytics/users.parquet");
        let pg = Artifact::new("postgres/app/users");

        let run_sql = Task::new("run_sql").with_outputs([bq.clone()]);
        let bq_to_gcs = Task::new("bq_to_gcs")
            .with_inputs([bq])
            .with_outputs([gcs.clone()]);
        let gcs_to_postgres = Task::new("gcs_to_postgres")
            .with_inputs([gcs])
            .with_outputs([pg]);
        let create_indexes = Task::new("create_indexes").with_after([&gcs_to_postgres]);
        let vacuum = Task::new("vacuum").with_after([&create_indexes]);

        Pipeline::new(
            "load",
            [run_sql, bq_to_gcs, gcs_to_postgres, create_indexes, vacuum],
        )
    }

    fn assert_ok(pipeline: &Pipeline) -> TaskGraph {
        match TaskGraph::from_pipeline(pipeline) {
            Ok(graph) => graph,
            Err(err) => panic!("expected plan to succeed: {err}"),
        }
    }

    fn assert_err(pipeline: &Pipeline) -> GraphError {
        match TaskGraph::from_pipeline(pipeline) {
            Ok(_) => panic!("expected plan to fail"),
            Err(err) => err,
        }
    }

    fn names<'a>(tasks: &[&'a Task]) -> Vec<&'a str> {
        tasks.iter().map(|task| task.name().as_str()).collect()
    }

    fn require_deps<'a>(deps: Option<Vec<&'a Task>>, label: &str) -> Vec<&'a Task> {
        match deps {
            Some(tasks) => tasks,
            None => panic!("expected {label} tasks"),
        }
    }

    #[test]
    fn plans_linear_data_chain_and_control_tail() {
        let pipeline = explore_shaped_pipeline();
        let graph = assert_ok(&pipeline);

        assert_eq!(graph.pipeline(), &PipelineName::from("load"));
        assert_eq!(
            names(&graph.topological_order()),
            [
                "run_sql",
                "bq_to_gcs",
                "gcs_to_postgres",
                "create_indexes",
                "vacuum"
            ]
        );
        assert_eq!(names(&graph.roots()), vec!["run_sql"]);

        let edges = graph.edges();
        assert_eq!(edges.len(), 4);

        assert_eq!(graph.edge_from(&edges[0]).name().as_str(), "run_sql");
        assert_eq!(graph.edge_to(&edges[0]).name().as_str(), "bq_to_gcs");
        assert!(matches!(
            edges[0].kind(),
            EdgeKind::Data { artifact } if artifact.slug() == "bigquery/analytics/users"
        ));

        assert_eq!(graph.edge_from(&edges[1]).name().as_str(), "bq_to_gcs");
        assert_eq!(graph.edge_to(&edges[1]).name().as_str(), "gcs_to_postgres");
        assert!(matches!(
            edges[1].kind(),
            EdgeKind::Data { artifact } if artifact.slug() == "gcs/analytics/users.parquet"
        ));

        assert_eq!(
            graph.edge_from(&edges[2]).name().as_str(),
            "gcs_to_postgres"
        );
        assert_eq!(graph.edge_to(&edges[2]).name().as_str(), "create_indexes");
        assert_eq!(edges[2].kind(), &EdgeKind::Control);

        assert_eq!(graph.edge_from(&edges[3]).name().as_str(), "create_indexes");
        assert_eq!(graph.edge_to(&edges[3]).name().as_str(), "vacuum");
        assert_eq!(edges[3].kind(), &EdgeKind::Control);

        let gcs_to_postgres = TaskName::from("gcs_to_postgres");
        let create_indexes = TaskName::from("create_indexes");
        assert_eq!(
            names(&require_deps(
                graph.downstream(&gcs_to_postgres),
                "downstream"
            )),
            vec!["create_indexes"]
        );
        assert_eq!(
            names(&require_deps(graph.upstream(&create_indexes), "upstream")),
            vec!["gcs_to_postgres"]
        );

        let completed = HashSet::from([gcs_to_postgres.clone()]);
        assert_eq!(
            names(&graph.ready(&completed)),
            vec!["run_sql", "create_indexes"]
        );

        let run_sql = TaskName::from("run_sql");
        let bq_to_gcs = TaskName::from("bq_to_gcs");
        let completed = HashSet::from([run_sql, bq_to_gcs, gcs_to_postgres]);
        assert_eq!(names(&graph.ready(&completed)), vec!["create_indexes"]);
    }

    #[test]
    fn duplicate_task_name_errors() {
        let pipeline = Pipeline::new("p", [Task::new("a"), Task::new("a")]);
        assert_eq!(
            assert_err(&pipeline),
            GraphError::DuplicateTask {
                name: TaskName::from("a")
            }
        );
    }

    #[test]
    fn unknown_after_errors() {
        let orphan = Task::new("missing");
        let b = Task::new("b").with_after([&orphan]);
        let pipeline = Pipeline::new("p", [b]);
        assert_eq!(
            assert_err(&pipeline),
            GraphError::UnknownAfter {
                task: TaskName::from("b"),
                missing: TaskName::from("missing"),
            }
        );
    }

    #[test]
    fn cycle_errors() {
        let x = Artifact::new("x");
        let y = Artifact::new("y");
        let a = Task::new("a")
            .with_inputs([y.clone()])
            .with_outputs([x.clone()]);
        let b = Task::new("b").with_inputs([x]).with_outputs([y]);
        let pipeline = Pipeline::new("p", [a, b]);
        assert!(matches!(assert_err(&pipeline), GraphError::Cycle { .. }));
    }

    #[test]
    fn duplicate_control_edge_errors() {
        let a = Task::new("a");
        let b = Task::new("b").with_after([&a, &a]);
        let pipeline = Pipeline::new("p", [a, b]);
        assert!(matches!(
            assert_err(&pipeline),
            GraphError::DuplicateEdge {
                kind: EdgeKind::Control,
                ..
            }
        ));
    }

    #[test]
    fn duplicate_data_edge_errors() {
        let art = Artifact::new("table");
        let a = Task::new("a").with_outputs([art.clone(), art.clone()]);
        let b = Task::new("b").with_inputs([art.clone(), art]);
        let pipeline = Pipeline::new("p", [a, b]);
        assert!(matches!(
            assert_err(&pipeline),
            GraphError::DuplicateEdge {
                kind: EdgeKind::Data { .. },
                ..
            }
        ));
    }

    #[test]
    fn data_and_control_same_pair_errors() {
        let art = Artifact::new("table");
        let a = Task::new("a").with_outputs([art.clone()]);
        let b = Task::new("b").with_inputs([art]).with_after([&a]);
        let pipeline = Pipeline::new("p", [a, b]);
        assert!(matches!(
            assert_err(&pipeline),
            GraphError::DuplicateEdge { .. }
        ));
    }

    #[test]
    fn multiple_data_artifacts_same_pair_allowed() {
        let x = Artifact::new("x");
        let y = Artifact::new("y");
        let a = Task::new("a").with_outputs([x.clone(), y.clone()]);
        let b = Task::new("b").with_inputs([x, y]);
        let pipeline = Pipeline::new("p", [a, b]);
        let graph = assert_ok(&pipeline);
        assert_eq!(graph.edges().len(), 2);
        assert!(matches!(
            graph.edges()[0].kind(),
            EdgeKind::Data { artifact } if artifact.slug() == "x"
        ));
        assert!(matches!(
            graph.edges()[1].kind(),
            EdgeKind::Data { artifact } if artifact.slug() == "y"
        ));
        let completed = HashSet::from([TaskName::from("a")]);
        assert_eq!(names(&graph.ready(&completed)), vec!["b"]);
        assert_eq!(
            names(&require_deps(
                graph.upstream(&TaskName::from("b")),
                "upstream"
            )),
            vec!["a"]
        );
    }

    #[test]
    fn external_input_adds_no_edge_and_consumer_is_root() {
        let external = Artifact::new("warehouse/external");
        let consumer = Task::new("consume").with_inputs([external]);
        let pipeline = Pipeline::new("p", [consumer]);
        let graph = assert_ok(&pipeline);
        assert!(graph.edges().is_empty());
        assert_eq!(names(&graph.roots()), vec!["consume"]);
        assert_eq!(names(&graph.ready(&HashSet::new())), vec!["consume"]);
    }
}
