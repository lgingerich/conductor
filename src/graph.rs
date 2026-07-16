//! Pipeline-local task dependency graph.
//!
//! Compiles [`Task`](crate::Task) input/output ports and `after` deps into a
//! validated DAG. Edges and queries are name-based; the graph stores a
//! name→index map for O(1) lookups. See `docs/core-primitives.md`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

use crate::artifact::Artifact;
use crate::errors::GraphError;
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
/// Endpoints are the human-readable task names; resolve them to [`Task`]s
/// through the owning graph with [`TaskGraph::edge_from`] /
/// [`TaskGraph::edge_to`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphEdge {
    // TODO(perf): each endpoint is an owned `TaskName` string, so every edge
    // allocates and carries strings for `from`/`to`. At scale — many graphs
    // held by a persistent scheduler, edge iteration in hot paths — this is
    // memory- and string-hash heavy.
    from: TaskName,
    to: TaskName,
    kind: EdgeKind,
}

impl GraphEdge {
    /// Returns the upstream task name for this edge.
    #[must_use]
    pub fn from(&self) -> &TaskName {
        &self.from
    }

    /// Returns the downstream task name for this edge.
    #[must_use]
    pub fn to(&self) -> &TaskName {
        &self.to
    }

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
    /// Tasks in declaration order.
    tasks: Vec<Task>,
    /// Task name → index into `tasks`, for O(1) lookup.
    by_name: HashMap<TaskName, usize>,
    edges: Vec<GraphEdge>,
    /// Unique upstream task indices per task (for ready-set).
    upstream: Vec<Vec<usize>>,
    /// Unique downstream task indices per task.
    downstream: Vec<Vec<usize>>,
    topological_order: Vec<usize>,
}

impl TaskGraph {
    /// Compiles a pipeline into a validated task dependency graph.
    pub(crate) fn from_pipeline(pipeline: &Pipeline) -> Result<Self, GraphError> {
        // TODO(perf): the graph is a pure function of the pipeline, so
        // persisting the compiled graph duplicates data re-derivable from the
        // pipeline declaration.
        let tasks = Self::collect_tasks(pipeline)?;
        let by_name = Self::index_task_names(&tasks);

        Self::validate_after_targets(&tasks, &by_name)?;
        let (edges, upstream, downstream, indegree) =
            Self::collect_edges(&tasks, &by_name, tasks.len())?;
        let topological_order = Self::topological_sort(&tasks, &downstream, indegree)?;

        Ok(Self {
            pipeline: pipeline.name().clone(),
            tasks,
            by_name,
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
        let idx = self.by_name[edge.from()];
        &self.tasks[idx]
    }

    /// Resolves an edge's downstream task.
    #[must_use]
    pub fn edge_to(&self, edge: &GraphEdge) -> &Task {
        let idx = self.by_name[edge.to()];
        &self.tasks[idx]
    }

    /// Returns tasks in topological order (declaration order among ties).
    #[must_use]
    pub fn topological_order(&self) -> Vec<&Task> {
        self.topological_order
            .iter()
            .map(|&idx| &self.tasks[idx])
            .collect()
    }

    /// Returns tasks with no upstream dependencies.
    #[must_use]
    pub fn roots(&self) -> Vec<&Task> {
        self.tasks
            .iter()
            .enumerate()
            .filter(|(idx, _)| self.upstream[*idx].is_empty())
            .map(|(_, task)| task)
            .collect()
    }

    /// Looks up a task by name.
    #[must_use]
    pub fn get(&self, name: &TaskName) -> Option<&Task> {
        self.by_name.get(name).map(|&idx| &self.tasks[idx])
    }

    /// Returns the declaration index of `name`, or `None` if not in the graph.
    ///
    /// Indices are stable for a given graph and match the position in
    /// [`tasks`](Self::tasks). Useful for callers that need a dense numeric
    /// handle for a task (e.g. feeding an external layout library) without
    /// rebuilding the graph's own name→index map.
    #[must_use]
    pub fn task_index(&self, name: &TaskName) -> Option<usize> {
        self.by_name.get(name).copied()
    }

    /// Returns unique upstream tasks for `task`, or `None` if unknown.
    #[must_use]
    pub fn upstream(&self, task: &TaskName) -> Option<Vec<&Task>> {
        let &idx = self.by_name.get(task)?;
        Some(
            self.upstream[idx]
                .iter()
                .map(|&up| &self.tasks[up])
                .collect(),
        )
    }

    /// Returns unique downstream tasks for `task`, or `None` if unknown.
    #[must_use]
    pub fn downstream(&self, task: &TaskName) -> Option<Vec<&Task>> {
        let &idx = self.by_name.get(task)?;
        Some(
            self.downstream[idx]
                .iter()
                .map(|&down| &self.tasks[down])
                .collect(),
        )
    }

    /// Returns tasks not in `completed` whose every upstream task is completed.
    #[must_use]
    pub fn ready(&self, completed: &HashSet<TaskName>) -> Vec<&Task> {
        // TODO(perf): O(tasks + edges) full rescan plus a Vec allocation per
        // call. Fine as an inspection query, but a push scheduler wants the
        // ready set maintained incrementally as tasks complete (O(newly-ready)
        // per completion), not recomputed from scratch each time. The
        // `completed` set is a string-keyed `HashSet<TaskName>`, so each
        // membership check hashes a string — costly once this is a hot path.
        self.tasks
            .iter()
            .enumerate()
            .filter(|(idx, task)| {
                if completed.contains(task.name()) {
                    return false;
                }
                self.upstream[*idx]
                    .iter()
                    .all(|&up| completed.contains(self.tasks[up].name()))
            })
            .map(|(_, task)| task)
            .collect()
    }

    fn index_task_names(tasks: &[Task]) -> HashMap<TaskName, usize> {
        tasks
            .iter()
            .enumerate()
            .map(|(i, task)| (task.name().clone(), i))
            .collect()
    }

    fn collect_tasks(pipeline: &Pipeline) -> Result<Vec<Task>, GraphError> {
        let mut tasks = Vec::with_capacity(pipeline.tasks().len());
        let mut seen_names: HashSet<&TaskName> = HashSet::new();

        for task in pipeline.tasks() {
            if !seen_names.insert(task.name()) {
                return Err(GraphError::DuplicateTask {
                    name: task.name().clone(),
                });
            }
            tasks.push(task.clone());
        }
        Ok(tasks)
    }

    fn validate_after_targets(
        tasks: &[Task],
        by_name: &HashMap<TaskName, usize>,
    ) -> Result<(), GraphError> {
        for task in tasks {
            for after in task.after() {
                if !by_name.contains_key(after) {
                    return Err(GraphError::UnknownAfter {
                        task: task.name().clone(),
                        missing: after.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Collects edges and builds adjacency lists in one pass.
    ///
    /// Pair-uniqueness is known at insertion time (the `EdgeIndex` rejects
    /// duplicate pairs), so adjacency is recorded there rather than re-scanned
    /// from the edge list.
    fn collect_edges(
        tasks: &[Task],
        by_name: &HashMap<TaskName, usize>,
        n: usize,
    ) -> Result<(Vec<GraphEdge>, Vec<Vec<usize>>, Vec<Vec<usize>>, Vec<usize>), GraphError> {
        // Artifact slug → indices of tasks that produce it.
        // TODO(perf): if the future cross-pipeline artifact catalog is
        // persisted as the source of truth, every materialization becomes a
        // hot durable write into that index.
        let mut producers: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, task) in tasks.iter().enumerate() {
            for artifact in task.outputs() {
                producers
                    .entry(artifact.slug().to_owned())
                    .or_default()
                    .push(idx);
            }
        }

        let mut edges = Vec::new();
        let mut upstream = vec![Vec::new(); n];
        let mut downstream = vec![Vec::new(); n];
        let mut indegree = vec![0usize; n];
        let mut index = EdgeIndex::default();

        for (to_idx, task) in tasks.iter().enumerate() {
            for artifact in task.inputs() {
                let Some(from_idxs) = producers.get(artifact.slug()) else {
                    continue;
                };
                for &from_idx in from_idxs {
                    if index.insert_data(&mut edges, tasks, from_idx, to_idx, artifact.clone())? {
                        upstream[to_idx].push(from_idx);
                        downstream[from_idx].push(to_idx);
                        indegree[to_idx] += 1;
                    }
                }
            }
            for after in task.after() {
                let from_idx = by_name[after];
                if index.insert_control(&mut edges, tasks, from_idx, to_idx)? {
                    upstream[to_idx].push(from_idx);
                    downstream[from_idx].push(to_idx);
                    indegree[to_idx] += 1;
                }
            }
        }
        Ok((edges, upstream, downstream, indegree))
    }

    fn topological_sort(
        tasks: &[Task],
        downstream: &[Vec<usize>],
        mut indegree: Vec<usize>,
    ) -> Result<Vec<usize>, GraphError> {
        let n = indegree.len();
        let mut ready: VecDeque<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();

        let mut topological_order = Vec::with_capacity(n);

        while let Some(idx) = ready.pop_front() {
            topological_order.push(idx);
            let mut newly_ready = Vec::new();
            for &down in &downstream[idx] {
                indegree[down] -= 1;
                if indegree[down] == 0 {
                    newly_ready.push(down);
                }
            }
            newly_ready.sort_unstable();
            for t in newly_ready {
                ready.push_back(t);
            }
        }

        if topological_order.len() != n {
            let ordered: HashSet<usize> = topological_order.iter().copied().collect();
            let leftover: HashSet<usize> = (0..n).filter(|id| !ordered.contains(id)).collect();
            return Err(GraphError::Cycle {
                path: Self::cycle_path(tasks, downstream, &leftover),
            });
        }

        Ok(topological_order)
    }

    fn cycle_path(
        tasks: &[Task],
        downstream: &[Vec<usize>],
        leftover: &HashSet<usize>,
    ) -> Vec<TaskName> {
        let Some(&start) = leftover.iter().min() else {
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
            .map(|&idx| tasks[idx].name().clone())
            .collect();
        names.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        names
    }

    fn find_cycle(
        idx: usize,
        tasks: &[Task],
        downstream: &[Vec<usize>],
        leftover: &HashSet<usize>,
        stack: &mut Vec<usize>,
        on_stack: &mut HashSet<usize>,
        visited: &mut HashSet<usize>,
    ) -> Option<Vec<TaskName>> {
        stack.push(idx);
        on_stack.insert(idx);
        visited.insert(idx);

        for &next in &downstream[idx] {
            if !leftover.contains(&next) {
                continue;
            }
            if on_stack.contains(&next) {
                let start_idx = stack.iter().position(|&x| x == next).unwrap_or(0);
                let mut path: Vec<TaskName> = stack[start_idx..]
                    .iter()
                    .map(|&t| tasks[t].name().clone())
                    .collect();
                path.push(tasks[next].name().clone());
                return Some(path);
            }
            if !visited.contains(&next)
                && let Some(path) =
                    Self::find_cycle(next, tasks, downstream, leftover, stack, on_stack, visited)
            {
                return Some(path);
            }
        }

        on_stack.remove(&idx);
        stack.pop();
        None
    }
}

#[derive(Default)]
struct EdgeIndex {
    control_pairs: HashSet<(usize, usize)>,
    data_keys: HashSet<(usize, usize, String)>,
    data_pairs: HashSet<(usize, usize)>,
}

impl EdgeIndex {
    /// Inserts a control edge. Returns `true` if the `(from, to)` pair was
    /// newly recorded (always `true` on `Ok` — a duplicate pair errors).
    fn insert_control(
        &mut self,
        edges: &mut Vec<GraphEdge>,
        tasks: &[Task],
        from: usize,
        to: usize,
    ) -> Result<bool, GraphError> {
        if self.control_pairs.contains(&(from, to)) || self.data_pairs.contains(&(from, to)) {
            return Err(GraphError::DuplicateEdge {
                from: tasks[from].name().clone(),
                to: tasks[to].name().clone(),
                kind: EdgeKind::Control,
            });
        }
        self.control_pairs.insert((from, to));
        edges.push(GraphEdge {
            from: tasks[from].name().clone(),
            to: tasks[to].name().clone(),
            kind: EdgeKind::Control,
        });
        Ok(true)
    }

    /// Inserts a data edge. Returns `true` if the `(from, to)` pair was newly
    /// recorded (multiple artifacts on the same pair return `false` for the
    /// second onward, so adjacency isn't double-counted).
    fn insert_data(
        &mut self,
        edges: &mut Vec<GraphEdge>,
        tasks: &[Task],
        from: usize,
        to: usize,
        artifact: Artifact,
    ) -> Result<bool, GraphError> {
        let key = (from, to, artifact.slug().to_owned());
        if self.data_keys.contains(&key) || self.control_pairs.contains(&(from, to)) {
            return Err(GraphError::DuplicateEdge {
                from: tasks[from].name().clone(),
                to: tasks[to].name().clone(),
                kind: EdgeKind::Data { artifact },
            });
        }
        let pair_is_new = !self.data_pairs.contains(&(from, to));
        self.data_keys.insert(key);
        self.data_pairs.insert((from, to));
        edges.push(GraphEdge {
            from: tasks[from].name().clone(),
            to: tasks[to].name().clone(),
            kind: EdgeKind::Data { artifact },
        });
        Ok(pair_is_new)
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
        let create_indexes = Task::new("create_indexes").with_after(["gcs_to_postgres"]);
        let vacuum = Task::new("vacuum").with_after(["create_indexes"]);

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
        let b = Task::new("b").with_after(["missing"]);
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
        let b = Task::new("b").with_after(["a", "a"]);
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
        let b = Task::new("b").with_inputs([art]).with_after(["a"]);
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
