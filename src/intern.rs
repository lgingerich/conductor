//! Process-local slug ↔ dense-id tables for planners and runners.
//!
//! Not part of the public definition API. Users create [`crate::Artifact`],
//! [`crate::Task`], and [`crate::Pipeline`] with human-readable names; an
//! [`Interner`] may be built internally when an execution plan needs cheap
//! `Copy` handles. See this module's persistence notes.

use std::collections::HashMap;

/// Dense id for an artifact slug (process-local).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ArtifactId(usize);

impl ArtifactId {
    /// Returns the raw numeric id.
    #[must_use]
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) const fn as_usize(self) -> usize {
        self.0
    }
}

/// Dense id for a task name (process-local).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TaskId(usize);

impl TaskId {
    /// Creates a task id from a dense index (process-local).
    #[must_use]
    pub(crate) const fn from_usize(id: usize) -> Self {
        Self(id)
    }

    /// Returns the raw numeric id.
    #[must_use]
    pub(crate) const fn as_usize(self) -> usize {
        self.0
    }
}

/// Dense id for a pipeline name (process-local).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PipelineId(usize);

impl PipelineId {
    /// Returns the raw numeric id.
    #[must_use]
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) const fn as_usize(self) -> usize {
        self.0
    }
}

/// Bidirectional map between slugs and dense ids for one namespace.
///
/// `by_name` answers "what id does this slug have?"; `by_id` answers "what
/// slug does this id have?" Both stay in sync on intern.
///
/// **Not durable.** This table is an in-process cache/index. It is not written
/// to disk today. On restart it starts empty; durable catalogs should store
/// slugs (and run history keyed by slug), then refill a new table via intern.
/// See this module's persistence notes.
#[derive(Debug, Default)]
struct NameTable {
    by_name: HashMap<String, usize>,
    by_id: Vec<String>,
}

impl NameTable {
    fn intern(&mut self, name: &str) -> usize {
        if let Some(&id) = self.by_name.get(name) {
            return id;
        }
        let id = self.by_id.len();
        self.by_name.insert(name.to_owned(), id);
        self.by_id.push(name.to_owned());
        id
    }

    fn resolve(&self, id: usize) -> Option<&str> {
        self.by_id.get(id).map(String::as_str)
    }
}

/// Maps human-readable slugs to typed numeric ids and back.
///
/// Internal to planning/execution. Ids are only meaningful relative to the
/// `Interner` that created them (and only for the lifetime of that process,
/// until persistence + re-intern on boot exists).
///
/// # Persistence
///
/// **Slugs are the durable identity. Numeric ids are process-local.**
///
/// Today the name tables live only in memory. If Conductor restarts, this
/// mapping is gone and previous id values must not be reused from disk or
/// another process. After persistence exists, boot should reload definitions
/// and run history **by slug**, then rebuild an `Interner` (re-intern) so
/// in-memory graphs get fresh ids for that process generation.
///
/// A WASM task container dying is separate: the interner stays valid as long
/// as the Conductor process does. Only Conductor itself exiting drops these
/// tables until they are rebuilt from durable state.
#[derive(Debug, Default)]
pub(crate) struct Interner {
    artifacts: NameTable,
    tasks: NameTable,
    pipelines: NameTable,
}

impl Interner {
    /// Creates an empty interner.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Interns an artifact slug, returning a stable [`ArtifactId`].
    pub(crate) fn artifact(&mut self, name: &str) -> ArtifactId {
        ArtifactId(self.artifacts.intern(name))
    }

    /// Resolves an [`ArtifactId`] to its slug.
    #[must_use]
    pub(crate) fn artifact_name(&self, id: ArtifactId) -> Option<&str> {
        self.artifacts.resolve(id.0)
    }

    /// Interns a task slug, returning a stable [`TaskId`].
    pub(crate) fn task(&mut self, name: &str) -> TaskId {
        TaskId(self.tasks.intern(name))
    }

    /// Resolves a [`TaskId`] to its slug.
    #[must_use]
    pub(crate) fn task_name(&self, id: TaskId) -> Option<&str> {
        self.tasks.resolve(id.0)
    }

    /// Interns a pipeline slug, returning a stable [`PipelineId`].
    pub(crate) fn pipeline(&mut self, name: &str) -> PipelineId {
        PipelineId(self.pipelines.intern(name))
    }

    /// Resolves a [`PipelineId`] to its slug.
    #[must_use]
    pub(crate) fn pipeline_name(&self, id: PipelineId) -> Option<&str> {
        self.pipelines.resolve(id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::Interner;

    #[test]
    fn intern_is_stable_and_resolvable() {
        let mut names = Interner::new();
        let a = names.artifact("postgres/app/users");
        let a2 = names.artifact("postgres/app/users");
        assert_eq!(a, a2);
        assert_eq!(names.artifact_name(a), Some("postgres/app/users"));

        let t = names.task("gcs_to_postgres");
        assert_eq!(names.task_name(t), Some("gcs_to_postgres"));

        let p = names.pipeline("load");
        assert_eq!(names.pipeline_name(p), Some("load"));

        assert_eq!(a.as_usize(), 0);
        assert_eq!(t.as_usize(), 0);
        assert_eq!(p.as_usize(), 0);
    }

    #[test]
    fn resolve_unknown_id_returns_none() {
        let names = Interner::new();
        assert_eq!(names.task_name(super::TaskId(99)), None);
    }
}
