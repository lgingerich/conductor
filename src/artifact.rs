use crate::intern::ArtifactId;

/// A named, addressable data product (table, file, model, etc.).
///
/// Artifacts are identity only — they are not runnable. Tasks declare
/// artifacts as inputs/outputs via [`crate::ArtifactId`]; lineage and
/// cascading recompute are derived from those declarations. The human-readable
/// slug for an id lives in an [`crate::Interner`]. See `docs/core-primitives.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    id: ArtifactId,
}

impl Artifact {
    /// Creates an artifact with the given id.
    #[must_use]
    pub fn new(id: ArtifactId) -> Self {
        Self { id }
    }

    /// Returns this artifact's id.
    #[must_use]
    pub fn id(&self) -> ArtifactId {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::Artifact;
    use crate::Interner;

    #[test]
    fn artifact_holds_interned_id() {
        let mut names = Interner::new();
        let id = names.artifact("postgres/app/users");
        let artifact = Artifact::new(id);
        assert_eq!(artifact.id(), id);
        assert_eq!(
            names.artifact_name(artifact.id()),
            Some("postgres/app/users")
        );
    }
}
