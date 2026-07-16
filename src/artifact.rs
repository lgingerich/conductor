use std::fmt;

/// A named, addressable data product (table, file, model, etc.).
///
/// Artifacts are identity only — they are not runnable. Tasks declare them as
/// [`inputs`](crate::Task::inputs) / [`outputs`](crate::Task::outputs); lineage
/// is derived from those declarations. See `docs/core-primitives.md`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Artifact {
    slug: String,
}

impl Artifact {
    /// Creates an artifact with the given human-readable slug.
    ///
    /// # Examples
    ///
    /// ```
    /// use conductor::Artifact;
    ///
    /// let artifact = Artifact::new("postgres/app/users");
    /// assert_eq!(artifact.slug(), "postgres/app/users");
    /// ```
    #[must_use]
    pub fn new(slug: impl Into<String>) -> Self {
        Self { slug: slug.into() }
    }

    /// Returns this artifact's slug.
    #[must_use]
    pub fn slug(&self) -> &str {
        &self.slug
    }
}

impl fmt::Display for Artifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.slug.fmt(f)
    }
}

impl From<&str> for Artifact {
    fn from(slug: &str) -> Self {
        Self::new(slug)
    }
}

#[cfg(test)]
mod tests {
    use super::Artifact;

    #[test]
    fn artifact_from_slug() {
        let artifact = Artifact::new("postgres/app/users");
        assert_eq!(artifact.slug(), "postgres/app/users");
        assert_eq!(artifact.to_string(), "postgres/app/users");
    }
}
