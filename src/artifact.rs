use std::fmt;

/// Why an [`Artifact`] slug failed validation.
///
/// Returned by [`Artifact::validate_slug`]. See `docs/core-primitives.md` for
/// the artifact identity contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidArtifactReason {
    /// The slug is empty or whitespace-only.
    Empty,
    /// The slug begins with `/`.
    LeadingSlash,
    /// The slug ends with `/`.
    TrailingSlash,
    /// The slug contains an empty segment (`//`).
    EmptySegment,
}

impl fmt::Display for InvalidArtifactReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("must not be empty or whitespace-only"),
            Self::LeadingSlash => f.write_str("must not begin with '/'"),
            Self::TrailingSlash => f.write_str("must not end with '/'"),
            Self::EmptySegment => f.write_str("must not contain empty segments ('//')"),
        }
    }
}

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

    /// Validates this artifact's slug against the path-shape rules.
    ///
    /// Slugs must be non-empty, must not begin or end with `/`, and must not
    /// contain empty segments (`//`). This is the leaf check; [`Pipeline::plan`](crate::Pipeline::plan)
    /// lifts failures into [`GraphError`](crate::GraphError).
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArtifactReason`] if the slug violates a rule.
    pub fn validate_slug(&self) -> Result<(), InvalidArtifactReason> {
        let slug = self.slug();
        if slug.trim().is_empty() {
            return Err(InvalidArtifactReason::Empty);
        }
        if slug.starts_with('/') {
            return Err(InvalidArtifactReason::LeadingSlash);
        }
        if slug.ends_with('/') {
            return Err(InvalidArtifactReason::TrailingSlash);
        }
        if slug.split('/').any(str::is_empty) {
            return Err(InvalidArtifactReason::EmptySegment);
        }
        Ok(())
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
    use super::{Artifact, InvalidArtifactReason};

    #[test]
    fn artifact_from_slug() {
        let artifact = Artifact::new("postgres/app/users");
        assert_eq!(artifact.slug(), "postgres/app/users");
        assert_eq!(artifact.to_string(), "postgres/app/users");
    }

    #[test]
    fn validate_slug_accepts_valid_and_rejects_invalid() {
        assert!(Artifact::new("postgres/app/users").validate_slug().is_ok());
        assert!(Artifact::new("users").validate_slug().is_ok());

        assert_eq!(
            Artifact::new("").validate_slug(),
            Err(InvalidArtifactReason::Empty)
        );
        assert_eq!(
            Artifact::new("  ").validate_slug(),
            Err(InvalidArtifactReason::Empty)
        );
        assert_eq!(
            Artifact::new("/x").validate_slug(),
            Err(InvalidArtifactReason::LeadingSlash)
        );
        assert_eq!(
            Artifact::new("x/").validate_slug(),
            Err(InvalidArtifactReason::TrailingSlash)
        );
        assert_eq!(
            Artifact::new("a//b").validate_slug(),
            Err(InvalidArtifactReason::EmptySegment)
        );
    }
}
