use std::fmt;
use std::time::Instant;

use crate::common::iso8601_now;

/// A unique identifier for an [`Asset`].
///
/// Wraps a string to provide type safety over raw asset identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AssetKey(String);

impl AssetKey {
    pub(crate) fn new(id: String) -> Self {
        Self(id)
    }
}

impl fmt::Display for AssetKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for AssetKey {
    fn from(id: String) -> Self {
        Self(id)
    }
}

/// An asset-scoped run identifier.
///
/// Use [`AssetRunId::new`] for a user-specified ID, or
/// [`AssetRunId::with_key`] to generate a timestamp-based ID from an
/// [`AssetKey`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AssetRunId(String);

impl AssetRunId {
    /// Creates a new `AssetRunId` from a user-specified identifier.
    pub(crate) fn new(id: String) -> Self {
        Self(id)
    }

    /// Creates a new `AssetRunId` from an [`AssetKey`] and the
    /// current timestamp, producing IDs like
    /// `my-asset-20260702T154301`.
    pub(crate) fn with_key(key: &AssetKey) -> Self {
        Self(format!("{key}-{}", iso8601_now()))
    }
}

impl From<String> for AssetRunId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

/// A data asset with a key and upstream dependencies.
///
/// Dependencies represent assets that must be materialized before this asset
/// can run.
#[derive(Debug, Clone)]
pub(crate) struct Asset {
    key: AssetKey,
    dependencies: Vec<AssetKey>,
}

impl Asset {
    pub(crate) fn new(key: AssetKey, dependencies: Vec<AssetKey>) -> Self {
        Self { key, dependencies }
    }
}

/// The execution state of an asset within a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AssetState {
    /// The asset has not yet started.
    None,
    /// The asset is currently running, started at the given instant.
    Running { at: Instant },
    /// The asset completed successfully at the given instant.
    Completed { at: Instant },
    /// The asset failed at the given instant with an error message.
    Failed { at: Instant, error: String },
}

/// A record of one asset's execution within a specific run.
#[derive(Debug, Clone)]
pub(crate) struct AssetRun {
    /// The asset being executed.
    asset: AssetKey,
    /// The [`AssetRunId`] this execution belongs to.
    run_id: AssetRunId,
    /// The current execution state.
    state: AssetState,
}

impl AssetRun {
    pub(crate) fn new(asset: AssetKey, run_id: AssetRunId) -> Self {
        Self {
            asset,
            run_id,
            state: AssetState::None,
        }
    }
}
