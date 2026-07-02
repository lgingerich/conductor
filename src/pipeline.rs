use std::fmt;

use crate::asset::{Asset, AssetRun};
use crate::common::iso8601_now;

/// A unique identifier for a [`Pipeline`].
///
/// Wraps a string to provide type safety over raw pipeline identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PipelineKey(String);

impl PipelineKey {
    pub(crate) fn new(id: String) -> Self {
        Self(id)
    }
}

impl fmt::Display for PipelineKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for PipelineKey {
    fn from(id: String) -> Self {
        Self(id)
    }
}

/// A pipeline-scoped run identifier.
///
/// Use [`PipelineRunId::new`] for a user-specified ID, or
/// [`PipelineRunId::with_key`] to generate a timestamp-based ID from a
/// [`PipelineKey`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PipelineRunId(String);

impl PipelineRunId {
    /// Creates a new `PipelineRunId` from a user-specified identifier.
    pub(crate) fn new(id: String) -> Self {
        Self(id)
    }

    /// Creates a new `PipelineRunId` from a [`PipelineKey`] and the
    /// current timestamp, producing IDs like
    /// `my-pipeline-20260702T154301`.
    pub(crate) fn with_key(key: &PipelineKey) -> Self {
        Self(format!("{key}-{}", iso8601_now()))
    }
}

impl From<String> for PipelineRunId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

/// A pipeline that orders execution of its constituent [`Asset`]s.
///
/// Assets are executed in dependency order: an asset starts only after all
/// of its dependencies have completed.
#[derive(Debug, Clone)]
pub(crate) struct Pipeline {
    key: PipelineKey,
    assets: Vec<Asset>,
}

impl Pipeline {
    pub(crate) fn new(key: PipelineKey, assets: Vec<Asset>) -> Self {
        Self { key, assets }
    }
}

/// A record of one pipeline execution.
///
/// Tracks which pipeline was run, under which [`PipelineRunId`], and the
/// per-asset [`AssetRun`] outcomes.
#[derive(Debug, Clone)]
pub(crate) struct PipelineRun {
    pipeline: PipelineKey,
    run_id: PipelineRunId,
    assets: Vec<AssetRun>,
}

impl PipelineRun {
    pub(crate) fn new(pipeline: PipelineKey, run_id: PipelineRunId) -> Self {
        Self {
            pipeline,
            run_id,
            assets: Vec::new(),
        }
    }
}
