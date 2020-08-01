use super::*;
use crate::domain::*;
use crate::infra::serde::yaml::formats::datetime_rfc3339;
use crate::infra::serde::yaml::*;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::path::Path;

pub struct PrepService {}

impl PrepService {
    pub fn new() -> Self {
        Self {}
    }

    pub fn prepare(
        &self,
        prep_step: &Vec<PrepStep>,
        old_checkpoint: Option<PrepCheckpoint>,
        target: &Path,
    ) -> Result<ExecutionResult<PrepCheckpoint>, PrepError> {
        unimplemented!();
    }
}

#[derive(Debug)]
pub struct PrepResult {
    was_updated: bool,
    checkpoint: PrepCheckpoint,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_prepared: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub for_fetched_at: DateTime<Utc>,
}
