// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use kamu::domain::*;

use crate::WorkspaceLayout;

pub struct GcService {
    workspace_layout: Arc<WorkspaceLayout>,
}

#[dill::component(pub)]
impl GcService {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        Self { workspace_layout }
    }

    /// Do a complete clean of the cache
    pub fn purge_cache(&self) -> Result<GcResult, InternalError> {
        tracing::info!("Purging cache");

        let mut entries_freed = 0;
        let mut bytes_freed = 0;

        if self.workspace_layout.cache_dir.exists() {
            for res in self.workspace_layout.cache_dir.read_dir().int_err()? {
                let entry = res.int_err()?;
                if entry.path().is_dir() {
                    bytes_freed += fs_extra::dir::get_size(entry.path()).int_err()?;
                    std::fs::remove_dir_all(entry.path()).int_err()?;
                } else {
                    bytes_freed += entry.metadata().int_err()?.len();
                    std::fs::remove_file(entry.path()).int_err()?;
                }
                entries_freed += 1;
            }
        }

        tracing::info!(%bytes_freed, %entries_freed, "Purged cache");

        Ok(GcResult {
            entries_freed,
            bytes_freed,
        })
    }

    /// Evict stale entries to manage cache size
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn evict_cache(&self) -> Result<GcResult, InternalError> {
        // TODO: Make const after https://github.com/chronotope/chrono/issues/309
        // Or make into a config option
        let eviction_threshold: Duration = Duration::try_hours(24).unwrap();
        let now = Utc::now();
        let mut entries_freed = 0;
        let mut bytes_freed = 0;

        if self.workspace_layout.cache_dir.exists() {
            for res in self.workspace_layout.cache_dir.read_dir().int_err()? {
                let entry = res.int_err()?;
                let mtime: DateTime<Utc> =
                    chrono::DateTime::from(entry.metadata().int_err()?.modified().int_err()?);

                if (now - mtime) > eviction_threshold {
                    if entry.path().is_dir() {
                        bytes_freed += fs_extra::dir::get_size(entry.path()).int_err()?;
                        std::fs::remove_dir_all(entry.path()).int_err()?;
                    } else {
                        bytes_freed += entry.metadata().int_err()?.len();
                        std::fs::remove_file(entry.path()).int_err()?;
                    }
                    entries_freed += 1;
                }
            }
        }

        if bytes_freed > 0 || entries_freed > 0 {
            tracing::info!(%bytes_freed, %entries_freed, "Evicted cache");
        }

        Ok(GcResult {
            entries_freed,
            bytes_freed,
        })
    }
}

pub struct GcResult {
    pub entries_freed: usize,
    pub bytes_freed: u64,
}
