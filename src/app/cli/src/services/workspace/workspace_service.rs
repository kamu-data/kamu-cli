// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::KAMU_WORKSPACE_DIR_NAME;

use crate::{WorkspaceConfig, WorkspaceLayout, WorkspaceVersion};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ENV_VAR_KAMU_WORKSPACE: &str = "KAMU_WORKSPACE";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WorkspaceService {
    workspace_layout: Arc<WorkspaceLayout>,
    workspace_config: WorkspaceConfig,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl WorkspaceService {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>, multi_tenant: bool) -> Self {
        let workspace_config = Self::init_workspace_config(&workspace_layout, multi_tenant);

        Self {
            workspace_layout,
            workspace_config,
        }
    }

    fn init_workspace_config(
        workspace_layout: &WorkspaceLayout,
        multi_tenant: bool,
    ) -> WorkspaceConfig {
        if workspace_layout.config_path.is_file() {
            WorkspaceConfig::load_from(&workspace_layout.config_path).unwrap()
        } else {
            WorkspaceConfig::new(multi_tenant)
        }
    }

    fn try_read_workspace_from_env(cwd: &Path) -> Option<WorkspaceLayout> {
        std::env::var_os(ENV_VAR_KAMU_WORKSPACE)
            .map(|workspace| cwd.join(workspace).join(KAMU_WORKSPACE_DIR_NAME))
            .map(WorkspaceLayout::new)
    }

    pub fn find_workspace() -> WorkspaceLayout {
        let cwd = std::env::current_dir().unwrap();

        if let Some(ws) = Self::try_read_workspace_from_env(&cwd) {
            return ws;
        }

        if let Some(ws) = Self::find_workspace_rec(&cwd) {
            ws
        } else {
            WorkspaceLayout::new(cwd.join(KAMU_WORKSPACE_DIR_NAME))
        }
    }

    fn find_workspace_rec(p: &Path) -> Option<WorkspaceLayout> {
        let root_dir = p.join(KAMU_WORKSPACE_DIR_NAME);

        if root_dir.exists() {
            Some(WorkspaceLayout::new(root_dir))
        } else if let Some(parent) = p.parent() {
            Self::find_workspace_rec(parent)
        } else {
            None
        }
    }

    /// Layout of the workspace (if we are in one)
    pub fn layout(&self) -> Option<&WorkspaceLayout> {
        if self.is_in_workspace() {
            Some(self.workspace_layout.as_ref())
        } else {
            None
        }
    }

    /// Whether there is an initialized workspace
    pub fn is_in_workspace(&self) -> bool {
        self.workspace_layout.root_dir.is_dir()
    }

    /// Whether the workspace is multi-tenant
    pub fn is_multi_tenant_workspace(&self) -> bool {
        self.workspace_config.multi_tenant
    }

    /// Whether workspace requires and upgrade
    pub fn is_upgrade_needed(&self) -> Result<bool, InternalError> {
        Ok(self.workspace_version()? != Some(WorkspaceVersion::LATEST))
    }

    /// Returns the version of the current workspace
    pub fn workspace_version(&self) -> Result<Option<WorkspaceVersion>, InternalError> {
        if !self.is_in_workspace() {
            Ok(None)
        } else if !self.workspace_layout.version_path.is_file() {
            Ok(Some(WorkspaceVersion::V0_Initial))
        } else {
            let version_str =
                std::fs::read_to_string(&self.workspace_layout.version_path).int_err()?;

            let version: u32 = version_str.trim().parse().int_err()?;
            Ok(Some(version.into()))
        }
    }

    /// Returns the version that code expects to function correctly
    pub fn latest_supported_version(&self) -> WorkspaceVersion {
        WorkspaceVersion::LATEST
    }

    /// Perform an upgrade of the workspace if necessary
    pub async fn upgrade(&self) -> Result<WorkspaceUpgradeResult, WorkspaceUpgradeError> {
        let prev_version = self
            .workspace_version()?
            .expect("Upgrade called when not in workspace");

        let mut current_version = prev_version;
        let new_version = WorkspaceVersion::LATEST;

        if current_version == new_version {
            return Ok(WorkspaceUpgradeResult {
                prev_version,
                new_version,
            });
        }

        tracing::info!(%prev_version, %new_version, "Upgrading workspace");

        while current_version != new_version {
            tracing::info!(
                "Upgrading from version {} to {}",
                current_version,
                new_version
            );

            match current_version {
                WorkspaceVersion::V0_Initial => self.upgrade_0_to_1(),
                WorkspaceVersion::V1_WorkspaceCacheDir => self.upgrade_1_to_2(),
                WorkspaceVersion::V2_DatasetConfig => self.upgrade_2_to_3(),
                WorkspaceVersion::V3_SavepointCreatedAt => self.upgrade_3_to_4(),
                WorkspaceVersion::V4_SavepointZeroCopy => self.upgrade_4_to_5(),
                WorkspaceVersion::V5_BreakingMetadataChanges => self.upgrade_5_to_6().await,
                WorkspaceVersion::V6_DatasetRepositoryUnification => self.upgrade_6_to_7(),
                WorkspaceVersion::V7_NoSummaryFiles => {
                    panic!("Already of latest version")
                }
                WorkspaceVersion::Unknown(_) => {
                    Err(WorkspaceFutureVersionError::new(current_version, new_version).into())
                }
            }?;

            current_version = current_version.next();
            std::fs::write(
                &self.workspace_layout.version_path,
                current_version.to_string(),
            )
            .int_err()?;
        }

        Ok(WorkspaceUpgradeResult {
            prev_version,
            new_version,
        })
    }

    fn upgrade_0_to_1(&self) -> Result<(), WorkspaceUpgradeError> {
        for entry in self.workspace_layout.datasets_dir.read_dir().int_err()? {
            let dataset_dir = entry.int_err()?;
            let cache_dir = dataset_dir.path().join("cache");
            if cache_dir.is_dir() {
                tracing::info!(?cache_dir, "Deleting old dataset ingest cache directory");
                std::fs::remove_dir_all(cache_dir).int_err()?;
            }
        }
        let _ = std::fs::create_dir(&self.workspace_layout.cache_dir);
        Ok(())
    }

    fn upgrade_1_to_2(&self) -> Result<(), WorkspaceUpgradeError> {
        for entry in self.workspace_layout.datasets_dir.read_dir().int_err()? {
            let dataset_dir = entry.int_err()?;
            let dataset_config_path = dataset_dir.path().join("config");
            if dataset_config_path.is_file() {
                tracing::info!(
                    ?dataset_config_path,
                    "Moving DatasetConfig file from old location"
                );

                let dataset_info_dir = dataset_dir.path().join("info");
                if !dataset_info_dir.exists() {
                    std::fs::create_dir(dataset_info_dir).int_err()?;
                }

                let new_dataset_config_path = dataset_dir.path().join("info/config");
                std::fs::rename(dataset_config_path, new_dataset_config_path).int_err()?;
            }
        }
        Ok(())
    }

    fn upgrade_2_to_3(&self) -> Result<(), WorkspaceUpgradeError> {
        if self.workspace_layout.cache_dir.exists() {
            tracing::info!("Clearing the cache directory");

            for res in self.workspace_layout.cache_dir.read_dir().int_err()? {
                let entry = res.int_err()?;
                if entry.path().is_dir() {
                    std::fs::remove_dir_all(entry.path()).int_err()?;
                } else {
                    std::fs::remove_file(entry.path()).int_err()?;
                }
            }
        }
        Ok(())
    }

    fn upgrade_3_to_4(&self) -> Result<(), WorkspaceUpgradeError> {
        if self.workspace_layout.cache_dir.exists() {
            tracing::info!("Clearing the cache directory");

            for res in self.workspace_layout.cache_dir.read_dir().int_err()? {
                let entry = res.int_err()?;
                if entry.path().is_dir() {
                    std::fs::remove_dir_all(entry.path()).int_err()?;
                } else {
                    std::fs::remove_file(entry.path()).int_err()?;
                }
            }
        }
        Ok(())
    }

    fn upgrade_4_to_5(&self) -> Result<(), WorkspaceUpgradeError> {
        Err(WorkspaceUpgradeImpossibleError::new(
            "This version of kamu contains major compatibility breaking changes in metadata and \
             data schemas. In an effort to continue evolving the protocol we made a decision to \
             forgo an expensive and long transitional period and introduced these changes without \
             a migration procedure. Please delete `.kamu` directory manually and re-create your \
             workspace. We apologise for the inconvenience and will work on improving stability \
             of our releases.",
        )
        .into())
    }

    async fn upgrade_5_to_6(&self) -> Result<(), WorkspaceUpgradeError> {
        if self.workspace_layout.datasets_dir.exists() {
            if self.is_multi_tenant_workspace() {
                for r_account_dir in self.workspace_layout.datasets_dir.read_dir().int_err()? {
                    let account_dir_entry = r_account_dir.int_err()?;
                    if let Some(s) = account_dir_entry.file_name().to_str()
                        && s.starts_with('.')
                    {
                        continue;
                    }
                    if !account_dir_entry.path().is_dir() {
                        continue;
                    }

                    let read_dataset_dir = std::fs::read_dir(account_dir_entry.path()).int_err()?;
                    for r_dataset_dir in read_dataset_dir {
                        let dataset_dir_entry = r_dataset_dir.int_err()?;
                        if let Some(s) = dataset_dir_entry.file_name().to_str()
                            && s.starts_with('.')
                        {
                            continue;
                        }

                        let dataset_current_path = dataset_dir_entry.path();
                        let dataset_id = self.read_dataset_id_from(&dataset_current_path).await?;
                        let dataset_alias =
                            self.read_dataset_alias_from(&dataset_current_path).await?;
                        let migrated_path =
                            self.move_dataset_to_v6_location(&dataset_current_path, &dataset_id)?;

                        std::fs::write(
                            migrated_path.join("info/alias"),
                            format!(
                                "{}/{}",
                                account_dir_entry.file_name().to_str().unwrap(),
                                dataset_alias.dataset_name
                            ),
                        )
                        .int_err()?;
                    }

                    std::fs::remove_dir(account_dir_entry.path()).int_err()?;
                }
            } else {
                for res in self.workspace_layout.datasets_dir.read_dir().int_err()? {
                    let dataset_dir_entry = res.int_err()?;
                    if let Some(s) = dataset_dir_entry.file_name().to_str()
                        && s.starts_with('.')
                    {
                        continue;
                    }

                    let dataset_current_path = dataset_dir_entry.path();
                    let dataset_id = self.read_dataset_id_from(&dataset_current_path).await?;
                    let migrated_path =
                        self.move_dataset_to_v6_location(&dataset_current_path, &dataset_id)?;

                    std::fs::write(
                        migrated_path.join("info/alias"),
                        dataset_dir_entry.file_name().to_str().unwrap(),
                    )
                    .int_err()?;
                }
            }
        }

        Ok(())
    }

    async fn read_dataset_id_from(
        &self,
        dataset_dir_path: &std::path::Path,
    ) -> Result<odf::DatasetID, InternalError> {
        let dataset = odf::dataset::DatasetFactoryImpl::get_local_fs(
            odf::dataset::DatasetLayout::new(dataset_dir_path),
        );

        use odf::dataset::{Dataset, MetadataChainExt};
        let mut seed_visitor = odf::dataset::SearchSeedVisitor::new();
        dataset
            .as_metadata_chain()
            .accept(&mut [&mut seed_visitor])
            .await
            .int_err()?;

        let seed = seed_visitor.into_event().unwrap();
        Ok(seed.dataset_id)
    }

    async fn read_dataset_alias_from(
        &self,
        dataset_dir_path: &std::path::Path,
    ) -> Result<odf::DatasetAlias, InternalError> {
        let dataset = odf::dataset::DatasetFactoryImpl::get_local_fs(
            odf::dataset::DatasetLayout::new(dataset_dir_path),
        );
        odf::dataset::read_dataset_alias(&dataset).await
    }

    fn move_dataset_to_v6_location(
        &self,
        existing_path: &Path,
        dataset_id: &odf::DatasetID,
    ) -> Result<std::path::PathBuf, InternalError> {
        let migrated_path = self
            .workspace_layout
            .datasets_dir
            .as_path()
            .join(dataset_id.as_multibase().to_stack_string());

        std::fs::rename(existing_path, &migrated_path).int_err()?;

        Ok(migrated_path)
    }

    fn upgrade_6_to_7(&self) -> Result<(), WorkspaceUpgradeError> {
        // Remove all summary files
        for entry in self.workspace_layout.datasets_dir.read_dir().int_err()? {
            let dataset_dir = entry.int_err()?;
            let summary_path = dataset_dir.path().join("info").join("summary");

            std::fs::remove_file(summary_path).int_err()?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkspaceUpgradeResult {
    pub prev_version: WorkspaceVersion,
    pub new_version: WorkspaceVersion,
}

#[derive(thiserror::Error, Debug)]
pub enum WorkspaceUpgradeError {
    #[error(transparent)]
    FutureVersion(#[from] WorkspaceFutureVersionError),
    #[error(transparent)]
    UpgradeImpossible(#[from] WorkspaceUpgradeImpossibleError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("{message}")]
pub struct WorkspaceUpgradeImpossibleError {
    message: String,
}

impl WorkspaceUpgradeImpossibleError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error(
    "Workspace version {workspace_version} is newer than supported version \
     {latest_supported_version} - upgrade to latest software version"
)]
pub struct WorkspaceFutureVersionError {
    pub workspace_version: WorkspaceVersion,
    pub latest_supported_version: WorkspaceVersion,
}

impl WorkspaceFutureVersionError {
    pub fn new(
        workspace_version: WorkspaceVersion,
        latest_supported_version: WorkspaceVersion,
    ) -> Self {
        Self {
            workspace_version,
            latest_supported_version,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
