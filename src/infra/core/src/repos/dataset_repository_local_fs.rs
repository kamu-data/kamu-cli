// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use dill::*;
use domain::auth::{DatasetAction, DatasetActionAuthorizer};
use event_bus::EventBus;
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME_STR};
use kamu_core::*;
use opendatafabric::*;
use url::Url;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetRepositoryLocalFs {
    current_account_subject: Arc<CurrentAccountSubject>,
    storage_strategy: Box<dyn DatasetStorageStrategy>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    event_bus: Arc<EventBus>,
    thrash_lock: tokio::sync::Mutex<()>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetRepositoryLocalFs {
    pub fn new(
        root: PathBuf,
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        event_bus: Arc<EventBus>,
        multi_tenant: bool,
        system_time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            current_account_subject: current_account_subject.clone(),
            storage_strategy: if multi_tenant {
                Box::new(DatasetMultiTenantStorageStrategy::new(
                    root,
                    current_account_subject,
                    event_bus.clone(),
                ))
            } else {
                Box::new(DatasetSingleTenantStorageStrategy::new(
                    root,
                    event_bus.clone(),
                ))
            },
            dataset_action_authorizer,
            dependency_graph_service,
            event_bus,
            thrash_lock: tokio::sync::Mutex::new(()),
            system_time_source,
        }
    }

    pub fn create(
        root: impl Into<PathBuf>,
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        event_bus: Arc<EventBus>,
        multi_tenant: bool,
        system_time_source: Arc<dyn SystemTimeSource>,
    ) -> Result<Self, std::io::Error> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self::new(
            root,
            current_account_subject,
            dataset_action_authorizer,
            dependency_graph_service,
            event_bus,
            multi_tenant,
            system_time_source,
        ))
    }

    fn build_dataset(layout: DatasetLayout, event_bus: Arc<EventBus>) -> Arc<dyn Dataset> {
        Arc::new(DatasetImpl::new(
            event_bus,
            MetadataChainImpl::new(
                MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                    ObjectRepositoryLocalFSSha3::new(layout.blocks_dir),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(layout.refs_dir)),
            ),
            ObjectRepositoryLocalFSSha3::new(layout.data_dir),
            ObjectRepositoryLocalFSSha3::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(layout.info_dir),
        ))
    }

    // TODO: Make dataset factory (and thus the hashing algo) configurable
    fn get_dataset_impl(&self, dataset_handle: &DatasetHandle) -> Arc<dyn Dataset> {
        let layout = DatasetLayout::new(self.storage_strategy.get_dataset_path(dataset_handle));
        Self::build_dataset(layout, self.event_bus.clone())
    }

    // TODO: Used only for testing, but should be removed it in future to discourage
    // file-based access
    pub async fn get_dataset_layout(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetLayout, GetDatasetError> {
        let dataset_handle = self.resolve_dataset_ref(dataset_ref).await?;
        Ok(DatasetLayout::new(
            self.storage_strategy.get_dataset_path(&dataset_handle),
        ))
    }

    fn get_canonical_path_param(dataset_path: &Path) -> Result<(PathBuf, String), InternalError> {
        let canonical_dataset_path = std::fs::canonicalize(dataset_path).int_err()?;
        let dataset_name_str = canonical_dataset_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        Ok((canonical_dataset_path, dataset_name_str))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRegistry for DatasetRepositoryLocalFs {
    async fn get_dataset_url(&self, dataset_ref: &DatasetRef) -> Result<Url, GetDatasetUrlError> {
        let dataset_handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset_path = self.storage_strategy.get_dataset_path(&dataset_handle);
        Ok(Url::from_directory_path(dataset_path).unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRepository for DatasetRepositoryLocalFs {
    fn is_multi_tenant(&self) -> bool {
        self.storage_strategy.is_multi_tenant()
    }

    // TODO: PERF: Cache data and speed up lookups by ID
    //
    // TODO: CONCURRENCY: Since resolving ID to Name currently requires accessing
    // all summaries multiple threads calling this function or iterating all
    // datasets can result in significant thrashing. We use a lock here until we
    // have a better solution.
    //
    // Note that this lock does not prevent concurrent updates to summaries, only
    // reduces the chances of it.
    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError> {
        // Anti-thrashing lock (see comment above)
        let _lock_guard = self.thrash_lock.lock().await;

        match dataset_ref {
            DatasetRef::Handle(h) => Ok(h.clone()),
            DatasetRef::Alias(alias) => self
                .storage_strategy
                .resolve_dataset_alias(alias)
                .await
                .map_err(|e| match e {
                    ResolveDatasetError::Internal(e) => GetDatasetError::Internal(e),
                    ResolveDatasetError::NotFound(e) => GetDatasetError::NotFound(e),
                }),
            DatasetRef::ID(id) => self
                .storage_strategy
                .resolve_dataset_id(id)
                .await
                .map_err(|e| match e {
                    ResolveDatasetError::Internal(e) => GetDatasetError::Internal(e),
                    ResolveDatasetError::NotFound(e) => GetDatasetError::NotFound(e),
                }),
        }
    }

    // TODO: PERF: Resolving handles currently involves reading summary files
    fn get_all_datasets(&self) -> DatasetHandleStream<'_> {
        self.storage_strategy.get_all_datasets()
    }

    fn get_datasets_by_owner(&self, account_name: &AccountName) -> DatasetHandleStream<'_> {
        self.storage_strategy.get_datasets_by_owner(account_name)
    }

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        let dataset_handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self.get_dataset_impl(&dataset_handle);
        Ok(dataset)
    }

    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        // Check if a dataset with the same alias can be resolved successfully
        let maybe_existing_dataset_handle = match self
            .resolve_dataset_ref(&dataset_alias.as_local_ref())
            .await
        {
            Ok(existing_handle) => Ok(Some(existing_handle)),
            Err(GetDatasetError::NotFound(_)) => Ok(None),
            Err(GetDatasetError::Internal(e)) => Err(e),
        }?;

        // If so, there are 2 possibilities:
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with name
        //   collision
        if let Some(existing_dataset_handle) = maybe_existing_dataset_handle {
            let existing_dataset = self
                .get_dataset(&existing_dataset_handle.as_local_ref())
                .await
                .int_err()?;

            match existing_dataset
                .as_metadata_chain()
                .resolve_ref(&BlockRef::Head)
                .await
            {
                // Existing head
                Ok(_) => {
                    return Err(CreateDatasetError::NameCollision(NameCollisionError {
                        alias: dataset_alias.clone(),
                    }));
                }

                // No head, so continue creating
                Err(GetRefError::NotFound(_)) => {}

                // Errors...
                Err(GetRefError::Access(e)) => {
                    return Err(CreateDatasetError::Internal(e.int_err()))
                }
                Err(GetRefError::Internal(e)) => return Err(CreateDatasetError::Internal(e)),
            }
        }

        // It's okay to create a new dataset by this point
        let dataset_handle = DatasetHandle::new(
            seed_block.event.dataset_id.clone(),
            self.storage_strategy
                .canonical_dataset_alias(dataset_alias)
                .int_err()?,
        );

        let dataset_path = self.storage_strategy.get_dataset_path(&dataset_handle);
        let layout = DatasetLayout::create(&dataset_path).int_err()?;
        let dataset = Self::build_dataset(layout, self.event_bus.clone());

        // There are three possibilities at this point:
        // - Dataset did not exist before - continue normally
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with name
        //   collision
        let head = match dataset
            .as_metadata_chain()
            .append(
                seed_block.into(),
                AppendOpts {
                    // We are using head ref CAS to detect previous existence of a dataset
                    // as atomically as possible
                    check_ref_is: Some(None),
                    ..AppendOpts::default()
                },
            )
            .await
        {
            Ok(head) => head,
            Err(err) => {
                return Err(match err {
                    AppendError::RefCASFailed(_) => {
                        CreateDatasetError::RefCollision(RefCollisionError {
                            id: dataset_handle.id,
                        })
                    }
                    _ => err.int_err().into(),
                })
            }
        };

        self.storage_strategy
            .handle_dataset_created(dataset.as_ref(), &dataset_handle.alias)
            .await?;

        tracing::info!(
            id = %dataset_handle.id,
            alias = %dataset_handle.alias,
            %head,
            "Created new dataset",
        );

        self.event_bus
            .dispatch_event(events::DatasetEventCreated {
                dataset_id: dataset_handle.id.clone(),
                owner_account_id: match self.current_account_subject.as_ref() {
                    CurrentAccountSubject::Anonymous(_) => {
                        panic!("Anonymous account cannot create dataset");
                    }
                    CurrentAccountSubject::Logged(l) => l.account_id.clone(),
                },
            })
            .await?;

        Ok(CreateDatasetResult {
            dataset_handle,
            dataset,
            head,
        })
    }

    async fn create_dataset_from_snapshot(
        &self,
        snapshot: DatasetSnapshot,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        create_dataset_from_snapshot_impl(
            self,
            self.event_bus.as_ref(),
            snapshot,
            self.system_time_source.now(),
        )
        .await
    }

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRef,
        new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError> {
        let dataset_handle = self.resolve_dataset_ref(dataset_ref).await?;

        let new_alias =
            DatasetAlias::new(dataset_handle.alias.account_name.clone(), new_name.clone());
        match self
            .storage_strategy
            .resolve_dataset_alias(&new_alias)
            .await
        {
            Ok(_) => Err(RenameDatasetError::NameCollision(NameCollisionError {
                alias: DatasetAlias::new(
                    dataset_handle.alias.account_name.clone(),
                    new_name.clone(),
                ),
            })),
            Err(ResolveDatasetError::Internal(e)) => Err(RenameDatasetError::Internal(e)),
            Err(ResolveDatasetError::NotFound(_)) => Ok(()),
        }?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Write)
            .await?;

        self.storage_strategy
            .handle_dataset_renamed(&dataset_handle, new_name)
            .await?;

        Ok(())
    }

    // TODO: PERF: Need fast inverse dependency lookup
    async fn delete_dataset(&self, dataset_ref: &DatasetRef) -> Result<(), DeleteDatasetError> {
        let dataset_handle = match self.resolve_dataset_ref(dataset_ref).await {
            Ok(h) => Ok(h),
            Err(GetDatasetError::NotFound(e)) => Err(DeleteDatasetError::NotFound(e)),
            Err(GetDatasetError::Internal(e)) => Err(DeleteDatasetError::Internal(e)),
        }?;

        use tokio_stream::StreamExt;
        let downstream_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_downstream_dependencies(&dataset_handle.id)
            .await
            .int_err()?
            .collect()
            .await;

        if !downstream_dataset_ids.is_empty() {
            let mut children = Vec::with_capacity(downstream_dataset_ids.len());
            for downstream_dataset_id in downstream_dataset_ids {
                let hdl = self
                    .resolve_dataset_ref(&downstream_dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                children.push(hdl);
            }

            return Err(DanglingReferenceError {
                dataset_handle,
                children,
            }
            .into());
        }

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Write)
            .await?;

        // // Update repo info
        // let mut repo_info = self.read_repo_info().await?;
        // let index = repo_info
        //     .datasets
        //     .iter()
        //     .position(|d| d.name == dataset_handle.name)
        //     .ok_or("Inconsistent repository info")
        //     .int_err()?;
        // repo_info.datasets.remove(index);
        // self.write_repo_info(repo_info).await?;

        let dataset_dir = self.storage_strategy.get_dataset_path(&dataset_handle);
        tokio::fs::remove_dir_all(dataset_dir).await.int_err()?;

        self.event_bus
            .dispatch_event(events::DatasetEventDeleted {
                dataset_id: dataset_handle.id,
            })
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
trait DatasetStorageStrategy: Sync + Send {
    fn is_multi_tenant(&self) -> bool;

    fn get_dataset_path(&self, dataset_handle: &DatasetHandle) -> PathBuf;

    fn get_all_datasets(&self) -> DatasetHandleStream<'_>;

    fn get_datasets_by_owner(&self, account_name: &AccountName) -> DatasetHandleStream<'_>;

    async fn resolve_dataset_alias(
        &self,
        dataset_alias: &DatasetAlias,
    ) -> Result<DatasetHandle, ResolveDatasetError>;

    async fn resolve_dataset_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetHandle, ResolveDatasetError>;

    async fn handle_dataset_created(
        &self,
        dataset: &dyn Dataset,
        dataset_alias: &DatasetAlias,
    ) -> Result<(), InternalError>;

    async fn handle_dataset_renamed(
        &self,
        dataset_handle: &DatasetHandle,
        new_name: &DatasetName,
    ) -> Result<(), InternalError>;

    fn canonical_dataset_alias(
        &self,
        raw_alias: &DatasetAlias,
    ) -> Result<DatasetAlias, ResolveDatasetError>;
}

#[derive(thiserror::Error, Debug)]
enum ResolveDatasetError {
    #[error(transparent)]
    NotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetSingleTenantStorageStrategy {
    root: PathBuf,
    event_bus: Arc<EventBus>,
}

impl DatasetSingleTenantStorageStrategy {
    pub fn new(root: impl Into<PathBuf>, event_bus: Arc<EventBus>) -> Self {
        Self {
            root: root.into(),
            event_bus,
        }
    }

    fn dataset_name<'a>(&self, dataset_alias: &'a DatasetAlias) -> &'a DatasetName {
        &dataset_alias.dataset_name
    }

    fn dataset_path_impl(&self, dataset_alias: &DatasetAlias) -> PathBuf {
        let dataset_name = self.dataset_name(dataset_alias);
        self.root.join(dataset_name)
    }

    async fn attempt_resolving_summary_via_path(
        &self,
        dataset_path: &PathBuf,
        dataset_alias: &DatasetAlias,
    ) -> Result<(DatasetSummary, DatasetAlias), ResolveDatasetError> {
        let layout = DatasetLayout::new(dataset_path);
        let dataset = DatasetRepositoryLocalFs::build_dataset(layout, self.event_bus.clone());

        let dataset_summary = dataset
            .get_summary(GetSummaryOpts::default())
            .await
            .map_err(|e| {
                if let GetSummaryError::EmptyDataset = e {
                    ResolveDatasetError::NotFound(DatasetNotFoundError {
                        dataset_ref: dataset_alias.as_local_ref(),
                    })
                } else {
                    ResolveDatasetError::Internal(e.int_err())
                }
            })?;

        let (_, canonical_dataset_name) =
            DatasetRepositoryLocalFs::get_canonical_path_param(dataset_path)?;
        let canonical_dataset_alias = DatasetAlias {
            dataset_name: DatasetName::new_unchecked(canonical_dataset_name.as_str()),
            account_name: None,
        };

        Ok((dataset_summary, canonical_dataset_alias))
    }

    async fn resolve_dataset_handle(
        &self,
        dataset_path: &PathBuf,
        dataset_alias: &DatasetAlias,
    ) -> Result<DatasetHandle, ResolveDatasetError> {
        let (summary, canonical_dataset_alias) = self
            .attempt_resolving_summary_via_path(dataset_path, dataset_alias)
            .await?;

        Ok(DatasetHandle::new(
            summary.id,
            canonical_dataset_alias.clone(),
        ))
    }
}

#[async_trait]
impl DatasetStorageStrategy for DatasetSingleTenantStorageStrategy {
    fn is_multi_tenant(&self) -> bool {
        false
    }

    fn get_dataset_path(&self, dataset_handle: &DatasetHandle) -> PathBuf {
        self.dataset_path_impl(&dataset_handle.alias)
    }

    fn get_all_datasets(&self) -> DatasetHandleStream<'_> {
        Box::pin(async_stream::try_stream! {
            let read_dataset_dir = std::fs::read_dir(&self.root).int_err()?;

            for r_dataset_dir in read_dataset_dir {
                let dataset_dir_entry = r_dataset_dir.int_err()?;
                if let Some(s) = dataset_dir_entry.file_name().to_str() {
                    if s.starts_with('.') {
                        continue;
                    }
                }
                let dataset_name = DatasetName::try_from(&dataset_dir_entry.file_name()).int_err()?;
                let dataset_alias = DatasetAlias::new(None, dataset_name);
                match self.resolve_dataset_handle(&dataset_dir_entry.path(), &dataset_alias).await {
                    Ok(hdl) => { yield hdl; Ok(()) }
                    Err(ResolveDatasetError::NotFound(_)) => Ok(()),
                    Err(e) => Err(e.int_err())
                }?;
            }
        })
    }

    fn get_datasets_by_owner(&self, account_name: &AccountName) -> DatasetHandleStream<'_> {
        if *account_name == DEFAULT_ACCOUNT_NAME_STR {
            self.get_all_datasets()
        } else {
            Box::pin(futures::stream::empty())
        }
    }

    async fn resolve_dataset_alias(
        &self,
        dataset_alias: &DatasetAlias,
    ) -> Result<DatasetHandle, ResolveDatasetError> {
        assert!(
            !dataset_alias.is_multi_tenant()
                || dataset_alias.account_name.as_ref().unwrap() == DEFAULT_ACCOUNT_NAME_STR,
            "Multi-tenant refs shouldn't have reached down to here with earlier validations"
        );

        let dataset_path = self.dataset_path_impl(dataset_alias);
        if !dataset_path.exists() {
            use tokio_stream::StreamExt;

            let mut all_datasets_stream = self.get_all_datasets();

            while let Some(dataset_handle) = all_datasets_stream.try_next().await.unwrap() {
                if &dataset_handle.alias == dataset_alias {
                    return self
                        .resolve_dataset_handle(
                            &self.root.join(&dataset_handle.alias.dataset_name),
                            &dataset_handle.alias,
                        )
                        .await;
                }
            }
            return Err(ResolveDatasetError::NotFound(DatasetNotFoundError {
                dataset_ref: dataset_alias.as_local_ref(),
            }));
        }

        self.resolve_dataset_handle(&dataset_path, dataset_alias)
            .await
    }

    async fn resolve_dataset_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetHandle, ResolveDatasetError> {
        let read_dataset_dir = std::fs::read_dir(&self.root).int_err()?;

        for r_dataset_dir in read_dataset_dir {
            let dataset_dir_entry = r_dataset_dir.int_err()?;
            if let Some(s) = dataset_dir_entry.file_name().to_str() {
                if s.starts_with('.') {
                    continue;
                }
            }

            let alias = DatasetAlias::new(
                None,
                DatasetName::try_from(&dataset_dir_entry.file_name()).int_err()?,
            );

            let dataset_path = self.dataset_path_impl(&alias);

            let (summary, canonical_dataset_alias) = self
                .attempt_resolving_summary_via_path(&dataset_path, &alias)
                .await?;

            if summary.id == *dataset_id {
                return Ok(DatasetHandle::new(summary.id, canonical_dataset_alias));
            }
        }

        Err(ResolveDatasetError::NotFound(DatasetNotFoundError {
            dataset_ref: dataset_id.as_local_ref(),
        }))
    }

    async fn handle_dataset_created(
        &self,
        _dataset: &dyn Dataset,
        _dataset_alias: &DatasetAlias,
    ) -> Result<(), InternalError> {
        // No extra action required in single-tenant mode
        Ok(())
    }

    async fn handle_dataset_renamed(
        &self,
        dataset_handle: &DatasetHandle,
        new_name: &DatasetName,
    ) -> Result<(), InternalError> {
        let old_dataset_path = self.get_dataset_path(dataset_handle);
        let new_dataset_path = old_dataset_path.parent().unwrap().join(new_name);
        std::fs::rename(old_dataset_path, new_dataset_path).int_err()?;
        Ok(())
    }

    fn canonical_dataset_alias(
        &self,
        raw_alias: &DatasetAlias,
    ) -> Result<DatasetAlias, ResolveDatasetError> {
        Ok(raw_alias.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetMultiTenantStorageStrategy {
    root: PathBuf,
    current_account_subject: Arc<CurrentAccountSubject>,
    event_bus: Arc<EventBus>,
}

impl DatasetMultiTenantStorageStrategy {
    pub fn new(
        root: impl Into<PathBuf>,
        current_account_subject: Arc<CurrentAccountSubject>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            root: root.into(),
            current_account_subject,
            event_bus,
        }
    }

    fn effective_account_name<'a>(&'a self, dataset_alias: &'a DatasetAlias) -> &'a AccountName {
        if dataset_alias.is_multi_tenant() {
            dataset_alias.account_name.as_ref().unwrap()
        } else {
            match self.current_account_subject.as_ref() {
                CurrentAccountSubject::Anonymous(_) => {
                    panic!("Anonymous account misused, use multi-tenant alias");
                }
                CurrentAccountSubject::Logged(l) => &l.account_name,
            }
        }
    }

    async fn attempt_resolving_dataset_alias_via_path(
        &self,
        account_name: &AccountName,
        dataset_path: &PathBuf,
        dataset_id: &DatasetID,
    ) -> Result<DatasetAlias, ResolveDatasetError> {
        let layout = DatasetLayout::new(dataset_path);
        let dataset = DatasetRepositoryLocalFs::build_dataset(layout, self.event_bus.clone());
        match dataset.as_info_repo().get("alias").await {
            Ok(bytes) => {
                let dataset_alias_str = std::str::from_utf8(&bytes[..]).int_err()?.trim();
                let mut dataset_alias = DatasetAlias::try_from(dataset_alias_str).int_err()?;
                if !dataset_alias.is_multi_tenant() {
                    dataset_alias =
                        DatasetAlias::new(Some(account_name.clone()), dataset_alias.dataset_name);
                }
                Ok(dataset_alias)
            }
            Err(GetNamedError::Internal(e)) => Err(ResolveDatasetError::Internal(e)),
            Err(GetNamedError::Access(e)) => Err(ResolveDatasetError::Internal(e.int_err())),
            Err(GetNamedError::NotFound(_)) => {
                Err(ResolveDatasetError::NotFound(DatasetNotFoundError {
                    dataset_ref: dataset_id.as_local_ref(),
                }))
            }
        }
    }

    async fn save_dataset_alias(
        &self,
        dataset: &dyn Dataset,
        dataset_alias: &DatasetAlias,
    ) -> Result<(), InternalError> {
        dataset
            .as_info_repo()
            .set("alias", dataset_alias.to_string().as_bytes())
            .await
            .int_err()?;

        Ok(())
    }

    fn stream_account_datasets<'s>(
        &'s self,
        account_name: &'s AccountName,
        account_dir_path: PathBuf,
    ) -> DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            let read_dataset_dir = std::fs::read_dir(account_dir_path).int_err()?;

            for r_dataset_dir in read_dataset_dir {
                let dataset_dir_entry = r_dataset_dir.int_err()?;
                if let Some(s) = dataset_dir_entry.file_name().to_str() {
                    if s.starts_with('.') {
                        continue;
                    }
                }
                let dataset_file_name = String::from(dataset_dir_entry.file_name().to_str().unwrap());

                let dataset_id =
                    DatasetID::from_did_str(format!("did:odf:{dataset_file_name}").as_str())
                        .int_err()?;

                let dataset_path = dataset_dir_entry.path();

                match self
                    .attempt_resolving_dataset_alias_via_path(account_name, &dataset_path, &dataset_id)
                    .await
                {
                    Ok(dataset_alias) => {
                        let dataset_handle = DatasetHandle::new(dataset_id, dataset_alias);
                        yield dataset_handle;
                        Ok(())
                    }
                    Err(ResolveDatasetError::NotFound(_)) => Ok(()),
                    Err(e) => Err(e.int_err()),
                }?;
            }
        })
    }

    fn resolve_account_dir(
        &self,
        account_name: &AccountName,
    ) -> Result<(PathBuf, AccountName), ResolveDatasetError> {
        let account_dataset_dir_path = self.root.join(account_name);

        if !account_dataset_dir_path.is_dir() {
            let read_account_dirs = std::fs::read_dir(self.root.as_path()).int_err()?;

            for read_account_dir in read_account_dirs {
                let account_dir_name = AccountName::new_unchecked(
                    read_account_dir
                        .int_err()?
                        .file_name()
                        .to_str()
                        .unwrap_or(""),
                );
                if account_name == &account_dir_name {
                    return Ok((self.root.join(&account_dir_name), account_dir_name));
                }
            }
            return Ok((account_dataset_dir_path, account_name.clone()));
        }

        let (canonical_account_dataset_dir_path, canonical_account_name) =
            DatasetRepositoryLocalFs::get_canonical_path_param(&account_dataset_dir_path)?;

        Ok((
            canonical_account_dataset_dir_path,
            AccountName::new_unchecked(canonical_account_name.as_str()),
        ))
    }
}

#[async_trait]
impl DatasetStorageStrategy for DatasetMultiTenantStorageStrategy {
    fn is_multi_tenant(&self) -> bool {
        true
    }

    fn get_dataset_path(&self, dataset_handle: &DatasetHandle) -> PathBuf {
        let account_name = self.effective_account_name(&dataset_handle.alias);

        self.root
            .join(account_name)
            .join(dataset_handle.id.as_multibase().to_stack_string())
    }

    fn get_all_datasets(&self) -> DatasetHandleStream<'_> {
        Box::pin(async_stream::try_stream! {
            let read_account_dir = std::fs::read_dir(&self.root).int_err()?;

            for r_account_dir in read_account_dir {
                let account_dir_entry = r_account_dir.int_err()?;
                if let Some(s) = account_dir_entry.file_name().to_str() {
                    if s.starts_with('.') {
                        continue;
                    }
                }
                if !account_dir_entry.path().is_dir() {
                    continue;
                }

                let account_name = AccountName::try_from(&account_dir_entry.file_name()).int_err()?;
                let mut account_datasets = self.stream_account_datasets(&account_name, account_dir_entry.path());

                use tokio_stream::StreamExt;
                while let Some(dataset_handle) = account_datasets.next().await {
                    yield dataset_handle?;
                }
            }
        })
    }

    fn get_datasets_by_owner(&self, account_name: &AccountName) -> DatasetHandleStream<'_> {
        let account_name = account_name.clone();
        Box::pin(async_stream::try_stream! {
            let account_dir_path = self.root.join(account_name.clone());
            if !account_dir_path.is_dir() {
                return;
            }

            let mut account_datasets = self.stream_account_datasets(&account_name, account_dir_path);

            use tokio_stream::StreamExt;
            while let Some(dataset_handle) = account_datasets.next().await {
                yield dataset_handle?;
            }
        })
    }

    async fn resolve_dataset_alias(
        &self,
        dataset_alias: &DatasetAlias,
    ) -> Result<DatasetHandle, ResolveDatasetError> {
        let effective_account_name = self.effective_account_name(dataset_alias);
        let (account_dataset_dir_path, _) = self.resolve_account_dir(effective_account_name)?;

        if account_dataset_dir_path.is_dir() {
            let read_dataset_dir = std::fs::read_dir(account_dataset_dir_path).map_err(|_| {
                ResolveDatasetError::NotFound(DatasetNotFoundError {
                    dataset_ref: dataset_alias.as_local_ref(),
                })
            })?;

            for r_dataset_dir in read_dataset_dir {
                let dataset_dir_entry = r_dataset_dir.int_err()?;
                if let Some(s) = dataset_dir_entry.file_name().to_str() {
                    if s.starts_with('.') {
                        continue;
                    }
                }

                let dataset_file_name =
                    String::from(dataset_dir_entry.file_name().to_str().unwrap());

                let dataset_id =
                    DatasetID::from_did_str(format!("did:odf:{dataset_file_name}").as_str())
                        .int_err()?;

                let dataset_path = dataset_dir_entry.path();

                let candidate_dataset_alias = self
                    .attempt_resolving_dataset_alias_via_path(
                        effective_account_name,
                        &dataset_path,
                        &dataset_id,
                    )
                    .await?;

                if candidate_dataset_alias.dataset_name == dataset_alias.dataset_name {
                    return Ok(DatasetHandle::new(dataset_id, candidate_dataset_alias));
                }
            }
        }

        Err(ResolveDatasetError::NotFound(DatasetNotFoundError {
            dataset_ref: dataset_alias.as_local_ref(),
        }))
    }

    async fn resolve_dataset_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetHandle, ResolveDatasetError> {
        let id_as_string = dataset_id.as_multibase().to_stack_string();
        let read_account_dir = std::fs::read_dir(&self.root).int_err()?;

        for r_account_dir in read_account_dir {
            let account_dir_entry = r_account_dir.int_err()?;
            if let Some(s) = account_dir_entry.file_name().to_str() {
                if s.starts_with('.') {
                    continue;
                }
            }

            let account_name = AccountName::try_from(&account_dir_entry.file_name()).int_err()?;
            let dataset_path = account_dir_entry.path().join(&id_as_string);
            if dataset_path.exists() {
                let dataset_alias = self
                    .attempt_resolving_dataset_alias_via_path(
                        &account_name,
                        &dataset_path,
                        dataset_id,
                    )
                    .await?;

                return Ok(DatasetHandle::new(dataset_id.to_owned(), dataset_alias));
            }
        }

        Err(ResolveDatasetError::NotFound(DatasetNotFoundError {
            dataset_ref: dataset_id.as_local_ref(),
        }))
    }

    async fn handle_dataset_created(
        &self,
        dataset: &dyn Dataset,
        dataset_alias: &DatasetAlias,
    ) -> Result<(), InternalError> {
        self.save_dataset_alias(dataset, dataset_alias).await
    }

    async fn handle_dataset_renamed(
        &self,
        dataset_handle: &DatasetHandle,
        new_name: &DatasetName,
    ) -> Result<(), InternalError> {
        let dataset_path = self.get_dataset_path(dataset_handle);
        let layout = DatasetLayout::new(dataset_path);
        let dataset = DatasetRepositoryLocalFs::build_dataset(layout, self.event_bus.clone());

        let new_alias =
            DatasetAlias::new(dataset_handle.alias.account_name.clone(), new_name.clone());
        self.save_dataset_alias(dataset.as_ref(), &new_alias)
            .await?;

        Ok(())
    }

    fn canonical_dataset_alias(
        &self,
        raw_alias: &DatasetAlias,
    ) -> Result<DatasetAlias, ResolveDatasetError> {
        Ok(if let Some(account_name) = &raw_alias.account_name {
            let (_, canonical_account_name) = self.resolve_account_dir(account_name).int_err()?;
            DatasetAlias::new(Some(canonical_account_name), raw_alias.dataset_name.clone())
        } else {
            raw_alias.clone()
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
