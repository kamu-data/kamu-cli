// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dill::*;
use futures::TryStreamExt;
use kamu_core::*;
use opendatafabric::*;
use url::Url;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetRepositoryLocalFs {
    storage_strategy: Box<dyn DatasetStorageStrategy>,
    thrash_lock: tokio::sync::Mutex<()>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetRepositoryLocalFs {
    pub fn new(
        root: PathBuf,
        current_account_config: Arc<CurrentAccountConfig>,
        multitenant: bool,
    ) -> Self {
        Self::from(root, current_account_config, multitenant)
    }

    pub fn from(
        root: impl Into<PathBuf>,
        current_account_config: Arc<CurrentAccountConfig>,
        multitenant: bool,
    ) -> Self {
        Self {
            storage_strategy: if multitenant {
                Box::new(DatasetMultiTenantStorageStrategy::new(
                    root.into(),
                    current_account_config,
                ))
            } else {
                Box::new(DatasetSingleTenantStorageStrategy::new(root.into()))
            },
            thrash_lock: tokio::sync::Mutex::new(()),
        }
    }

    // TODO: Make dataset factory (and thus the hashing algo) configurable
    fn get_dataset_impl(
        &self,
        dataset_handle: &DatasetHandle,
    ) -> Result<impl Dataset, InternalError> {
        let layout = DatasetLayout::new(self.storage_strategy.get_dataset_path(&dataset_handle));
        Ok(DatasetFactoryImpl::get_local_fs(layout))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRegistry for DatasetRepositoryLocalFs {
    async fn get_dataset_url(&self, dataset_ref: &DatasetRef) -> Result<Url, GetDatasetUrlError> {
        let handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset_path = self.storage_strategy.get_dataset_path(&handle);
        Ok(Url::from_directory_path(dataset_path).unwrap())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRepository for DatasetRepositoryLocalFs {
    fn is_multitenant(&self) -> bool {
        self.storage_strategy.is_multitenant()
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
                .resolve_dataset_alias(&alias)
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
    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s> {
        self.storage_strategy.get_all_datasets()
    }

    fn get_account_datasets<'s>(&'s self, account_name: AccountName) -> DatasetHandleStream<'s> {
        self.storage_strategy.get_account_datasets(account_name)
    }

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        let handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self.get_dataset_impl(&handle)?;
        Ok(Arc::new(dataset))
    }

    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        let dataset_id = seed_block.event.dataset_id.clone();
        let dataset_handle = DatasetHandle::new(dataset_id, dataset_alias.clone());
        let dataset_path = self.storage_strategy.get_dataset_path(&dataset_handle);

        let layout = DatasetLayout::create(&dataset_path).int_err()?;

        let dataset = DatasetFactoryImpl::get_local_fs(layout);

        // There are three possiblities at this point:
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
            Ok(head) => Ok(head),
            Err(AppendError::RefCASFailed(_)) => {
                Err(CreateDatasetError::NameCollision(NameCollisionError {
                    alias: dataset_alias.clone(),
                }))
            }
            Err(err) => Err(err.int_err().into()),
        }?;

        self.storage_strategy
            .handle_dataset_created(&dataset, &dataset_handle.alias.dataset_name)
            .await?;

        Ok(CreateDatasetResult {
            dataset_handle,
            dataset: Arc::new(dataset),
            head,
        })
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

        let children: Vec<_> = get_downstream_dependencies_impl(self, dataset_ref)
            .try_collect()
            .await?;

        if !children.is_empty() {
            return Err(DanglingReferenceError {
                dataset_handle,
                children,
            }
            .into());
        }

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
        Ok(())
    }

    fn get_downstream_dependencies<'s>(
        &'s self,
        dataset_ref: &'s DatasetRef,
    ) -> DatasetHandleStream<'s> {
        Box::pin(get_downstream_dependencies_impl(self, dataset_ref))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
trait DatasetStorageStrategy: Sync + Send {
    fn is_multitenant(&self) -> bool;

    fn get_dataset_path(&self, dataset_handle: &DatasetHandle) -> PathBuf;

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s>;

    fn get_account_datasets<'s>(&'s self, account_name: AccountName) -> DatasetHandleStream<'s>;

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
        dataset_name: &DatasetName,
    ) -> Result<(), InternalError>;

    async fn handle_dataset_renamed(
        &self,
        dataset_handle: &DatasetHandle,
        new_name: &DatasetName,
    ) -> Result<(), InternalError>;
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

/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetSingleTenantStorageStrategy {
    root: PathBuf,
}

impl DatasetSingleTenantStorageStrategy {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn dataset_name<'a>(&self, dataset_alias: &'a DatasetAlias) -> &'a DatasetName {
        if dataset_alias.is_multitenant() {
            panic!("Multitenant alias applied to singletenant repository")
        } else {
            &dataset_alias.dataset_name
        }
    }

    fn dataset_path_impl(&self, dataset_alias: &DatasetAlias) -> PathBuf {
        let dataset_name = self.dataset_name(&dataset_alias);
        self.root.join(dataset_name)
    }

    async fn attempt_resolving_summary_via_path(
        &self,
        dataset_path: &PathBuf,
        dataset_alias: &DatasetAlias,
    ) -> Result<DatasetSummary, ResolveDatasetError> {
        let layout = DatasetLayout::new(dataset_path);
        let dataset = DatasetFactoryImpl::get_local_fs(layout);
        dataset
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
            })
    }
}

#[async_trait]
impl DatasetStorageStrategy for DatasetSingleTenantStorageStrategy {
    fn is_multitenant(&self) -> bool {
        false
    }

    fn get_dataset_path(&self, dataset_handle: &DatasetHandle) -> PathBuf {
        self.dataset_path_impl(&dataset_handle.alias)
    }

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            let read_dataset_dir = std::fs::read_dir(&self.root).int_err()?;

            for r_dataset_dir in read_dataset_dir {
                let dataset_dir_entry = r_dataset_dir.int_err()?;
                if let Some(s) = dataset_dir_entry.file_name().to_str() {
                    if s.starts_with(".") {
                        continue;
                    }
                }
                let dataset_name = DatasetName::try_from(&dataset_dir_entry.file_name()).int_err()?;
                let dataset_alias = DatasetAlias::new(None, dataset_name);
                match self.resolve_dataset_alias(&dataset_alias).await {
                    Ok(hdl) => { yield hdl; Ok(()) }
                    Err(ResolveDatasetError::NotFound(_)) => Ok(()),
                    Err(e) => Err(e.int_err())
                }?;
            }
        })
    }

    fn get_account_datasets<'s>(&'s self, _account_name: AccountName) -> DatasetHandleStream<'s> {
        unreachable!("Singletenant dataset repository queried by account");
    }

    async fn resolve_dataset_alias(
        &self,
        dataset_alias: &DatasetAlias,
    ) -> Result<DatasetHandle, ResolveDatasetError> {
        let dataset_path = self.dataset_path_impl(&dataset_alias);
        if !dataset_path.exists() {
            return Err(ResolveDatasetError::NotFound(DatasetNotFoundError {
                dataset_ref: dataset_alias.as_local_ref(),
            }));
        }
        let summary = self
            .attempt_resolving_summary_via_path(&dataset_path, dataset_alias)
            .await?;

        Ok(DatasetHandle::new(summary.id, dataset_alias.clone()))
    }

    async fn resolve_dataset_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetHandle, ResolveDatasetError> {
        let read_dataset_dir = std::fs::read_dir(&self.root).int_err()?;

        for r_dataset_dir in read_dataset_dir {
            let dataset_dir_entry = r_dataset_dir.int_err()?;
            if let Some(s) = dataset_dir_entry.file_name().to_str() {
                if s.starts_with(".") {
                    continue;
                }
            }

            let alias = DatasetAlias::new(
                None,
                DatasetName::try_from(&dataset_dir_entry.file_name()).int_err()?,
            );

            let dataset_path = self.dataset_path_impl(&alias);

            let summary = self
                .attempt_resolving_summary_via_path(&dataset_path, &alias)
                .await?;

            if summary.id == *dataset_id {
                return Ok(DatasetHandle::new(summary.id, alias));
            }
        }

        Err(ResolveDatasetError::NotFound(DatasetNotFoundError {
            dataset_ref: dataset_id.as_local_ref(),
        }))
    }

    async fn handle_dataset_created(
        &self,
        _dataset: &dyn Dataset,
        _dataset_name: &DatasetName,
    ) -> Result<(), InternalError> {
        // No extra action required in singletenant mode
        Ok(())
    }

    async fn handle_dataset_renamed(
        &self,
        dataset_handle: &DatasetHandle,
        new_name: &DatasetName,
    ) -> Result<(), InternalError> {
        let old_dataset_path = self.get_dataset_path(dataset_handle);
        let new_dataset_path = old_dataset_path.parent().unwrap().join(&new_name);
        std::fs::rename(old_dataset_path, new_dataset_path).int_err()?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetMultiTenantStorageStrategy {
    root: PathBuf,
    current_account_config: Arc<CurrentAccountConfig>,
}

impl DatasetMultiTenantStorageStrategy {
    pub fn new(
        root: impl Into<PathBuf>,
        current_account_config: Arc<CurrentAccountConfig>,
    ) -> Self {
        Self {
            root: root.into(),
            current_account_config,
        }
    }

    fn effective_account_name<'a>(&'a self, dataset_alias: &'a DatasetAlias) -> &'a AccountName {
        if dataset_alias.is_multitenant() {
            dataset_alias.account_name.as_ref().unwrap()
        } else {
            &self.current_account_config.account_name
        }
    }

    async fn attempt_resolving_dataset_name_via_path(
        &self,
        dataset_path: &PathBuf,
        dataset_id: &DatasetID,
    ) -> Result<DatasetName, ResolveDatasetError> {
        let layout = DatasetLayout::new(dataset_path);
        let dataset = DatasetFactoryImpl::get_local_fs(layout);
        match dataset.as_info_repo().get("alias").await {
            Ok(bytes) => {
                let dataset_name_str = std::str::from_utf8(&bytes[..]).int_err()?.trim();
                let dataset_name = DatasetName::try_from(dataset_name_str).int_err()?;
                Ok(dataset_name)
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

    async fn save_dataset_name(
        &self,
        dataset: &dyn Dataset,
        dataset_name: &DatasetName,
    ) -> Result<(), InternalError> {
        dataset
            .as_info_repo()
            .set("alias", dataset_name.as_bytes())
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
                    if s.starts_with(".") {
                        continue;
                    }
                }
                let dataset_file_name = String::from(dataset_dir_entry.file_name().to_str().unwrap());

                let dataset_id =
                    DatasetID::from_did_string(format!("did:odf:{}", dataset_file_name).as_str())
                        .int_err()?;

                let dataset_path = dataset_dir_entry.path();

                match self
                    .attempt_resolving_dataset_name_via_path(&dataset_path, &dataset_id)
                    .await
                {
                    Ok(dataset_name) => {
                        let dataset_alias = DatasetAlias::new(Some(account_name.clone()), dataset_name);
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
}

#[async_trait]
impl DatasetStorageStrategy for DatasetMultiTenantStorageStrategy {
    fn is_multitenant(&self) -> bool {
        true
    }

    fn get_dataset_path(&self, dataset_handle: &DatasetHandle) -> PathBuf {
        let account_name = self.effective_account_name(&dataset_handle.alias);

        self.root
            .join(account_name)
            .join(dataset_handle.id.cid.to_string())
    }

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            let read_account_dir = std::fs::read_dir(&self.root).int_err()?;

            for r_account_dir in read_account_dir {
                let account_dir_entry = r_account_dir.int_err()?;
                if let Some(s) = account_dir_entry.file_name().to_str() {
                    if s.starts_with(".") {
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

    fn get_account_datasets<'s>(&'s self, account_name: AccountName) -> DatasetHandleStream<'s> {
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

        let read_dataset_dir =
            std::fs::read_dir(self.root.join(effective_account_name)).int_err()?;

        for r_dataset_dir in read_dataset_dir {
            let dataset_dir_entry = r_dataset_dir.int_err()?;
            if let Some(s) = dataset_dir_entry.file_name().to_str() {
                if s.starts_with(".") {
                    continue;
                }
            }

            let dataset_file_name = String::from(dataset_dir_entry.file_name().to_str().unwrap());

            let dataset_id =
                DatasetID::from_did_string(format!("did:odf:{}", dataset_file_name).as_str())
                    .int_err()?;

            let dataset_path = dataset_dir_entry.path();

            let dataset_name = self
                .attempt_resolving_dataset_name_via_path(&dataset_path, &dataset_id)
                .await?;

            if dataset_name == dataset_alias.dataset_name {
                return Ok(DatasetHandle::new(dataset_id, dataset_alias.to_owned()));
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
        let id_as_string = dataset_id.to_string();
        let read_account_dir = std::fs::read_dir(&self.root).int_err()?;

        for r_account_dir in read_account_dir {
            let account_dir_entry = r_account_dir.int_err()?;
            if let Some(s) = account_dir_entry.file_name().to_str() {
                if s.starts_with(".") {
                    continue;
                }
            }

            let dataset_path = account_dir_entry.path().join(id_as_string.clone());
            if dataset_path.exists() {
                let dataset_name = self
                    .attempt_resolving_dataset_name_via_path(&dataset_path, dataset_id)
                    .await?;

                let dataset_alias = DatasetAlias::new(
                    Some(AccountName::try_from(&account_dir_entry.file_name()).int_err()?),
                    dataset_name,
                );
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
        dataset_name: &DatasetName,
    ) -> Result<(), InternalError> {
        self.save_dataset_name(dataset, dataset_name).await
    }

    async fn handle_dataset_renamed(
        &self,
        dataset_handle: &DatasetHandle,
        new_name: &DatasetName,
    ) -> Result<(), InternalError> {
        let dataset_path = self.get_dataset_path(dataset_handle);
        let layout = DatasetLayout::new(dataset_path);
        let dataset = DatasetFactoryImpl::get_local_fs(layout);

        self.save_dataset_name(&dataset, new_name).await?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
