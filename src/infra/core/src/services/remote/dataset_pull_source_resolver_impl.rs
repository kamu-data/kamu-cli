// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};
use tokio_stream::StreamExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetPullSourceResolver)]
pub struct DatasetPullSourceResolverImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
    remote_dataset_url_resolver: Arc<dyn RemoteDatasetUrlResolver>,
}

impl DatasetPullSourceResolverImpl {
    async fn has_pull_alias(
        &self,
        handle: &odf::DatasetHandle,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<bool, InternalError> {
        Ok(self
            .remote_alias_registry
            .get_remote_aliases(handle)
            .await
            .int_err()?
            .contains(remote_ref, RemoteAliasKind::Pull))
    }
}

#[async_trait::async_trait]
impl DatasetPullSourceResolver for DatasetPullSourceResolverImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(?target))]
    async fn has_pull_source(&self, target: &odf::DatasetHandle) -> Result<bool, InternalError> {
        let remote_aliases = match self.remote_alias_registry.get_remote_aliases(target).await {
            Ok(remote_aliases) => remote_aliases,
            Err(GetAliasesError::Internal(e)) => return Err(e),
        };
        Ok(!remote_aliases.is_empty(RemoteAliasKind::Pull))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?target))]
    async fn find_pull_alias(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<odf::DatasetRefRemote>, FindPullAliasError> {
        let remote_aliases = match self.remote_alias_registry.get_remote_aliases(target).await {
            Ok(remote_aliases) => remote_aliases,
            Err(GetAliasesError::Internal(e)) => return Err(e.into()),
        };

        match remote_aliases.single_by_kind(RemoteAliasKind::Pull) {
            Ok(maybe_alias) => Ok(maybe_alias.cloned()),
            Err(AmbiguousRemoteAliasError { .. }) => Err(AmbiguousPullSourceError.into()),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?target))]
    async fn resolve_pull_source(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<DatasetPullSource>, ResolvePullSourceError> {
        let Some(remote_ref) = self.find_pull_alias(target).await? else {
            return Ok(None);
        };
        Ok(Some(self.resolve_remote_source(&remote_ref).await?))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?remote_ref))]
    async fn resolve_remote_source(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<DatasetPullSource, ResolveRemoteSourceError> {
        let url = self
            .remote_dataset_url_resolver
            .resolve_dataset_url(remote_ref)
            .await?;
        Ok(DatasetPullSource {
            remote_ref: remote_ref.clone(),
            url,
        })
    }

    // TODO: avoid traversing all datasets for every alias
    #[tracing::instrument(level = "debug", skip_all, fields(?remote_ref))]
    async fn find_dataset_by_pull_alias(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<Option<odf::DatasetHandle>, InternalError> {
        // Quick check of the local dataset named as the remote one
        if let Some(remote_name) = remote_ref.dataset_name()
            && let Some(local_handle) = self
                .dataset_registry
                .try_resolve_dataset_handle_by_ref(
                    &odf::DatasetAlias::new(None, remote_name.clone()).as_local_ref(),
                )
                .await?
            && self.has_pull_alias(&local_handle, remote_ref).await?
        {
            return Ok(Some(local_handle));
        }

        let CurrentAccountSubject::Logged(account) = self.current_account_subject.as_ref() else {
            return Ok(None);
        };

        let mut datasets = self
            .dataset_registry
            .all_dataset_handles_by_owner_name(&account.account_handle.name);
        while let Some(dataset_handle) = datasets.next().await {
            let dataset_handle = dataset_handle?;
            if self.has_pull_alias(&dataset_handle, remote_ref).await? {
                return Ok(Some(dataset_handle));
            }
        }

        Ok(None)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
