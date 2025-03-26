// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::utils::metadata_chain_comparator::{
    CompareChainsResult,
    MetadataChainComparator,
    NullCompareChainsListener,
};
use kamu_core::{
    DatasetPushStatuses,
    DatasetRegistry,
    PushStatus,
    RemoteAliasKind,
    RemoteAliasesRegistry,
    RemoteStatusService,
    StatusCheckError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteStatusServiceImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
}

#[component(pub)]
#[interface(dyn RemoteStatusService)]
impl RemoteStatusServiceImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_factory,
            remote_alias_reg,
        }
    }

    async fn status(
        &self,
        lhs_chain: &dyn odf::MetadataChain,
        lhs_head: &odf::Multihash,
        alias: &odf::DatasetRefRemote,
    ) -> Result<CompareChainsResult, StatusCheckError> {
        let url = alias.url().ok_or(StatusCheckError::Internal(
            "Couldn't figure out remote dataset location".int_err(),
        ))?;
        let Ok(rhs_ds) = self.dataset_factory.get_dataset(url, false).await else {
            return Err(StatusCheckError::Internal(
                "Couldn't figure out remote dataset location".int_err(),
            ));
        };
        let rhs_chain = rhs_ds.as_metadata_chain();
        let rhs_head = match rhs_chain.resolve_ref(&odf::BlockRef::Head).await {
            Ok(head) => head,
            Err(odf::GetRefError::Access(e)) => return Err(StatusCheckError::Access(e)),
            Err(odf::GetRefError::NotFound(_)) => {
                return Err(StatusCheckError::RemoteDatasetNotFound)
            }
            Err(e) => return Err(StatusCheckError::Internal(e.int_err())),
        };
        let result = match MetadataChainComparator::compare_chains(
            lhs_chain,
            lhs_head,
            rhs_chain,
            Some(&rhs_head),
            &NullCompareChainsListener,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => return Err(StatusCheckError::Internal(e.int_err())),
        };
        Ok(result)
    }
}

#[async_trait]
impl RemoteStatusService for RemoteStatusServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle))]
    async fn check_remotes_status(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<DatasetPushStatuses, InternalError> {
        let lhs_ds = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await?;
        let lhs_chain = lhs_ds.as_metadata_chain();
        let lhs_head = lhs_chain
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?;

        let aliases = self
            .remote_alias_reg
            .get_remote_aliases(dataset_handle)
            .await
            .int_err()?;
        let push_aliases: Vec<&odf::DatasetRefRemote> =
            aliases.get_by_kind(RemoteAliasKind::Push).collect();

        tracing::debug!(?push_aliases, "Fetched dataset remote push aliases");

        let mut statuses = vec![];

        for alias in push_aliases {
            statuses.push(PushStatus {
                remote: alias.clone(),
                check_result: self.status(lhs_chain, &lhs_head, alias).await,
            });
        }

        tracing::debug!(?statuses, "Determined push alias statuses");

        Ok(DatasetPushStatuses { statuses })
    }
}
