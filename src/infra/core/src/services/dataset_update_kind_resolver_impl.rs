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
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetUpdateKindResolver)]
pub struct DatasetUpdateKindResolverImpl {
    remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
}

#[async_trait::async_trait]
impl DatasetUpdateKindResolver for DatasetUpdateKindResolverImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(?target))]
    async fn resolve_update_kind(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<DatasetUpdateKind, ResolveDatasetUpdateKindError> {
        let remote_aliases = match self.remote_alias_registry.get_remote_aliases(target).await {
            Ok(remote_aliases) => remote_aliases,
            Err(GetAliasesError::Internal(e)) => return Err(e.into()),
        };

        let mut pull_aliases: Vec<_> = remote_aliases.get_by_kind(RemoteAliasKind::Pull).collect();

        match pull_aliases.len() {
            0 => Ok(match target.kind {
                odf::DatasetKind::Root => DatasetUpdateKind::PollingIngest,
                odf::DatasetKind::Derivative => DatasetUpdateKind::Transform,
            }),
            1 => Ok(DatasetUpdateKind::SyncFrom {
                remote_ref: pull_aliases.remove(0).clone(),
            }),
            _ => Err(ResolveDatasetUpdateKindError::AmbiguousSource),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
