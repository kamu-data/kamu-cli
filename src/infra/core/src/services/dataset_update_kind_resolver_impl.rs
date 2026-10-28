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
    pull_source_resolver: Arc<dyn DatasetPullSourceResolver>,
}

#[async_trait::async_trait]
impl DatasetUpdateKindResolver for DatasetUpdateKindResolverImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(?target))]
    async fn resolve_update_kind(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<DatasetUpdateKind, ResolveDatasetUpdateKindError> {
        if let Some(remote_ref) = self.pull_source_resolver.find_pull_alias(target).await? {
            return Ok(DatasetUpdateKind::SyncFrom { remote_ref });
        }

        Ok(match target.kind {
            odf::DatasetKind::Root => DatasetUpdateKind::PollingIngest,
            odf::DatasetKind::Derivative => DatasetUpdateKind::Transform,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
