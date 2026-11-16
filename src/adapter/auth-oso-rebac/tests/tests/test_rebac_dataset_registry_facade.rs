// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::Principal;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_datasets::DatasetPermission;
use pretty_assertions::{assert_eq, assert_matches};

use crate::harness::BaseOsoRebacHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_facade_checks_the_principal_of_its_catalog() {
    let stranger_harness =
        RebacDatasetRegistryFacadeHarness::new(BaseOsoRebacHarness::user("stranger")).await;
    let owned = stranger_harness
        .create_private_dataset("owner", "owned")
        .await;

    assert_matches!(
        stranger_harness.resolve_handle(&owned).await,
        Err(RebacDatasetRefUnresolvedError::Access(_))
    );
    assert_eq!(
        stranger_harness.accessible_refs(&owned).await,
        Vec::<odf::DatasetRef>::new()
    );

    let node_harness = RebacDatasetRegistryFacadeHarness::new(Principal::Node).await;
    let owned = node_harness.create_private_dataset("owner", "owned").await;

    assert_matches!(
        node_harness.resolve_handle(&owned).await,
        Ok(handle) if handle == owned
    );
    assert_eq!(
        node_harness.accessible_refs(&owned).await,
        vec![owned.as_local_ref()]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNTS: [&str; 2] = ["owner", "stranger"];

#[oop::extend(BaseOsoRebacHarness, base)]
struct RebacDatasetRegistryFacadeHarness {
    base: BaseOsoRebacHarness,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl RebacDatasetRegistryFacadeHarness {
    /// `principal` is the only principal of the catalog the facade is built in
    async fn new(principal: Principal) -> Self {
        let base =
            BaseOsoRebacHarness::new(BaseOsoRebacHarness::predefined_accounts(&ACCOUNTS), |b| {
                b.add_value(principal);
            })
            .await;

        let catalog = dill::CatalogBuilder::new_chained(base.catalog())
            .add::<RebacDatasetRegistryFacadeImpl>()
            .build();

        Self {
            rebac_dataset_registry_facade: catalog.get_one().unwrap(),
            base,
        }
    }

    async fn resolve_handle(
        &self,
        dataset: &odf::DatasetHandle,
    ) -> Result<odf::DatasetHandle, RebacDatasetRefUnresolvedError> {
        self.rebac_dataset_registry_facade
            .resolve_dataset_handle_by_ref(&dataset.as_local_ref(), DatasetPermission::Read)
            .await
    }

    /// The refs among `[dataset]` the catalog's principal may read
    async fn accessible_refs(&self, dataset: &odf::DatasetHandle) -> Vec<odf::DatasetRef> {
        self.rebac_dataset_registry_facade
            .classify_dataset_refs_by_allowance(&[&dataset.as_local_ref()], DatasetPermission::Read)
            .await
            .unwrap()
            .accessible_resolved_refs
            .into_iter()
            .map(|(dataset_ref, _)| dataset_ref)
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
