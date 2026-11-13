// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::testing::CurrentAccountSubjectTestHelper;
use kamu_accounts::{CurrentAccountSubject, Principal};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_datasets::DatasetPermission;
use pretty_assertions::{assert_eq, assert_matches};

use crate::harness::BaseOsoRebacHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_facade_checks_the_given_principal_not_the_current_account() {
    let harness =
        RebacDatasetRegistryFacadeHarness::new(CurrentAccountSubjectTestHelper::logged("owner"))
            .await;
    let owned = harness.create_private_dataset("owner", "owned").await;
    let stranger = BaseOsoRebacHarness::user("stranger");

    assert_matches!(
        harness.resolve_handle_as(&stranger, &owned).await,
        Err(RebacDatasetRefUnresolvedError::Access(_))
    );
    assert_eq!(
        harness.accessible_refs_as(&stranger, &owned).await,
        Vec::<odf::DatasetRef>::new()
    );

    assert_matches!(
        harness.resolve_handle_as(&Principal::Node, &owned).await,
        Ok(handle) if handle == owned
    );
    assert_eq!(
        harness.accessible_refs_as(&Principal::Node, &owned).await,
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
    /// `current_account_subject` is the catalog's subject; each call names its
    /// principal on its own
    async fn new(current_account_subject: CurrentAccountSubject) -> Self {
        let base =
            BaseOsoRebacHarness::new(BaseOsoRebacHarness::predefined_accounts(&ACCOUNTS), |b| {
                current_account_subject.register(b);
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

    async fn resolve_handle_as(
        &self,
        principal: &Principal,
        dataset: &odf::DatasetHandle,
    ) -> Result<odf::DatasetHandle, RebacDatasetRefUnresolvedError> {
        self.rebac_dataset_registry_facade
            .resolve_dataset_handle_by_ref(
                principal,
                &dataset.as_local_ref(),
                DatasetPermission::Read,
            )
            .await
    }

    /// The refs among `[dataset]` the principal may read
    async fn accessible_refs_as(
        &self,
        principal: &Principal,
        dataset: &odf::DatasetHandle,
    ) -> Vec<odf::DatasetRef> {
        self.rebac_dataset_registry_facade
            .classify_dataset_refs_by_allowance(
                principal,
                &[&dataset.as_local_ref()],
                DatasetPermission::Read,
            )
            .await
            .unwrap()
            .accessible_resolved_refs
            .into_iter()
            .map(|(dataset_ref, _)| dataset_ref)
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
