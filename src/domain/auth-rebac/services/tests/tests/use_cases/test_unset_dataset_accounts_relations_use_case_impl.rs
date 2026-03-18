// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::CatalogBuilder;
use kamu_auth_rebac::{
    AccountToDatasetRelation,
    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
    RebacDatasetRelationsMessage,
    RebacService,
    UnsetDatasetAccountsRelationsUseCase,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::*;
use messaging_outbox::{MockOutbox, Outbox};
use pretty_assertions::assert_matches;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Harness = UnsetDatasetAccountsRelationsUseCaseImplHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unset_existing_relation_emits_message() {
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;
    let account_id = odf::AccountID::new_generated_ed25519().1;
    let remaining_account_id = odf::AccountID::new_generated_ed25519().1;

    let harness = Harness::new_with_expected_modified_message(
        dataset_id.clone(),
        vec![(
            remaining_account_id.clone(),
            AccountToDatasetRelation::Maintainer,
        )],
    );

    let _ = harness
        .rebac_service
        .set_account_dataset_relation(&account_id, AccountToDatasetRelation::Reader, &dataset_id)
        .await
        .unwrap();
    let _ = harness
        .rebac_service
        .set_account_dataset_relation(
            &remaining_account_id,
            AccountToDatasetRelation::Maintainer,
            &dataset_id,
        )
        .await
        .unwrap();

    assert_matches!(
        harness.use_case.execute(&dataset_id, &[&account_id]).await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unset_partially_existing_relations_emits_message() {
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;
    let existing_account_id = odf::AccountID::new_generated_ed25519().1;
    let missing_account_id = odf::AccountID::new_generated_ed25519().1;
    let remaining_account_id = odf::AccountID::new_generated_ed25519().1;

    let harness = Harness::new_with_expected_modified_message(
        dataset_id.clone(),
        vec![(
            remaining_account_id.clone(),
            AccountToDatasetRelation::Maintainer,
        )],
    );

    let _ = harness
        .rebac_service
        .set_account_dataset_relation(
            &existing_account_id,
            AccountToDatasetRelation::Reader,
            &dataset_id,
        )
        .await
        .unwrap();
    let _ = harness
        .rebac_service
        .set_account_dataset_relation(
            &remaining_account_id,
            AccountToDatasetRelation::Maintainer,
            &dataset_id,
        )
        .await
        .unwrap();

    assert_matches!(
        harness
            .use_case
            .execute(&dataset_id, &[&existing_account_id, &missing_account_id])
            .await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unset_missing_relation_does_not_emit_message() {
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;
    let account_id = odf::AccountID::new_generated_ed25519().1;

    let harness = Harness::new(MockOutbox::new());

    assert_matches!(
        harness.use_case.execute(&dataset_id, &[&account_id]).await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct UnsetDatasetAccountsRelationsUseCaseImplHarness {
    use_case: Arc<dyn UnsetDatasetAccountsRelationsUseCase>,
    rebac_service: Arc<dyn RebacService>,
}

impl UnsetDatasetAccountsRelationsUseCaseImplHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_value(mock_outbox).bind::<dyn Outbox, MockOutbox>();
            b.add::<UnsetDatasetAccountsRelationsUseCaseImpl>();
            b.add::<RebacServiceImpl>();
            b.add_value(DefaultAccountProperties::default());
            b.add_value(DefaultDatasetProperties::default());
            b.add::<InMemoryRebacRepository>();

            b.build()
        };

        Self {
            use_case: catalog.get_one().unwrap(),
            rebac_service: catalog.get_one().unwrap(),
        }
    }

    fn new_with_expected_modified_message(
        dataset_id: odf::DatasetID,
        expected_accounts: Vec<(odf::AccountID, AccountToDatasetRelation)>,
    ) -> Self {
        let mut outbox = MockOutbox::new();
        outbox.expect_post_message_as_json().times(1).returning(
            move |producer_name, message_as_json, version| {
                assert_eq!(
                    producer_name,
                    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE
                );
                assert_eq!(version, 1);

                let mut expected_accounts = expected_accounts
                    .iter()
                    .map(|(account_id, role)| (account_id.clone(), *role))
                    .collect::<Vec<_>>();
                expected_accounts.sort_by_key(|(account_id, _)| account_id.clone());

                let message =
                    serde_json::from_value::<RebacDatasetRelationsMessage>(message_as_json.clone())
                        .unwrap();
                let RebacDatasetRelationsMessage::Modified(message) = message else {
                    panic!("unexpected message kind");
                };
                let mut actual_accounts = message
                    .authorized_accounts
                    .into_iter()
                    .map(|account| (account.account_id, account.role))
                    .collect::<Vec<_>>();
                actual_accounts.sort_by_key(|(account_id, _)| account_id.clone());

                assert_eq!(message.dataset_id, dataset_id);
                assert_eq!(actual_accounts, expected_accounts);
                Ok(())
            },
        );

        Self::new(outbox)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
