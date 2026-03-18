// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dill::CatalogBuilder;
use kamu_auth_rebac::{
    AccountProperties,
    AccountPropertyName,
    AccountToDatasetRelation,
    AuthorizedAccount,
    AuthorizedDataset,
    DatasetProperties,
    DatasetPropertyName,
    EntityWithRelation,
    RebacService,
    Relation,
    SetAccountDatasetRelationsOperation,
    UnsetAccountDatasetRelationsOperation,
    boolean_property_value,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::*;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Harness = RebacServiceImplHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_account_property_changed_vs_unchanged() {
    let harness = Harness::new();
    let account_id = odf::AccountID::new_generated_ed25519().1;

    let first = harness
        .rebac_service
        .set_account_property(
            &account_id,
            AccountPropertyName::IsAdmin,
            &boolean_property_value(true),
        )
        .await
        .unwrap();
    let second = harness
        .rebac_service
        .set_account_property(
            &account_id,
            AccountPropertyName::IsAdmin,
            &boolean_property_value(true),
        )
        .await
        .unwrap();

    assert!(first.is_changed());
    assert!(!second.is_changed());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unset_account_property_changed_vs_unchanged() {
    let harness = Harness::new();
    let account_id = odf::AccountID::new_generated_ed25519().1;

    let _ = harness
        .rebac_service
        .set_account_property(
            &account_id,
            AccountPropertyName::CanProvisionAccounts,
            &boolean_property_value(true),
        )
        .await
        .unwrap();

    let first = harness
        .rebac_service
        .unset_account_property(&account_id, AccountPropertyName::CanProvisionAccounts)
        .await
        .unwrap();
    let second = harness
        .rebac_service
        .unset_account_property(&account_id, AccountPropertyName::CanProvisionAccounts)
        .await
        .unwrap();

    assert!(first.is_changed());
    assert!(!second.is_changed());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_dataset_property_changed_vs_unchanged() {
    let harness = Harness::new();
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;

    let first = harness
        .rebac_service
        .set_dataset_property(
            &dataset_id,
            DatasetPropertyName::AllowsPublicRead,
            &boolean_property_value(true),
        )
        .await
        .unwrap();
    let second = harness
        .rebac_service
        .set_dataset_property(
            &dataset_id,
            DatasetPropertyName::AllowsPublicRead,
            &boolean_property_value(true),
        )
        .await
        .unwrap();

    assert!(first.is_changed());
    assert!(!second.is_changed());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unset_dataset_property_changed_vs_unchanged() {
    let harness = Harness::new();
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;

    let _ = harness
        .rebac_service
        .set_dataset_property(
            &dataset_id,
            DatasetPropertyName::AllowsAnonymousRead,
            &boolean_property_value(true),
        )
        .await
        .unwrap();

    let first = harness
        .rebac_service
        .unset_dataset_property(&dataset_id, DatasetPropertyName::AllowsAnonymousRead)
        .await
        .unwrap();
    let second = harness
        .rebac_service
        .unset_dataset_property(&dataset_id, DatasetPropertyName::AllowsAnonymousRead)
        .await
        .unwrap();

    assert!(first.is_changed());
    assert!(!second.is_changed());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_dataset_properties_changed_vs_unchanged() {
    let harness = Harness::new();
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;

    let _ = harness
        .rebac_service
        .set_dataset_property(
            &dataset_id,
            DatasetPropertyName::AllowsPublicRead,
            &boolean_property_value(true),
        )
        .await
        .unwrap();

    let first = harness
        .rebac_service
        .delete_dataset_properties(&dataset_id)
        .await
        .unwrap();
    let second = harness
        .rebac_service
        .delete_dataset_properties(&dataset_id)
        .await
        .unwrap();

    assert!(first.is_changed());
    assert!(!second.is_changed());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_properties_queries_and_count() {
    let harness = Harness::new();
    let account_id = odf::AccountID::new_generated_ed25519().1;
    let dataset_id_1 = odf::DatasetID::new_generated_ed25519().1;
    let dataset_id_2 = odf::DatasetID::new_generated_ed25519().1;

    assert_eq!(harness.rebac_service.properties_count().await.unwrap(), 0);
    assert_eq!(
        harness
            .rebac_service
            .get_account_properties(&account_id)
            .await
            .unwrap(),
        AccountProperties::default()
    );
    assert_eq!(
        harness
            .rebac_service
            .get_dataset_properties(&dataset_id_1)
            .await
            .unwrap(),
        DatasetProperties::default()
    );

    let _ = harness
        .rebac_service
        .set_account_property(
            &account_id,
            AccountPropertyName::CanProvisionAccounts,
            &boolean_property_value(true),
        )
        .await
        .unwrap();
    let _ = harness
        .rebac_service
        .set_dataset_property(
            &dataset_id_1,
            DatasetPropertyName::AllowsPublicRead,
            &boolean_property_value(true),
        )
        .await
        .unwrap();
    let _ = harness
        .rebac_service
        .set_dataset_property(
            &dataset_id_1,
            DatasetPropertyName::AllowsAnonymousRead,
            &boolean_property_value(true),
        )
        .await
        .unwrap();

    assert_eq!(harness.rebac_service.properties_count().await.unwrap(), 3);
    assert_eq!(
        harness
            .rebac_service
            .get_account_properties(&account_id)
            .await
            .unwrap(),
        AccountProperties {
            is_admin: false,
            can_provision_accounts: true,
        }
    );
    assert_eq!(
        harness
            .rebac_service
            .get_dataset_properties(&dataset_id_1)
            .await
            .unwrap(),
        DatasetProperties {
            allows_anonymous_read: true,
            allows_public_read: true,
        }
    );
    assert_eq!(
        harness
            .rebac_service
            .get_dataset_properties_by_ids(&[dataset_id_1.clone(), dataset_id_2.clone()])
            .await
            .unwrap(),
        HashMap::from([
            (
                dataset_id_1,
                DatasetProperties {
                    allows_anonymous_read: true,
                    allows_public_read: true,
                },
            ),
            (dataset_id_2, DatasetProperties::default()),
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_relation_mutations_changed_vs_unchanged() {
    let harness = Harness::new();
    let account_id_1 = odf::AccountID::new_generated_ed25519().1;
    let account_id_2 = odf::AccountID::new_generated_ed25519().1;
    let dataset_id_1 = odf::DatasetID::new_generated_ed25519().1;
    let dataset_id_2 = odf::DatasetID::new_generated_ed25519().1;

    let first_set = harness
        .rebac_service
        .set_account_dataset_relation(
            &account_id_1,
            AccountToDatasetRelation::Reader,
            &dataset_id_1,
        )
        .await
        .unwrap();
    let second_set = harness
        .rebac_service
        .set_account_dataset_relation(
            &account_id_1,
            AccountToDatasetRelation::Reader,
            &dataset_id_1,
        )
        .await
        .unwrap();

    assert_eq!(
        first_set.changed_dataset_ids,
        HashSet::from([dataset_id_1.clone()])
    );
    assert!(second_set.is_empty());

    let bulk_set = harness
        .rebac_service
        .set_account_dataset_relations(&[
            SetAccountDatasetRelationsOperation {
                account_id: Cow::Borrowed(&account_id_1),
                relationship: AccountToDatasetRelation::Reader,
                dataset_id: Cow::Borrowed(&dataset_id_1),
            },
            SetAccountDatasetRelationsOperation {
                account_id: Cow::Borrowed(&account_id_2),
                relationship: AccountToDatasetRelation::Maintainer,
                dataset_id: Cow::Borrowed(&dataset_id_2),
            },
        ])
        .await
        .unwrap();

    assert_eq!(
        bulk_set.changed_dataset_ids,
        HashSet::from([dataset_id_2.clone()])
    );

    let unset_single = harness
        .rebac_service
        .unset_accounts_dataset_relations(&[&account_id_1], &dataset_id_1)
        .await
        .unwrap();
    let unset_single_again = harness
        .rebac_service
        .unset_accounts_dataset_relations(&[&account_id_1], &dataset_id_1)
        .await
        .unwrap();

    assert_eq!(
        unset_single.changed_dataset_ids,
        HashSet::from([dataset_id_1.clone()])
    );
    assert!(unset_single_again.is_empty());

    let bulk_unset = harness
        .rebac_service
        .unset_account_dataset_relations(&[
            UnsetAccountDatasetRelationsOperation {
                account_id: Cow::Borrowed(&account_id_2),
                dataset_id: Cow::Borrowed(&dataset_id_2),
            },
            UnsetAccountDatasetRelationsOperation {
                account_id: Cow::Borrowed(&account_id_1),
                dataset_id: Cow::Borrowed(&dataset_id_1),
            },
        ])
        .await
        .unwrap();

    assert_eq!(
        bulk_unset.changed_dataset_ids,
        HashSet::from([dataset_id_2])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_relation_queries_return_expected_results() {
    let harness = Harness::new();
    let account_id_1 = odf::AccountID::new_generated_ed25519().1;
    let account_id_2 = odf::AccountID::new_generated_ed25519().1;
    let dataset_id_1 = odf::DatasetID::new_generated_ed25519().1;
    let dataset_id_2 = odf::DatasetID::new_generated_ed25519().1;
    let dataset_id_3 = odf::DatasetID::new_generated_ed25519().1;

    let _ = harness
        .rebac_service
        .set_account_dataset_relations(&[
            SetAccountDatasetRelationsOperation {
                account_id: Cow::Borrowed(&account_id_1),
                relationship: AccountToDatasetRelation::Reader,
                dataset_id: Cow::Borrowed(&dataset_id_1),
            },
            SetAccountDatasetRelationsOperation {
                account_id: Cow::Borrowed(&account_id_1),
                relationship: AccountToDatasetRelation::Maintainer,
                dataset_id: Cow::Borrowed(&dataset_id_2),
            },
            SetAccountDatasetRelationsOperation {
                account_id: Cow::Borrowed(&account_id_2),
                relationship: AccountToDatasetRelation::Reader,
                dataset_id: Cow::Borrowed(&dataset_id_2),
            },
        ])
        .await
        .unwrap();

    let mut account_relations = harness
        .rebac_service
        .get_account_dataset_relations(&account_id_1)
        .await
        .unwrap();
    account_relations.sort_by_key(|entry| entry.entity.entity_id.to_string());
    let mut expected_account_relations = vec![
        EntityWithRelation::new_dataset(
            dataset_id_1.to_string(),
            Relation::AccountToDataset(AccountToDatasetRelation::Reader),
        ),
        EntityWithRelation::new_dataset(
            dataset_id_2.to_string(),
            Relation::AccountToDataset(AccountToDatasetRelation::Maintainer),
        ),
    ];
    expected_account_relations.sort_by_key(|entry| entry.entity.entity_id.to_string());

    assert_eq!(account_relations, expected_account_relations);

    let mut authorized_datasets_by_account = harness
        .rebac_service
        .get_authorized_datasets_by_account_ids(&[&account_id_1, &account_id_2])
        .await
        .unwrap();
    RebacServiceImplHarness::sort_authorized_datasets_map(&mut authorized_datasets_by_account);
    let mut expected_authorized_datasets_by_account = HashMap::from([
        (
            account_id_1.clone(),
            vec![
                AuthorizedDataset {
                    dataset_id: dataset_id_1.clone(),
                    role: AccountToDatasetRelation::Reader,
                },
                AuthorizedDataset {
                    dataset_id: dataset_id_2.clone(),
                    role: AccountToDatasetRelation::Maintainer,
                },
            ],
        ),
        (
            account_id_2.clone(),
            vec![AuthorizedDataset {
                dataset_id: dataset_id_2.clone(),
                role: AccountToDatasetRelation::Reader,
            }],
        ),
    ]);
    RebacServiceImplHarness::sort_authorized_datasets_map(
        &mut expected_authorized_datasets_by_account,
    );

    assert_eq!(
        authorized_datasets_by_account,
        expected_authorized_datasets_by_account
    );

    let mut authorized_accounts = harness
        .rebac_service
        .get_authorized_accounts(&dataset_id_2)
        .await
        .unwrap();
    authorized_accounts.sort_by_key(|entry| entry.account_id.clone());
    let mut expected_authorized_accounts = vec![
        AuthorizedAccount {
            account_id: account_id_1.clone(),
            role: AccountToDatasetRelation::Maintainer,
        },
        AuthorizedAccount {
            account_id: account_id_2.clone(),
            role: AccountToDatasetRelation::Reader,
        },
    ];
    expected_authorized_accounts.sort_by_key(|entry| entry.account_id.clone());

    assert_eq!(authorized_accounts, expected_authorized_accounts);

    let mut authorized_accounts_by_dataset = harness
        .rebac_service
        .get_authorized_accounts_by_ids(&[dataset_id_1.clone(), dataset_id_2.clone(), dataset_id_3])
        .await
        .unwrap();
    RebacServiceImplHarness::sort_authorized_accounts_map(&mut authorized_accounts_by_dataset);
    let mut expected_authorized_accounts_by_dataset = HashMap::from([
        (
            dataset_id_1,
            vec![AuthorizedAccount {
                account_id: account_id_1.clone(),
                role: AccountToDatasetRelation::Reader,
            }],
        ),
        (
            dataset_id_2,
            vec![
                AuthorizedAccount {
                    account_id: account_id_1.clone(),
                    role: AccountToDatasetRelation::Maintainer,
                },
                AuthorizedAccount {
                    account_id: account_id_2,
                    role: AccountToDatasetRelation::Reader,
                },
            ],
        ),
    ]);
    RebacServiceImplHarness::sort_authorized_accounts_map(
        &mut expected_authorized_accounts_by_dataset,
    );

    assert_eq!(
        authorized_accounts_by_dataset,
        expected_authorized_accounts_by_dataset
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RebacServiceImplHarness {
    rebac_service: Arc<dyn RebacService>,
}

impl RebacServiceImplHarness {
    fn new() -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add::<RebacServiceImpl>();
            b.add_value(DefaultAccountProperties::default());
            b.add_value(DefaultDatasetProperties::default());
            b.add::<InMemoryRebacRepository>();

            b.build()
        };

        Self {
            rebac_service: catalog.get_one().unwrap(),
        }
    }

    fn sort_authorized_datasets_map(
        authorized_datasets_by_account: &mut HashMap<odf::AccountID, Vec<AuthorizedDataset>>,
    ) {
        for authorized_datasets in authorized_datasets_by_account.values_mut() {
            authorized_datasets.sort_by_key(|entry| entry.dataset_id.clone());
        }
    }

    fn sort_authorized_accounts_map(
        authorized_accounts_by_dataset: &mut HashMap<odf::DatasetID, Vec<AuthorizedAccount>>,
    ) {
        for authorized_accounts in authorized_accounts_by_dataset.values_mut() {
            authorized_accounts.sort_by_key(|entry| entry.account_id.clone());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
