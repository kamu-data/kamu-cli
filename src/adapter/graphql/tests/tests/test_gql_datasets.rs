// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::NoOpDatabasePlugin;
use dill::Component;
use indoc::indoc;
use kamu::*;
use kamu_accounts::testing::MockAuthenticationService;
use kamu_accounts::*;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{MultiTenantRebacDatasetLifecycleMessageConsumer, RebacServiceImpl};
use kamu_core::*;
use kamu_datasets::CreateDatasetFromSnapshotUseCase;
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::{
    CreateDatasetFromSnapshotUseCaseImpl,
    DeleteDatasetUseCaseImpl,
    DependencyGraphServiceImpl,
    RenameDatasetUseCaseImpl,
};
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxImmediateImpl};
use mockall::predicate::eq;
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceDefault;

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_dataset_create_empty_without_visibility {
    ($tenancy_config:expr) => {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_account_by_name()
            .with(eq(DEFAULT_ACCOUNT_NAME.clone()))
            .returning(|_| {
                Ok(Some(Account::dummy()))
            });
        let harness = GraphQLDatasetsHarness::new_custom_authentication(
            mock_authentication_service,
            $tenancy_config,
        )
        .await;

        let request_code = indoc::indoc!(
            r#"
            mutation {
              datasets {
                createEmpty(datasetKind: ROOT, datasetAlias: "foo") {
                  ... on CreateDatasetResultSuccess {
                    dataset {
                      name
                    }
                  }
                }
              }
            }
            "#
        );

        expect_anonymous_access_error(harness.execute_anonymous_query(request_code).await);

        let res = harness.execute_authorized_query(request_code).await;

        assert!(res.is_ok(), "{res:?}");
        pretty_assertions::assert_eq!(
            async_graphql::value!({
                "datasets": {
                    "createEmpty": {
                        "dataset": {
                            "name": "foo",
                        }
                    }
                }
            }),
            res.data,
        );
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_dataset_create_empty_public {
    ($tenancy_config:expr) => {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_account_by_name()
            .with(eq(DEFAULT_ACCOUNT_NAME.clone()))
            .returning(|_| {
                Ok(Some(Account::dummy()))
            });
        let harness = GraphQLDatasetsHarness::new_custom_authentication(
            mock_authentication_service,
            $tenancy_config,
        )
        .await;

        let request_code = indoc::indoc!(
            r#"
            mutation {
              datasets {
                createEmpty(datasetKind: ROOT, datasetAlias: "foo", datasetVisibility: PUBLIC) {
                  ... on CreateDatasetResultSuccess {
                    dataset {
                      name
                    }
                  }
                }
              }
            }
            "#
        );

        expect_anonymous_access_error(harness.execute_anonymous_query(request_code).await);

        let res = harness.execute_authorized_query(request_code).await;

        assert!(res.is_ok(), "{res:?}");
        pretty_assertions::assert_eq!(
            async_graphql::value!({
                "datasets": {
                    "createEmpty": {
                        "dataset": {
                            "name": "foo",
                        }
                    }
                }
            }),
            res.data,
        );
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_by_id_does_not_exist() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;
    let res = harness.execute_anonymous_query(indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc") {
                        name
                    }
                }
            }
            "#
        ))
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": null,
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_by_id() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let res = harness
        .execute_anonymous_query(
            indoc!(
                r#"
                {
                    datasets {
                        byId (datasetId: "<id>") {
                            name
                        }
                    }
                }
                "#
            )
            .replace(
                "<id>",
                &foo_result.dataset_handle.id.as_did_str().to_stack_string(),
            ),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "name": "foo",
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_by_account_and_name_case_insensitive() {
    let account_name = odf::AccountName::new_unchecked("KaMu");

    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_account_by_name()
        .with(eq(account_name.clone()))
        .returning(|_| Ok(Some(Account::dummy())));

    let harness = GraphQLDatasetsHarness::new_custom_authentication(
        mock_authentication_service,
        TenancyConfig::MultiTenant,
    )
    .await;

    harness
        .create_root_dataset(
            Some(account_name.clone()),
            odf::DatasetName::new_unchecked("Foo"),
        )
        .await;

    let res = harness
        .execute_anonymous_query(
            indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName(accountName: "kAmU", datasetName: "<name>") {
                            name,
                            owner { accountName },
                        }
                    }
                }
                "#
            )
            .replace("<name>", "FoO"),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byOwnerAndName": {
                    "name": "Foo",
                    "owner": {
                       "accountName": DEFAULT_ACCOUNT_NAME_STR,
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_by_account_id() {
    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_find_account_name_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(DEFAULT_ACCOUNT_NAME.clone())));

    let harness = GraphQLDatasetsHarness::new_custom_authentication(
        mock_authentication_service,
        TenancyConfig::SingleTenant,
    )
    .await;
    harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("Foo"))
        .await;

    let res = harness
        .execute_anonymous_query(
            indoc!(
                r#"
                {
                    datasets {
                        byAccountId(accountId: "<accountId>") {
                            nodes {
                                name,
                                owner { accountName }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<accountId>", DEFAULT_ACCOUNT_ID.to_string().as_str()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byAccountId": {
                    "nodes": [
                        {
                            "name": "Foo",
                            "owner": {
                               "accountName": DEFAULT_ACCOUNT_NAME_STR,
                            }
                        }
                    ]

                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_empty_without_visibility_st() {
    test_dataset_create_empty_without_visibility!(TenancyConfig::SingleTenant);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_empty_without_visibility_mt() {
    test_dataset_create_empty_without_visibility!(TenancyConfig::MultiTenant);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_empty_public_st() {
    test_dataset_create_empty_public!(TenancyConfig::SingleTenant);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_empty_public_mt() {
    test_dataset_create_empty_public!(TenancyConfig::MultiTenant);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_from_snapshot() {
    let mut mock_authentication_service = MockAuthenticationService::built_in();
    mock_authentication_service
        .expect_account_by_name()
        .returning(|_| {
            Ok(Some(Account::test(
                odf::AccountID::new(odf::metadata::DidOdf::new_seeded_ed25519(&[1, 2, 3])),
                "kamu",
            )))
        });
    let harness = GraphQLDatasetsHarness::new_custom_authentication(
        mock_authentication_service,
        TenancyConfig::MultiTenant,
    )
    .await;

    let snapshot = MetadataFactory::dataset_snapshot()
        .name("foo")
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    use odf::metadata::serde::yaml::YamlDatasetSnapshotSerializer;
    use odf::metadata::serde::DatasetSnapshotSerializer;

    let snapshot_yaml = String::from_utf8_lossy(
        &YamlDatasetSnapshotSerializer
            .write_manifest(&snapshot)
            .unwrap(),
    )
    .to_string();

    let request_code = indoc!(
        r#"
        mutation {
            datasets {
                createFromSnapshot (snapshot: "<content>", snapshotFormat: YAML) {
                    ... on CreateDatasetResultSuccess {
                        dataset {
                            name
                            kind
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<content>", &snapshot_yaml.escape_default().to_string());

    expect_anonymous_access_error(harness.execute_anonymous_query(request_code.clone()).await);

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "createFromSnapshot": {
                    "dataset": {
                        "name": "foo",
                        "kind": "ROOT",
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_from_snapshot_malformed() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;

    let res = harness
        .execute_authorized_query(indoc!(
            r#"
        mutation {
            datasets {
                createFromSnapshot(snapshot: "version: 1", snapshotFormat: YAML) {
                    ... on MetadataManifestMalformed {
                        __typename
                    }
                }
            }
        }
        "#
        ))
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "createFromSnapshot": {
                    "__typename": "MetadataManifestMalformed",
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_rename_success() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let request_code = indoc!(
        r#"
        mutation {
            datasets {
                byId (datasetId: "<id>") {
                    rename(newName: "<newName>") {
                        __typename
                        message
                        ... on RenameResultSuccess {
                            oldName
                            newName
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &foo_result.dataset_handle.id.to_string())
    .replace("<newName>", "bar");

    expect_anonymous_access_error(harness.execute_anonymous_query(request_code.clone()).await);

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "rename": {
                        "__typename": "RenameResultSuccess",
                        "message": "Success",
                        "oldName": "foo",
                        "newName": "bar"
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_rename_no_changes() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let res = harness
        .execute_authorized_query(
            indoc!(
                r#"
                mutation {
                    datasets {
                        byId (datasetId: "<id>") {
                            rename(newName: "<newName>") {
                                __typename
                                message
                                ... on RenameResultNoChanges {
                                    preservedName
                                }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<id>", &foo_result.dataset_handle.id.to_string())
            .replace("<newName>", "foo"),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "rename": {
                        "__typename": "RenameResultNoChanges",
                        "message": "No changes",
                        "preservedName": "foo"
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_rename_name_collision() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;
    let _bar_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("bar"))
        .await;

    let res = harness
        .execute_authorized_query(
            indoc!(
                r#"
                mutation {
                    datasets {
                        byId (datasetId: "<id>") {
                            rename(newName: "<newName>") {
                                __typename
                                message
                                ... on RenameResultNameCollision {
                                    collidingAlias
                                }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<id>", &foo_result.dataset_handle.id.to_string())
            .replace("<newName>", "bar"),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "rename": {
                        "__typename": "RenameResultNameCollision",
                        "message": "Dataset 'bar' already exists",
                        "collidingAlias": "bar"
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_delete_success() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let request_code = indoc!(
        r#"
        mutation {
            datasets {
                byId (datasetId: "<id>") {
                    delete {
                        __typename
                        message
                        ... on DeleteResultSuccess {
                            deletedDataset
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &foo_result.dataset_handle.id.to_string());

    expect_anonymous_access_error(harness.execute_anonymous_query(request_code.clone()).await);

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "delete": {
                        "__typename": "DeleteResultSuccess",
                        "message": "Success",
                        "deletedDataset": "foo"
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_delete_dangling_ref() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;
    let _bar_result = harness
        .create_derived_dataset(
            odf::DatasetName::new_unchecked("bar"),
            &foo_result.dataset_handle,
        )
        .await;

    let res = harness
        .execute_authorized_query(
            indoc!(
                r#"
                mutation {
                    datasets {
                        byId (datasetId: "<id>") {
                            delete {
                                __typename
                                message
                                ... on DeleteResultDanglingReference {
                                    notDeletedDataset
                                    danglingChildRefs
                                }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<id>", &foo_result.dataset_handle.id.to_string()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "delete": {
                        "__typename": "DeleteResultDanglingReference",
                        "message": "Dataset 'foo' has 1 dangling reference(s)",
                        "notDeletedDataset": "foo",
                        "danglingChildRefs": ["bar"]
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_view_permissions() {
    let harness = GraphQLDatasetsHarness::new(TenancyConfig::SingleTenant).await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let request_code = indoc!(
        r#"
        query {
            datasets {
                byId (datasetId: "<id>") {
                    permissions {
                        canView
                        canDelete
                        canRename
                        canCommit
                        canSchedule
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &foo_result.dataset_handle.id.to_string());

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "permissions": {
                        "canView": true,
                        "canDelete": true,
                        "canRename": true,
                        "canCommit": true,
                        "canSchedule": true,
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLDatasetsHarness {
    _tempdir: tempfile::TempDir,
    catalog_authorized: dill::Catalog,
    catalog_anonymous: dill::Catalog,
}

impl GraphQLDatasetsHarness {
    pub async fn new(tenancy_config: TenancyConfig) -> Self {
        Self::new_custom_authentication(MockAuthenticationService::built_in(), tenancy_config).await
    }

    pub async fn new_custom_authentication(
        mock_authentication_service: MockAuthenticationService,
        tenancy_config: TenancyConfig,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let base_catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add::<SystemTimeSourceDefault>()
                .add::<DidGeneratorDefault>()
                .add_builder(
                    OutboxImmediateImpl::builder()
                        .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
                )
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<RenameDatasetUseCaseImpl>()
                .add::<DeleteDatasetUseCaseImpl>()
                .add::<ViewDatasetUseCaseImpl>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add_value(tenancy_config)
                .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
                .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
                .bind::<dyn DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
                .add::<DatasetRegistrySoloUnitBridge>()
                .add_value(mock_authentication_service)
                .bind::<dyn AuthenticationService, MockAuthenticationService>()
                .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
                .add::<RebacServiceImpl>()
                .add_value(kamu_auth_rebac_services::DefaultAccountProperties { is_admin: false })
                .add_value(kamu_auth_rebac_services::DefaultDatasetProperties {
                    allows_anonymous_read: false,
                    allows_public_read: false,
                })
                .add::<InMemoryRebacRepository>();

            if tenancy_config == TenancyConfig::MultiTenant {
                b.add::<MultiTenantRebacDatasetLifecycleMessageConsumer>();
            }

            NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
            );

            b.build()
        };

        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&base_catalog).await;

        Self {
            _tempdir: tempdir,
            catalog_anonymous,
            catalog_authorized,
        }
    }

    pub async fn create_root_dataset(
        &self,
        account_name: Option<odf::AccountName>,
        name: odf::DatasetName,
    ) -> odf::CreateDatasetResult {
        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(account_name, name))
                    .kind(odf::DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset(
        &self,
        name: odf::DatasetName,
        input_dataset: &odf::DatasetHandle,
    ) -> odf::CreateDatasetResult {
        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(None, name))
                    .kind(odf::DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(vec![input_dataset.alias.clone()])
                            .build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(query.into().data(self.catalog_authorized.clone()))
            .await
    }

    pub async fn execute_anonymous_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(query.into().data(self.catalog_anonymous.clone()))
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
