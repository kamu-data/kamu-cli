// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::Engine;
use bon::bon;
use indoc::indoc;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::RebacServiceImpl;
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_services::*;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::utils::{authentication_catalogs, BaseGQLDatasetHarness};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn base64usnp_encode(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_versioned_file_create_in_band() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create versioned file dataset
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetAlias: DatasetAlias!) {
                    datasets {
                        createVersionedFile(
                            datasetAlias: $datasetAlias,
                            datasetVisibility: PUBLIC,
                        ) {
                            ... on CreateDatasetResultSuccess {
                                dataset {
                                    id
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetAlias": "x",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let did = res.data.into_json().unwrap()["datasets"]["createVersionedFile"]["dataset"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Upload first version
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $content: Base64Usnp!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                uploadNewVersion(content: $content) {
                                    ... on UpdateVersionSuccess {
                                        newVersion
                                        contentHash
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
                "content": base64usnp_encode(b"hello"),
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["uploadNewVersion"]
        .clone();
    let new_version = upload_result["newVersion"].as_i64().unwrap();
    let content_hash = upload_result["contentHash"].as_str().unwrap();

    assert_eq!(new_version, 1);
    assert_eq!(
        content_hash,
        "f16203338be694f50c5f338814986cdf0686453a888b84f424d792af4b9202398f392"
    );

    // Upload second version
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $content: Base64Usnp!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                uploadNewVersion(content: $content) {
                                    ... on UpdateVersionSuccess {
                                        newVersion
                                        contentHash
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
                "content": base64usnp_encode(b"bye"),
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["uploadNewVersion"]
        .clone();
    let new_version = upload_result["newVersion"].as_i64().unwrap();
    let content_hash = upload_result["contentHash"].as_str().unwrap();

    assert_eq!(new_version, 2);
    assert_eq!(
        content_hash,
        "f162040d234965143cf2113060344aec5c3ad74b34a5f713b16df21c6fc9349fb047b"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct GraphQLDatasetsHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

#[bon]
impl GraphQLDatasetsHarness {
    #[builder]
    pub async fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    ) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .maybe_mock_dataset_action_authorizer(mock_dataset_action_authorizer)
            .build();

        let base_catalog = dill::CatalogBuilder::new_chained(base_gql_harness.catalog())
            .add::<RenameDatasetUseCaseImpl>()
            .add::<DeleteDatasetUseCaseImpl>()
            .add::<RebacServiceImpl>()
            .add_value(kamu_auth_rebac_services::DefaultAccountProperties { is_admin: false })
            .add_value(kamu_auth_rebac_services::DefaultDatasetProperties {
                allows_anonymous_read: false,
                allows_public_read: false,
            })
            .add::<InMemoryRebacRepository>()
            .add_value(kamu::EngineConfigDatafusionEmbeddedBatchQuery::default())
            .add::<kamu::QueryServiceImpl>()
            .add::<kamu::ObjectStoreRegistryImpl>()
            .add::<kamu::ObjectStoreBuilderLocalFs>()
            .add_value(kamu::EngineConfigDatafusionEmbeddedIngest::default())
            .add::<kamu::EngineProvisionerNull>()
            .add::<kamu::DataFormatRegistryImpl>()
            .add::<kamu::PushIngestPlannerImpl>()
            .add::<kamu::PushIngestExecutorImpl>()
            .add::<kamu::PushIngestDataUseCaseImpl>()
            .build();

        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&base_catalog).await;

        Self {
            base_gql_harness,
            catalog_anonymous,
            catalog_authorized,
        }
    }

    pub async fn create_root_dataset(
        &self,
        account_name: Option<odf::AccountName>,
        name: odf::DatasetName,
    ) -> CreateDatasetResult {
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
    ) -> CreateDatasetResult {
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
