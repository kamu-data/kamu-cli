// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use async_graphql::value;
use indoc::indoc;
use kamu_accounts::DEFAULT_ACCOUNT_NAME;
use kamu_core::*;
use kamu_datasets::*;
use kamu_webhooks::{
    MockWebhookSecretGenerator,
    WebhookSecretGenerator,
    WebhookSubscriptionSecret,
};
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::utils::{authentication_catalogs, BaseGQLDatasetHarness, PredefinedAccountOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_and_see_subscription() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

    const FIXED_SECRET: &str = "6923bdb993ca8290d0705dd13a31956f2853a1de15860ba8418293028bb1d17a";
    let mock_secret_generator =
        WebhookSubscriptiuonsHarness::make_mock_secret_generator(FIXED_SECRET.to_string());

    let harness = WebhookSubscriptiuonsHarness::new(mock_secret_generator).await;

    let create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::create_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": create_result.dataset_handle.id.to_string(),
                "targetUrl": "https://example.com/webhook",
                "eventTypes": ["DATASET.REF.UPDATED"],
                "label": "My Webhook Subscription",
            })))
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "webhooks": {
                "subscriptions": {
                    "createSubscription": {
                        "__typename": "CreateWebhookSubscriptionResultSuccess",
                        "message": "Success",
                        "secret": FIXED_SECRET
                    }
                }
            }
        })
    );

    let res =
        schema
            .execute(
                async_graphql::Request::new(
                    WebhookSubscriptiuonsHarness::dataset_subscriptions_query(),
                )
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": create_result.dataset_handle.id.to_string(),
                })))
                .data(harness.catalog_authorized.clone()),
            )
            .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "webhooks": {
                "subscriptions": {
                    "byDataset": [
                        {
                            "__typename": "WebhookSubscription",
                            "datasetId": create_result.dataset_handle.id.to_string(),
                            "targetUrl": "https://example.com/webhook",
                            "eventTypes": ["DATASET.REF.UPDATED"],
                            "label": "My Webhook Subscription",
                            "status": "ENABLED",
                        }
                    ]
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct WebhookSubscriptiuonsHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
}

impl WebhookSubscriptiuonsHarness {
    async fn new(mock_secret_generator: MockWebhookSecretGenerator) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());
            b.add_value(mock_secret_generator);
            b.bind::<dyn WebhookSecretGenerator, MockWebhookSecretGenerator>();
            b.add::<InMemoryWebhookSubscriptionEventStore>();

            b.build()
        };

        let (_, catalog_authorized) =
            authentication_catalogs(&catalog_base, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            catalog_authorized,
        }
    }

    fn make_mock_secret_generator(with_secret: String) -> MockWebhookSecretGenerator {
        let mut mock_secret_generator = MockWebhookSecretGenerator::new();
        mock_secret_generator
            .expect_generate_secret()
            .returning(move || WebhookSubscriptionSecret::try_new(&with_secret).unwrap());
        mock_secret_generator
    }

    async fn create_root_dataset(&self, dataset_alias: odf::DatasetAlias) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(dataset_alias)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    fn create_subscription_mutation() -> &'static str {
        indoc!(
            r#"
            mutation ($datasetId: DatasetID!, $targetUrl: String!, $eventTypes: [String!]!, $label: String!) {
                webhooks {
                    subscriptions {
                        createSubscription(
                            input: {
                                datasetId: $datasetId
                                targetUrl: $targetUrl
                                eventTypes: $eventTypes
                                label: $label
                            }
                        ) {
                            __typename
                            message
                            ... on CreateWebhookSubscriptionResultSuccess {
                                secret
                            }
                        }
                    }
                }
            }
          "#
        )
    }

    fn dataset_subscriptions_query() -> &'static str {
        indoc!(
            r#"
            query($datasetId: DatasetID!) {
                webhooks {
                    subscriptions {
                        byDataset(datasetId: $datasetId) {
                            __typename
                            datasetId
                            targetUrl
                            eventTypes
                            label
                            status
                        }
                    }
                }
            }
          "#
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
