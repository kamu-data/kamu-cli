// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use async_graphql::{value, PathSegment};
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
use kamu_webhooks_services::{
    CreateWebhookSubscriptionUseCaseImpl,
    WebhookSubscriptionQueryServiceImpl,
};
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::utils::{authentication_catalogs, BaseGQLDatasetHarness, PredefinedAccountOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FIXED_SECRET: &str = "6923bdb993ca8290d0705dd13a31956f2853a1de15860ba8418293028bb1d17a";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_expected_event_types() {
    let harness = WebhookSubscriptiuonsHarness::new(MockWebhookSecretGenerator::new()).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(
            async_graphql::Request::new(WebhookSubscriptiuonsHarness::event_types_query())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "webhooks": {
                "eventTypes": [
                    kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED
                ]
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_and_see_subscription() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

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
                "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                "label": "My Webhook Subscription",
            })))
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    let subscription_id = res.data.clone().into_json().unwrap()["datasets"]["byId"]["webhooks"]
        ["createSubscription"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "webhooks": {
                        "createSubscription": {
                            "__typename": "CreateWebhookSubscriptionResultSuccess",
                            "message": "Success",
                            "subscriptionId": subscription_id,
                            "secret": FIXED_SECRET
                        }
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
            "datasets": {
                "byId": {
                    "webhooks": {
                        "subscriptions": [
                            {
                                "__typename": "WebhookSubscription",
                                "id": subscription_id,
                                "datasetId": create_result.dataset_handle.id.to_string(),
                                "targetUrl": "https://example.com/webhook",
                                "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                                "label": "My Webhook Subscription",
                                "status": "ENABLED",
                            }
                        ]
                    }
                }

            }
        })
    );

    let res = schema
        .execute(
            async_graphql::Request::new(WebhookSubscriptiuonsHarness::subscription_by_id_query())
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": create_result.dataset_handle.id.to_string(),
                    "subscriptionId": subscription_id.clone(),
                })))
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "webhooks": {
                        "subscription": {
                            "__typename": "WebhookSubscription",
                            "id": subscription_id,
                            "datasetId": create_result.dataset_handle.id.to_string(),
                            "targetUrl": "https://example.com/webhook",
                            "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                            "label": "My Webhook Subscription",
                            "status": "ENABLED",
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completely_invalid_target_urls() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

    let mock_secret_generator =
        WebhookSubscriptiuonsHarness::make_mock_secret_generator(FIXED_SECRET.to_string());

    let harness = WebhookSubscriptiuonsHarness::new(mock_secret_generator).await;

    let create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let invalid_urls = ["", "123", "http://"];

    for invalid_url in invalid_urls {
        let res = schema
            .execute(
                async_graphql::Request::new(
                    WebhookSubscriptiuonsHarness::create_subscription_mutation(),
                )
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": create_result.dataset_handle.id.to_string(),
                    "targetUrl": invalid_url,
                    "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                    "label": "My Webhook Subscription",
                })))
                .data(harness.catalog_authorized.clone()),
            )
            .await;

        assert!(res.is_err(), "{res:?}");

        let errors = res.errors;
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("Invalid URL"),);

        assert_eq!(
            errors[0].path,
            &[
                PathSegment::Field("datasets".into()),
                PathSegment::Field("byId".into()),
                PathSegment::Field("webhooks".into()),
                PathSegment::Field("createSubscription".into()),
            ],
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_target_url_non_https_or_localhost() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

    let mock_secret_generator =
        WebhookSubscriptiuonsHarness::make_mock_secret_generator(FIXED_SECRET.to_string());

    let harness = WebhookSubscriptiuonsHarness::new(mock_secret_generator).await;

    let create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let invalid_urls = [
        "http://example.com/webhook",
        "https://localhost:8080/webhook",
        "https://127.0.0.1/webhook",
    ];

    for invalid_url in invalid_urls {
        let res = schema
            .execute(
                async_graphql::Request::new(
                    WebhookSubscriptiuonsHarness::create_subscription_mutation(),
                )
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": create_result.dataset_handle.id.to_string(),
                    "targetUrl": invalid_url,
                    "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                    "label": "My Webhook Subscription",
                })))
                .data(harness.catalog_authorized.clone()),
            )
            .await;

        assert!(res.is_ok(), "{res:?}");
        assert_eq!(
            res.data,
            value!({
                "datasets": {
                    "byId": {
                        "webhooks": {
                            "createSubscription": {
                                "__typename": "WebhookSubscriptionInvalidTargetUrl",
                                "message": format!("Expecting https:// target URLs with host not pointing to 'localhost': {invalid_url}"),
                            }
                        }
                    }
                }
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_bad_event_type() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

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
                "eventTypes": ["BAD.EVENT.TYPE"],
                "label": "My Webhook Subscription",
            })))
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_err(), "{res:?}");

    let errors = res.errors;
    assert_eq!(errors.len(), 1);
    assert!(errors[0].message.contains("Invalid value 'BAD.EVENT.TYPE'"),);

    assert_eq!(
        errors[0].path,
        &[
            PathSegment::Field("datasets".into()),
            PathSegment::Field("byId".into()),
            PathSegment::Field("webhooks".into()),
            PathSegment::Field("createSubscription".into()),
        ],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_event_types() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

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
                "eventTypes": [],
                "label": "My Webhook Subscription",
            })))
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "webhooks": {
                        "createSubscription": {
                            "__typename": "WebhookSubscriptionNoEventTypesProvided",
                            "message": "At least one event type must be provided",
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_duplicate_labels() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

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
                "targetUrl": "https://example.com/webhook/1",
                "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                "label": "My Webhook Subscription",
            })))
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");

    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::create_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": create_result.dataset_handle.id.to_string(),
                "targetUrl": "https://example.com/webhook/2",
                "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                "label": "My Webhook Subscription",
            })))
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "webhooks": {
                        "createSubscription": {
                            "__typename": "WebhookSubscriptionDuplicateLabel",
                            "message": "Label 'My Webhook Subscription' is not unique",
                        }
                    }
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
            b.add::<CreateWebhookSubscriptionUseCaseImpl>();
            b.add::<WebhookSubscriptionQueryServiceImpl>();
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
                datasets {
                    byId(datasetId: $datasetId) {
                        webhooks {
                            createSubscription(
                                input: {
                                    targetUrl: $targetUrl
                                    eventTypes: $eventTypes
                                    label: $label
                                }
                            ) {
                                __typename
                                message
                                ... on CreateWebhookSubscriptionResultSuccess {
                                    subscriptionId
                                    secret
                                }
                            }
                        }
                    }
                }
            }
          "#
        )
    }

    fn event_types_query() -> &'static str {
        indoc!(
            r#"
            query {
                webhooks {
                    eventTypes
                }
            }
          "#
        )
    }

    fn dataset_subscriptions_query() -> &'static str {
        indoc!(
            r#"
            query($datasetId: DatasetID!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        webhooks {
                            subscriptions {
                                __typename
                                id
                                datasetId
                                targetUrl
                                eventTypes
                                label
                                status
                            }
                        }
                    }
                }
            }
          "#
        )
    }

    fn subscription_by_id_query() -> &'static str {
        indoc!(
            r#"
            query($datasetId: DatasetID!, $subscriptionId: WebhookSubscriptionID!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        webhooks {
                            subscription(id: $subscriptionId) {
                                __typename
                                id
                                datasetId
                                targetUrl
                                eventTypes
                                label
                                status
                            }
                        }
                    }
                }
            }
          "#
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
