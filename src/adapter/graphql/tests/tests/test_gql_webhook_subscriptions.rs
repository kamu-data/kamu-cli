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
use kamu_webhooks::*;
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;
use kamu_webhooks_services::*;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::utils::{authentication_catalogs, BaseGQLDatasetHarness, PredefinedAccountOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FIXED_SECRET: &str = "6923bdb993ca8290d0705dd13a31956f2853a1de15860ba8418293028bb1d17a";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_expected_event_types() {
    let harness = WebhookSubscriptiuonsHarness::new().await;
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
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

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
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

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
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

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
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

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
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

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
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

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

#[test_log::test(tokio::test)]
async fn test_update_subscription() {
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let subscription_id = harness
        .create_webhook_subscription(
            &schema,
            &create_result.dataset_handle.id,
            "https://example.com/webhook".to_string(),
            vec![kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
            "My Webhook Subscription".to_string(),
        )
        .await;

    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::update_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": create_result.dataset_handle.id.to_string(),
                "subscriptionId": subscription_id.clone(),
                "targetUrl": "https://example.com/webhook/updated",
                "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                "label": "Updated Webhook Subscription",
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
                        "subscription": {
                            "update": {
                                "__typename": "UpdateWebhookSubscriptionResultSuccess",
                                "message": "Success",
                            }
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
                                "targetUrl": "https://example.com/webhook/updated",
                                "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
                                "label": "Updated Webhook Subscription",
                                "status": "ENABLED",
                            }
                        ]
                    }
                }

            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_subscription_failure() {
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let subscription_id = harness
        .create_webhook_subscription(
            &schema,
            &create_result.dataset_handle.id,
            "https://example.com/webhook".to_string(),
            vec![kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
            "My Webhook Subscription".to_string(),
        )
        .await;

    let base_variables_json = json!({
        "datasetId": create_result.dataset_handle.id.to_string(),
        "subscriptionId": subscription_id.clone(),
        "targetUrl": "https://example.com/webhook/updated",
        "eventTypes": [kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
        "label": "Updated Webhook Subscription",
    });

    // Wrong dataset ID
    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::update_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json({
                let mut variables = base_variables_json.clone();
                variables["datasetId"] =
                    json!(odf::DatasetID::new_seeded_ed25519(b"invalid").to_string());
                variables
            }))
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": null,
            }
        })
    );

    // Wrong subscription ID
    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::update_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json({
                let mut variables = base_variables_json.clone();
                variables["subscriptionId"] = json!(uuid::Uuid::new_v4().to_string());
                variables
            }))
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
                        "subscription": null,
                    }
                },
            }
        })
    );

    // Wrong target URL
    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::update_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json({
                let mut variables = base_variables_json.clone();
                variables["targetUrl"] = json!("http://example.com/"); // not https!
                variables
            }))
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
                        "subscription": {
                            "update": {
                                "__typename": "WebhookSubscriptionInvalidTargetUrl",
                                "message": "Expecting https:// target URLs with host not pointing to 'localhost': http://example.com/",
                            }
                        },
                    }
                },
            }
        })
    );

    // No event types
    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::update_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json({
                let mut variables = base_variables_json.clone();
                variables["eventTypes"] = json!([]); // no event types
                variables
            }))
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
                        "subscription": {
                            "update": {
                                "__typename": "WebhookSubscriptionNoEventTypesProvided",
                                "message": "At least one event type must be provided",
                            }
                        },
                    }
                },
            }
        })
    );

    // Duplicate label: modifying self is not an error
    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::update_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json({
                let mut variables = base_variables_json.clone();
                variables["label"] = json!("My Webhook Subscription"); // same label
                variables
            }))
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
                        "subscription": {
                            "update": {
                                "__typename": "UpdateWebhookSubscriptionResultSuccess",
                                "message": "Success",
                            }
                        },
                    }
                },
            }
        })
    );

    let subscription_id_2 = harness
        .create_webhook_subscription(
            &schema,
            &create_result.dataset_handle.id,
            "https://example.com/webhook/2".to_string(),
            vec![kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
            "My Other Webhook Subscription".to_string(),
        )
        .await;

    // Duplicate label: modifying other subscription with same label as first one
    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::update_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json({
                let mut variables = base_variables_json.clone();
                variables["subscriptionId"] = json!(subscription_id_2);
                variables["label"] = json!("My Webhook Subscription"); // same label as 1st subscription
                variables
            }))
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
                        "subscription": {
                            "update": {
                                "__typename": "WebhookSubscriptionDuplicateLabel",
                                "message": "Label 'My Webhook Subscription' is not unique",
                            }
                        },
                    }
                },
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_subscription() {
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let subscription_id = harness
        .create_webhook_subscription(
            &schema,
            &create_result.dataset_handle.id,
            "https://example.com/webhook".to_string(),
            vec![kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
            "My Webhook Subscription".to_string(),
        )
        .await;

    let res =
        schema
            .execute(
                async_graphql::Request::new(
                    WebhookSubscriptiuonsHarness::pause_subscription_mutation(),
                )
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": create_result.dataset_handle.id.to_string(),
                    "subscriptionId": subscription_id.clone(),
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
                        "subscription": {
                            "pause": {
                                "__typename": "PauseWebhookSubscriptionResultSuccess",
                                "message": "Success",
                            }
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
                                "status": "PAUSED",
                            }
                        ]
                    }
                }

            }
        })
    );

    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::resume_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": create_result.dataset_handle.id.to_string(),
                "subscriptionId": subscription_id.clone(),
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
                        "subscription": {
                            "resume": {
                                "__typename": "ResumeWebhookSubscriptionResultSuccess",
                                "message": "Success",
                            }
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_remove_subscription() {
    let harness = WebhookSubscriptiuonsHarness::new().await;
    let create_result = harness.create_root_dataset("foo").await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let subscription_id = harness
        .create_webhook_subscription(
            &schema,
            &create_result.dataset_handle.id,
            "https://example.com/webhook".to_string(),
            vec![kamu_webhooks::WebhookEventTypeCatalog::DATASET_REF_UPDATED],
            "My Webhook Subscription".to_string(),
        )
        .await;

    let res = schema
        .execute(
            async_graphql::Request::new(
                WebhookSubscriptiuonsHarness::remove_subscription_mutation(),
            )
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": create_result.dataset_handle.id.to_string(),
                "subscriptionId": subscription_id.clone(),
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
                        "subscription": {
                            "remove": {
                                "__typename": "RemoveWebhookSubscriptionResultSuccess",
                                "message": "Success",
                            }
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
                        "subscriptions": []
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
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .build();

        let mock_secret_generator =
            WebhookSubscriptiuonsHarness::make_mock_secret_generator(FIXED_SECRET.to_string());

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());
            b.add::<CreateWebhookSubscriptionUseCaseImpl>();
            b.add::<UpdateWebhookSubscriptionUseCaseImpl>();
            b.add::<PauseWebhookSubscriptionUseCaseImpl>();
            b.add::<ResumeWebhookSubscriptionUseCaseImpl>();
            b.add::<RemoveWebhookSubscriptionUseCaseImpl>();
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

    async fn create_root_dataset(&self, dataset_name: &str) -> CreateDatasetResult {
        let dataset_name = odf::DatasetName::new_unchecked(dataset_name);
        let dataset_alias =
            odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), dataset_name.clone());

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

    async fn create_webhook_subscription(
        &self,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
        target_url: String,
        event_types: Vec<&'static str>,
        label: String,
    ) -> String {
        let res = schema
            .execute(
                async_graphql::Request::new(
                    WebhookSubscriptiuonsHarness::create_subscription_mutation(),
                )
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": dataset_id.to_string(),
                    "targetUrl": target_url,
                    "eventTypes": event_types,
                    "label": label,
                })))
                .data(self.catalog_authorized.clone()),
            )
            .await;

        assert!(res.is_ok(), "{res:?}");

        res.data.clone().into_json().unwrap()["datasets"]["byId"]["webhooks"]["createSubscription"]
            ["subscriptionId"]
            .as_str()
            .unwrap()
            .to_string()
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

    fn update_subscription_mutation() -> &'static str {
        indoc!(
            r#"
            mutation (
                $datasetId: DatasetID!,
                $subscriptionId: WebhookSubscriptionID!,
                $targetUrl: String!,
                $eventTypes: [String!]!,
                $label: String!
            ) {
                datasets {
                    byId(datasetId: $datasetId) {
                        webhooks {
                            subscription(id: $subscriptionId) {
                                update(
                                    input: {
                                        targetUrl: $targetUrl
                                        eventTypes: $eventTypes
                                        label: $label
                                    }
                                ) {
                                    __typename
                                    message
                                }
                            }
                        }
                    }
                }
            }
          "#
        )
    }

    fn pause_subscription_mutation() -> &'static str {
        indoc!(
            r#"
            mutation (
                $datasetId: DatasetID!,
                $subscriptionId: WebhookSubscriptionID!
            ) {
                datasets {
                    byId(datasetId: $datasetId) {
                        webhooks {
                            subscription(id: $subscriptionId) {
                                pause {
                                    __typename
                                    message
                                }
                            }
                        }
                    }
                }
            }
          "#
        )
    }

    fn resume_subscription_mutation() -> &'static str {
        indoc!(
            r#"
            mutation (
                $datasetId: DatasetID!,
                $subscriptionId: WebhookSubscriptionID!
            ) {
                datasets {
                    byId(datasetId: $datasetId) {
                        webhooks {
                            subscription(id: $subscriptionId) {
                                resume {
                                    __typename
                                    message
                                }
                            }
                        }
                    }
                }
            }
          "#
        )
    }

    fn remove_subscription_mutation() -> &'static str {
        indoc!(
            r#"
            mutation (
                $datasetId: DatasetID!,
                $subscriptionId: WebhookSubscriptionID!
            ) {
                datasets {
                    byId(datasetId: $datasetId) {
                        webhooks {
                            subscription(id: $subscriptionId) {
                                remove {
                                    __typename
                                    message
                                }
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
