// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::HashSet;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use kamu_cli_e2e_common::{
    CreateDatasetResponse,
    CreatedWebhookSubscription,
    GraphQLResponseExt,
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
    WebhookFlowSummary,
};
use sha2::Digest as _;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::webhook_receiver::{VerifiedWebhookSignature, WebhookVerificationError, verify_webhook};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Fast flows, a 1-second delivery timeout, and a subscription made
/// unreachable after 2 failed deliveries in a row
pub const WEBHOOKS_E2E_CONFIG: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      flowSystem:
        awaitingStepSecs: 1
        mandatoryThrottlingPeriodSecs: 1
      backgroundAgents:
        maxListeningTimeout: 1s
      webhooks:
        deliveryTimeout: 1
        maxConsecutiveFailures: 2
    "#
);

/// [`WEBHOOKS_E2E_CONFIG`] with one retry of a failed delivery after a second
pub const WEBHOOKS_WITH_RETRY_E2E_CONFIG: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      flowSystem:
        awaitingStepSecs: 1
        mandatoryThrottlingPeriodSecs: 1
        defaultRetryPolicies:
          dev.kamu.flow.webhook.deliver:
            maxAttempts: 1
            minDelaySecs: 1
            backoffType: Fixed
      backgroundAgents:
        maxListeningTimeout: 1s
      webhooks:
        deliveryTimeout: 1
        maxConsecutiveFailures: 2
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_delivered_signed(kamu_api_server_client: KamuApiServerClient) {
    let scenario = WebhookScenario::new(kamu_api_server_client).await;
    scenario.receiver_responds(ResponseTemplate::new(200)).await;
    let subscription = scenario.subscribe(&scenario.receiver_url()).await;

    let old_head = scenario.head().await;
    scenario.ingest_match(1).await;
    let new_head = scenario.head().await;

    let flows = scenario.wait_for_finished_flows(&subscription, 1).await;
    pretty_assertions::assert_eq!(flows, [WebhookScenario::succeeded_flow(1)],);

    let requests = scenario.received_requests().await;
    assert_eq!(requests.len(), 1);
    let request = &requests[0];

    pretty_assertions::assert_eq!(
        [
            "content-type",
            "x-webhook-event-type",
            "x-webhook-subscription-id",
            "x-webhook-delivery-attempt",
            "user-agent",
        ]
        .map(|name| WebhookScenario::header(request, name)),
        [
            "application/json",
            "DATASET.REF.UPDATED",
            subscription.subscription_id.as_str(),
            "1",
            "Kamu-Webhook/1.0",
        ],
    );

    // A receiver holding the secret accepts the delivery, and the signature
    // covers the request line, the timestamp and the body digest
    let receiver_url = scenario.receiver_url();
    pretty_assertions::assert_eq!(
        verify_webhook(request, &receiver_url, &subscription.secret),
        Ok(WebhookScenario::expected_signature(request)),
    );

    // Anything else is rejected
    assert_eq!(
        verify_webhook(request, &receiver_url, "not-the-secret"),
        Err(WebhookVerificationError::SignatureMismatch)
    );
    assert_eq!(
        verify_webhook(
            &WebhookScenario::with_body(request, b"{}", false),
            &receiver_url,
            &subscription.secret
        ),
        Err(WebhookVerificationError::DigestMismatch)
    );
    // The digest is signed, so recomputing it for a new body does not help
    assert_eq!(
        verify_webhook(
            &WebhookScenario::with_body(request, b"{}", true),
            &receiver_url,
            &subscription.secret
        ),
        Err(WebhookVerificationError::SignatureMismatch)
    );
    assert_eq!(
        verify_webhook(
            &WebhookScenario::with_header(request, "x-webhook-timestamp", "0"),
            &receiver_url,
            &subscription.secret
        ),
        Err(WebhookVerificationError::SignatureMismatch)
    );
    assert_eq!(
        verify_webhook(
            request,
            &Url::parse("http://127.0.0.1:1/elsewhere").unwrap(),
            &subscription.secret
        ),
        Err(WebhookVerificationError::SignatureMismatch)
    );

    pretty_assertions::assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({
            "version": 2,
            "dataset_id": scenario.dataset.dataset_id.to_string(),
            "owner_account_id": scenario.owner_id.to_string(),
            "block_ref": "head",
            "new_hash": new_head.to_string(),
            "old_hash": old_head.to_string(),
            "is_breaking_change": false,
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_delivery_failed_on_unsuccessful_response(
    kamu_api_server_client: KamuApiServerClient,
) {
    let scenario = WebhookScenario::new(kamu_api_server_client).await;
    scenario.receiver_responds(ResponseTemplate::new(500)).await;
    let subscription = scenario.subscribe(&scenario.receiver_url()).await;

    scenario.ingest_match(1).await;

    pretty_assertions::assert_eq!(
        scenario.wait_for_finished_flows(&subscription, 1).await,
        [WebhookScenario::failed_flow(
            1,
            "Unsuccessful response code: 500 Internal Server Error"
        )],
    );
    assert_eq!(scenario.received_requests().await.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_delivery_failed_to_connect(kamu_api_server_client: KamuApiServerClient) {
    let scenario = WebhookScenario::new(kamu_api_server_client).await;
    let subscription = scenario
        .subscribe(&WebhookScenario::closed_port_url())
        .await;

    scenario.ingest_match(1).await;

    pretty_assertions::assert_eq!(
        scenario.wait_for_finished_flows(&subscription, 1).await,
        [WebhookScenario::failed_flow(1, "Failed to connect")],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_delivery_timed_out(kamu_api_server_client: KamuApiServerClient) {
    let scenario = WebhookScenario::new(kamu_api_server_client).await;
    // Longer than the configured 1-second delivery timeout
    scenario
        .receiver_responds(ResponseTemplate::new(200).set_delay(Duration::from_secs(3)))
        .await;
    let subscription = scenario.subscribe(&scenario.receiver_url()).await;

    scenario.ingest_match(1).await;

    pretty_assertions::assert_eq!(
        scenario.wait_for_finished_flows(&subscription, 1).await,
        [WebhookScenario::failed_flow(
            1,
            "Response timeout after 1 seconds"
        )],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_delivery_retried_until_success(
    kamu_api_server_client: KamuApiServerClient,
) {
    let scenario = WebhookScenario::new(kamu_api_server_client).await;
    // The first attempt fails, the retry succeeds
    scenario
        .receiver_responds_once(ResponseTemplate::new(500))
        .await;
    scenario.receiver_responds(ResponseTemplate::new(200)).await;
    let subscription = scenario.subscribe(&scenario.receiver_url()).await;

    scenario.ingest_match(1).await;

    pretty_assertions::assert_eq!(
        scenario.wait_for_finished_flows(&subscription, 1).await,
        [WebhookScenario::succeeded_flow(2)],
    );

    // A retry is a new delivery: same payload, new delivery ID
    let requests = scenario.received_requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body, requests[1].body);
    assert_eq!(
        requests
            .iter()
            .map(|request| WebhookScenario::header(request, "x-webhook-delivery-id"))
            .collect::<HashSet<_>>()
            .len(),
        2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_subscription_unreachable_until_reactivated(
    kamu_api_server_client: KamuApiServerClient,
) {
    let scenario = WebhookScenario::new(kamu_api_server_client).await;
    scenario.receiver_responds(ResponseTemplate::new(500)).await;
    let subscription = scenario.subscribe(&scenario.receiver_url()).await;

    // Two failures in a row reach the configured limit
    scenario.ingest_match(1).await;
    scenario.wait_for_finished_flows(&subscription, 1).await;
    scenario.ingest_match(2).await;
    scenario.wait_for_finished_flows(&subscription, 2).await;

    scenario
        .wait_for_subscription_status(&subscription, "UNREACHABLE")
        .await;

    // An unreachable subscription gets no delivery
    scenario.ingest_match(3).await;
    scenario.wait_for_flow_agent_steps().await;
    assert_eq!(scenario.received_requests().await.len(), 2);
    assert_eq!(scenario.list_flows(&subscription).await.len(), 2);

    // Once reactivated, the next update is delivered
    scenario.reset_receiver().await;
    scenario.receiver_responds(ResponseTemplate::new(200)).await;
    scenario.reactivate(&subscription).await;
    assert_eq!(scenario.subscription_status(&subscription).await, "ENABLED");

    scenario.ingest_match(4).await;

    let flows = scenario.wait_for_finished_flows(&subscription, 3).await;
    pretty_assertions::assert_eq!(flows[0], WebhookScenario::succeeded_flow(1));
    assert_eq!(scenario.received_requests().await.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_signed_with_rotated_secret(kamu_api_server_client: KamuApiServerClient) {
    let scenario = WebhookScenario::new(kamu_api_server_client).await;
    scenario.receiver_responds(ResponseTemplate::new(200)).await;
    let receiver_url = scenario.receiver_url();
    let subscription = scenario.subscribe(&receiver_url).await;

    scenario.ingest_match(1).await;
    scenario.wait_for_finished_flows(&subscription, 1).await;

    let new_secret = scenario.rotate_secret(&subscription).await;
    assert_ne!(new_secret, subscription.secret);

    scenario.ingest_match(2).await;
    scenario.wait_for_finished_flows(&subscription, 2).await;

    let requests = scenario.received_requests().await;
    assert_eq!(requests.len(), 2);

    // Before the rotation: the original secret
    assert_matches!(
        verify_webhook(&requests[0], &receiver_url, &subscription.secret),
        Ok(_)
    );

    // After it: only the new secret
    assert_matches!(
        verify_webhook(&requests[1], &receiver_url, &new_secret),
        Ok(_)
    );
    assert_eq!(
        verify_webhook(&requests[1], &receiver_url, &subscription.secret),
        Err(WebhookVerificationError::SignatureMismatch)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_external_target_rejected(kamu_api_server_client: KamuApiServerClient) {
    let scenario = WebhookScenario::new(kamu_api_server_client).await;

    // E2E mode accepts only loopback targets, whatever the scheme, so a test
    // can never deliver to an external host
    for external_url in [
        "https://example.com/hook",
        "http://192.0.2.1/hook",
        "https://unresolvable.invalid/hook",
    ] {
        assert_eq!(
            scenario
                .try_subscribe_bypassing_client_checks(external_url)
                .await,
            "WebhookSubscriptionInvalidTargetUrl",
            "{external_url}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A player-scores root dataset with push ingest, owned by the logged-in
/// user, and a local receiver for its webhook deliveries
struct WebhookScenario {
    client: KamuApiServerClient,
    receiver: MockServer,
    dataset: CreateDatasetResponse,
    owner_id: odf::AccountID,
}

impl WebhookScenario {
    async fn new(mut client: KamuApiServerClient) -> Self {
        client.auth().login_as_kamu().await;
        let owner_id = client.auth().logged_account_id();

        let dataset = client.dataset().create_player_scores_dataset().await;

        Self {
            client,
            receiver: MockServer::start().await,
            dataset,
            owner_id,
        }
    }

    fn receiver_url(&self) -> Url {
        Url::parse(&format!("{}/hook", self.receiver.uri())).unwrap()
    }

    /// A loopback URL nothing listens on
    fn closed_port_url() -> Url {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        Url::parse(&format!("http://127.0.0.1:{port}/hook")).unwrap()
    }

    async fn receiver_responds(&self, response: ResponseTemplate) {
        Self::hook_mock(response).mount(&self.receiver).await;
    }

    /// Takes precedence over responses mounted after it, for one request
    async fn receiver_responds_once(&self, response: ResponseTemplate) {
        Self::hook_mock(response)
            .up_to_n_times(1)
            .mount(&self.receiver)
            .await;
    }

    fn hook_mock(response: ResponseTemplate) -> Mock {
        Mock::given(method("POST"))
            .and(path("/hook"))
            .respond_with(response)
    }

    /// Forgets the mounted responses and the requests received so far
    async fn reset_receiver(&self) {
        self.receiver.reset().await;
    }

    async fn received_requests(&self) -> Vec<Request> {
        self.receiver.received_requests().await.unwrap()
    }

    async fn subscribe(&self, target_url: &Url) -> CreatedWebhookSubscription {
        self.client
            .webhook()
            .create_subscription(&self.dataset.dataset_id, target_url, "e2e")
            .await
    }

    /// Sends the create mutation directly, without the loopback check of the
    /// client API, and returns the result type name
    async fn try_subscribe_bypassing_client_checks(&self, target_url: &str) -> String {
        let response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    mutation($datasetId: DatasetID!, $targetUrl: String!) {
                      datasets {
                        byId(datasetId: $datasetId) {
                          webhooks {
                            createSubscription(
                              input: {
                                targetUrl: $targetUrl
                                eventTypes: ["DATASET.REF.UPDATED"]
                                label: "external"
                              }
                            ) {
                              __typename
                            }
                          }
                        }
                      }
                    }
                    "#
                ),
                Some(serde_json::json!({
                    "datasetId": self.dataset.dataset_id.to_string(),
                    "targetUrl": target_url,
                })),
            )
            .await
            .data();

        response["datasets"]["byId"]["webhooks"]["createSubscription"]["__typename"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn rotate_secret(&self, subscription: &CreatedWebhookSubscription) -> String {
        self.client
            .webhook()
            .rotate_secret(&self.dataset.dataset_id, &subscription.subscription_id)
            .await
    }

    async fn reactivate(&self, subscription: &CreatedWebhookSubscription) {
        self.client
            .webhook()
            .reactivate_subscription(&self.dataset.dataset_id, &subscription.subscription_id)
            .await;
    }

    async fn subscription_status(&self, subscription: &CreatedWebhookSubscription) -> String {
        self.client
            .webhook()
            .subscription_status(&self.dataset.dataset_id, &subscription.subscription_id)
            .await
    }

    async fn wait_for_subscription_status(
        &self,
        subscription: &CreatedWebhookSubscription,
        expected_status: &str,
    ) {
        for _ in 0..30 {
            if self.subscription_status(subscription).await == expected_status {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!("Subscription did not become {expected_status}");
    }

    /// Long enough for the flow agent to react to a change, if it would
    async fn wait_for_flow_agent_steps(&self) {
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    async fn list_flows(
        &self,
        subscription: &CreatedWebhookSubscription,
    ) -> Vec<WebhookFlowSummary> {
        self.client
            .webhook()
            .list_flows(&self.dataset.dataset_id, &subscription.subscription_id)
            .await
    }

    async fn wait_for_finished_flows(
        &self,
        subscription: &CreatedWebhookSubscription,
        expected_flow_count: usize,
    ) -> Vec<WebhookFlowSummary> {
        self.client
            .webhook()
            .wait_for_finished_flows(
                &self.dataset.dataset_id,
                &subscription.subscription_id,
                expected_flow_count,
            )
            .await
    }

    /// Pushes the scores of one new match, which adds a block
    async fn ingest_match(&self, match_id: u32) {
        let data = format!(
            "{{\"match_time\": \"2000-01-{match_id:02}\", \"match_id\": {match_id}, \
             \"player_id\": \"Alice\", \"score\": 100}}\n"
        );

        self.client
            .dataset()
            .ingest_data(&self.dataset.dataset_alias, RequestBody::NdJson(data))
            .await;
    }

    async fn head(&self) -> odf::Multihash {
        self.client
            .odf_transfer()
            .metadata_block_hash_by_ref(&self.dataset.dataset_alias, odf::BlockRef::Head)
            .await
            .unwrap()
    }

    fn succeeded_flow(task_count: usize) -> WebhookFlowSummary {
        WebhookFlowSummary {
            status: "FINISHED".to_string(),
            task_count,
            outcome: Some("FlowSuccessResult".to_string()),
            failure_message: None,
        }
    }

    fn failed_flow(task_count: usize, failure_message: &str) -> WebhookFlowSummary {
        WebhookFlowSummary {
            status: "FINISHED".to_string(),
            task_count,
            outcome: Some("FlowFailedError".to_string()),
            failure_message: Some(failure_message.to_string()),
        }
    }

    fn header<'a>(request: &'a Request, name: &str) -> &'a str {
        request
            .headers
            .get(name)
            .unwrap_or_else(|| panic!("Missing header: {name}"))
            .to_str()
            .unwrap()
    }

    /// The signature Kamu puts on every delivery
    fn expected_signature(request: &Request) -> VerifiedWebhookSignature {
        VerifiedWebhookSignature {
            covered_components: [
                "@method",
                "@path",
                "@authority",
                "x-webhook-timestamp",
                "content-digest",
            ]
            .map(ToString::to_string)
            .to_vec(),
            params: [
                ("keyid", "default"),
                ("alg", "hmac-sha256"),
                ("created", Self::header(request, "x-webhook-timestamp")),
            ]
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .to_vec(),
        }
    }

    /// The request with another body, and optionally a `Content-Digest` that
    /// matches it
    fn with_body(request: &Request, body: &[u8], recompute_digest: bool) -> Request {
        let mut tampered = request.clone();
        tampered.body = body.to_vec();
        if recompute_digest {
            let digest = format!("sha-256=:{}:", BASE64.encode(sha2::Sha256::digest(body)));
            tampered = Self::with_header(&tampered, "content-digest", &digest);
        }
        tampered
    }

    fn with_header(request: &Request, name: &'static str, value: &str) -> Request {
        let mut tampered = request.clone();
        tampered.headers.insert(name, value.parse().unwrap());
        tampered
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
