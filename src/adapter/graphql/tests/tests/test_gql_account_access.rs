// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indoc::indoc;
use kamu_accounts::{
    AccessTokenService,
    AccountConfig,
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_NAME,
    TEST_ACCOUNT_ID,
};
use kamu_accounts_inmem::InMemoryAccessTokenRepository;
use kamu_accounts_services::utils::AccountAuthorizationHelperImpl;
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    DeleteAccountUseCaseImpl,
    ModifyAccountPasswordUseCaseImpl,
};
use kamu_auth_rebac::{AccountPropertyName, RebacService, boolean_property_value};
use kamu_core::TenancyConfig;
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, CreateDatasetUseCaseOptions};
use kamu_flow_system::FlowSystemEventAgent;
use messaging_outbox::OutboxProvider;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;

use crate::utils::{
    BaseGQLDatasetHarness,
    BaseGQLFlowHarness,
    BaseGQLFlowRunsHarness,
    FlowRunsHarnessOverrides,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Every field of `Account` and every operation of `AccountMut` is exercised
// against the same target account by four callers: anonymous, the account
// itself, a different account, and an administrator.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNT_ACCESS_ERROR: &str = "Account access error";
const STAFF_ONLY_ERROR: &str = "Access restricted to administrators only";
const UNAUTHENTICATED_ERROR: &str = "Unauthenticated";
const UNAUTHORIZED_ERROR: &str = "Unauthorized";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Account: queries
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_public_profile_fields() {
    for field in [
        "id",
        "accountName",
        "displayName",
        "accountType",
        "accountProvider",
        "avatarUrl",
    ] {
        let access = AccountAccessHarness::query_access_matrix(field).await;

        assert_eq!(access, AccessMatrix::everyone(), "{field}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_is_admin_hidden_from_other_accounts() {
    let data = AccountAccessHarness::account_data(Caller::Anonymous, "isAdmin").await;
    assert_eq!(data, serde_json::json!({ "isAdmin": null }));

    let data = AccountAccessHarness::account_data(Caller::Owner, "isAdmin").await;
    assert_eq!(data, serde_json::json!({ "isAdmin": false }));

    let data = AccountAccessHarness::account_data(Caller::OtherAccount, "isAdmin").await;
    assert_eq!(data, serde_json::json!({ "isAdmin": null }));

    let data = AccountAccessHarness::account_data(Caller::Admin, "isAdmin").await;
    assert_eq!(data, serde_json::json!({ "isAdmin": false }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_email() {
    let access = AccountAccessHarness::query_access_matrix("email").await;

    assert_eq!(access, AccessMatrix::owner_only(ACCOUNT_ACCESS_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flows() {
    for selection in [
        "flows { processes { primaryRollup { total } } }",
        "flows { processes { webhookRollup { total } } }",
        "flows { processes { fullRollup { total } } }",
        "flows { processes { primaryCards { nodes { flowType } } } }",
        "flows { processes { webhookCards { nodes { name } } } }",
        "flows { processes { allCards { nodes { __typename } } } }",
        "flows { runs { listFlows { nodes { flowId } } } }",
        "flows { runs { listDatasetsWithFlow { nodes { name } } } }",
        "flows { triggers { allPaused } }",
    ] {
        let access = AccountAccessHarness::query_access_matrix(selection).await;

        assert_eq!(
            access,
            AccessMatrix::owner_and_admin(ACCOUNT_ACCESS_ERROR),
            "{selection}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flows_include_private_datasets_for_owner_and_admin() {
    let selection = indoc!(
        r#"
        flows {
            processes {
                fullRollup { total }
                allCards { nodes { ... on DatasetFlowProcess { dataset { name } } } }
            }
            runs {
                listFlows { nodes { flowId } }
                listDatasetsWithFlow { nodes { name } }
            }
            triggers { allPaused }
        }
        "#
    );
    let expected = serde_json::json!({
        "flows": {
            "processes": {
                "fullRollup": { "total": 1 },
                "allCards": { "nodes": [{ "dataset": { "name": "private-dataset" } }] },
            },
            "runs": {
                "listFlows": { "nodes": [{ "flowId": "0" }] },
                "listDatasetsWithFlow": { "nodes": [{ "name": "private-dataset" }] },
            },
            "triggers": { "allPaused": false },
        }
    });

    let data = AccountAccessHarness::account_data(Caller::Owner, selection).await;
    assert_eq!(data, expected);

    let data = AccountAccessHarness::account_data(Caller::Admin, selection).await;
    assert_eq!(data, expected);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_access_tokens() {
    let access = AccountAccessHarness::query_access_matrix(
        "accessTokens { listAccessTokens { nodes { id name } } }",
    )
    .await;

    assert_eq!(access, AccessMatrix::owner_only(ACCOUNT_ACCESS_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_usage() {
    let access = AccountAccessHarness::query_access_matrix(
        "usage { storage { totalRecords totalSizeBytes } }",
    )
    .await;

    assert_eq!(access, AccessMatrix::owner_and_admin(ACCOUNT_ACCESS_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_quotas() {
    let access = AccountAccessHarness::query_access_matrix(
        "quotas { user { storage { limitTotalBytes } } }",
    )
    .await;

    assert_eq!(access, AccessMatrix::owner_and_admin(ACCOUNT_ACCESS_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owned_datasets() {
    let access =
        AccountAccessHarness::query_access_matrix("ownedDatasets { nodes { name } }").await;

    assert_eq!(access, AccessMatrix::everyone());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owned_datasets_hide_private_datasets() {
    let only_public = ["public-dataset"];
    let all = ["private-dataset", "public-dataset"];

    let names = AccountAccessHarness::owned_dataset_names(Caller::Anonymous).await;
    assert_eq!(names, only_public);

    let names = AccountAccessHarness::owned_dataset_names(Caller::Owner).await;
    assert_eq!(names, all);

    let names = AccountAccessHarness::owned_dataset_names(Caller::OtherAccount).await;
    assert_eq!(names, only_public);

    let names = AccountAccessHarness::owned_dataset_names(Caller::Admin).await;
    assert_eq!(names, all);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountMut: operations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rename() {
    let access = AccountAccessHarness::mutation_access_matrix(
        r#"rename(newName: "renamed-account") { __typename }"#,
    )
    .await;

    assert_eq!(access, AccessMatrix::owner_and_admin(UNAUTHORIZED_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_email() {
    let access = AccountAccessHarness::mutation_access_matrix(
        r#"updateEmail(newEmail: "renamed@example.com") { __typename }"#,
    )
    .await;

    assert_eq!(access, AccessMatrix::owner_and_admin(ACCOUNT_ACCESS_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_password() {
    let access = AccountAccessHarness::mutation_access_matrix(
        r#"modifyPassword(password: "new-password") { __typename }"#,
    )
    .await;

    assert_eq!(access, AccessMatrix::owner_and_admin(UNAUTHENTICATED_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_password_with_confirmation() {
    let access = AccountAccessHarness::mutation_access_matrix(
        r#"modifyPasswordWithConfirmation(oldPassword: $oldPassword, newPassword: "new-password") {
            __typename
        }"#,
    )
    .await;

    assert_eq!(access, AccessMatrix::owner_and_admin(UNAUTHENTICATED_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete() {
    let access = AccountAccessHarness::mutation_access_matrix("delete { __typename }").await;

    assert_eq!(access, AccessMatrix::owner_and_admin(UNAUTHENTICATED_ERROR));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_triggers_mut() {
    for selection in [
        "flows { triggers { pauseAccountDatasetFlows } }",
        "flows { triggers { resumeAccountDatasetFlows } }",
    ] {
        let access = AccountAccessHarness::mutation_access_matrix(selection).await;

        assert_eq!(
            access,
            AccessMatrix::owner_only(ACCOUNT_ACCESS_ERROR),
            "{selection}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_access_tokens_mut() {
    for selection in [
        r#"accessTokens { createAccessToken(tokenName: "new-token") { __typename } }"#,
        "accessTokens { revokeAccessToken(tokenId: $tokenId) { __typename } }",
    ] {
        let access = AccountAccessHarness::mutation_access_matrix(selection).await;

        assert_eq!(
            access,
            AccessMatrix::owner_only(ACCOUNT_ACCESS_ERROR),
            "{selection}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_quotas_mut() {
    let access = AccountAccessHarness::mutation_access_matrix(
        "quotas { setAccountQuotas(quotas: { storage: { limitTotalBytes: 12345 } }) { __typename \
         } }",
    )
    .await;

    assert_eq!(
        access,
        AccessMatrix {
            anonymous: Access::denied(STAFF_ONLY_ERROR),
            owner: Access::denied(STAFF_ONLY_ERROR),
            other_account: Access::denied(STAFF_ONLY_ERROR),
            admin: Access::Allowed,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum Caller {
    Anonymous,
    Owner,
    OtherAccount,
    Admin,
}

#[derive(Debug, PartialEq, Eq)]
enum Access {
    Allowed,
    Denied(String),
}

impl Access {
    fn denied(message: &str) -> Self {
        Self::Denied(message.to_string())
    }

    fn from_response(response: async_graphql::Response) -> Self {
        if response.errors.is_empty() {
            Self::Allowed
        } else {
            let messages = response
                .errors
                .into_iter()
                .map(|e| e.message)
                .collect::<Vec<_>>();
            Self::Denied(messages.join("; "))
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct AccessMatrix {
    anonymous: Access,
    owner: Access,
    other_account: Access,
    admin: Access,
}

impl AccessMatrix {
    fn everyone() -> Self {
        Self {
            anonymous: Access::Allowed,
            owner: Access::Allowed,
            other_account: Access::Allowed,
            admin: Access::Allowed,
        }
    }

    fn owner_only(error: &str) -> Self {
        Self {
            anonymous: Access::denied(error),
            owner: Access::Allowed,
            other_account: Access::denied(error),
            admin: Access::denied(error),
        }
    }

    fn owner_and_admin(error: &str) -> Self {
        Self {
            anonymous: Access::denied(error),
            owner: Access::Allowed,
            other_account: Access::denied(error),
            admin: Access::Allowed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowRunsHarness, base_gql_flow_runs_harness)]
struct AccountAccessHarness {
    base_gql_flow_runs_harness: BaseGQLFlowRunsHarness,
    catalog_other_account: dill::Catalog,
    catalog_admin: dill::Catalog,
    owner_token_id: String,
}

impl AccountAccessHarness {
    fn account_query(selection: &str) -> String {
        indoc!(
            r#"
            query($accountName: AccountName!) {
                accounts {
                    byName(name: $accountName) {
                        <selection>
                    }
                }
            }
            "#
        )
        .replace("<selection>", selection)
    }

    fn account_mutation(selection: &str) -> String {
        let variables = [
            ("$oldPassword", "$oldPassword: AccountPassword!"),
            ("$tokenId", "$tokenId: AccessTokenID!"),
        ]
        .into_iter()
        .filter(|(usage, _)| selection.contains(usage))
        .flat_map(|(_, declaration)| [", ", declaration])
        .collect::<String>();

        indoc!(
            r#"
            mutation($accountName: AccountName!<variables>) {
                accounts {
                    byName(accountName: $accountName) {
                        <selection>
                    }
                }
            }
            "#
        )
        .replace("<variables>", &variables)
        .replace("<selection>", selection)
    }

    /// Owner of the target account is the default test account. It owns a
    /// private dataset with flows, a public dataset, and an access token.
    async fn new() -> Self {
        let account_services_catalog = {
            let mut b = dill::CatalogBuilder::new();
            b.add::<InMemoryAccessTokenRepository>()
                .add::<AccessTokenServiceImpl>()
                .add::<ModifyAccountPasswordUseCaseImpl>()
                .add::<DeleteAccountUseCaseImpl>()
                .add::<AccountAuthorizationHelperImpl>();
            b.build()
        };

        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .base_catalog(&account_services_catalog)
            .outbox_provider(OutboxProvider::Immediate {
                force_immediate: true,
            })
            .build();

        let base_gql_flow_catalog =
            BaseGQLFlowHarness::make_base_gql_flow_catalog(base_gql_harness.catalog());
        let base_gql_flow_runs_catalog = BaseGQLFlowRunsHarness::make_base_gql_flow_runs_catalog(
            &base_gql_flow_catalog,
            FlowRunsHarnessOverrides::default(),
        );
        let base_gql_flow_runs_harness =
            BaseGQLFlowRunsHarness::new(base_gql_harness, base_gql_flow_runs_catalog).await;

        let catalog_other_account = base_gql_flow_runs_harness
            .catalog_for_subject(CurrentAccountSubject::new_test_with(&"other-account"));

        let admin_subject = CurrentAccountSubject::new_test_with(&"admin-account");
        let rebac_service = base_gql_flow_runs_harness
            .catalog_authorized
            .get_one::<dyn RebacService>()
            .unwrap();
        rebac_service
            .set_account_property(
                admin_subject.account_id(),
                AccountPropertyName::IsAdmin,
                &boolean_property_value(true),
            )
            .await
            .unwrap();
        let catalog_admin = base_gql_flow_runs_harness.catalog_for_subject(admin_subject);

        let access_token_service = base_gql_flow_runs_harness
            .catalog_authorized
            .get_one::<dyn AccessTokenService>()
            .unwrap();
        let owner_token = access_token_service
            .create_access_token("owner-token", &TEST_ACCOUNT_ID)
            .await
            .unwrap();

        let harness = Self {
            base_gql_flow_runs_harness,
            catalog_other_account,
            catalog_admin,
            owner_token_id: owner_token.id.to_string(),
        };
        harness.create_owner_datasets().await;
        harness
    }

    async fn create_owner_datasets(&self) {
        let schema = kamu_adapter_graphql::schema_quiet();

        let private_dataset = self
            .create_root_dataset(odf::DatasetAlias::new(
                Some(DEFAULT_ACCOUNT_NAME.clone()),
                odf::DatasetName::new_unchecked("private-dataset"),
            ))
            .await;
        self.set_time_delta_trigger(
            &private_dataset.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            None,
        )
        .execute(&schema, &self.catalog_authorized)
        .await;
        self.trigger_ingest_flow_mutation(&private_dataset.dataset_handle.id)
            .execute(&schema, &self.catalog_authorized)
            .await;

        self.catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap()
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(odf::DatasetAlias::new(
                        Some(DEFAULT_ACCOUNT_NAME.clone()),
                        odf::DatasetName::new_unchecked("public-dataset"),
                    ))
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                CreateDatasetUseCaseOptions {
                    dataset_visibility: odf::DatasetVisibility::Public,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        self.catalog_authorized
            .get_one::<dyn FlowSystemEventAgent>()
            .unwrap()
            .catchup_remaining_events()
            .await
            .unwrap();
    }

    fn catalog_for(&self, caller: Caller) -> &dill::Catalog {
        match caller {
            Caller::Anonymous => &self.catalog_anonymous,
            Caller::Owner => &self.catalog_authorized,
            Caller::OtherAccount => &self.catalog_other_account,
            Caller::Admin => &self.catalog_admin,
        }
    }

    async fn execute_as(&self, caller: Caller, request_code: &str) -> async_graphql::Response {
        let request = async_graphql::Request::new(request_code).variables(
            async_graphql::Variables::from_json(serde_json::json!({
                "accountName": DEFAULT_ACCOUNT_NAME.as_str(),
                "tokenId": self.owner_token_id,
                "oldPassword": AccountConfig::generate_password(&DEFAULT_ACCOUNT_NAME)
                    .as_str(),
            })),
        );

        self.execute_query(request, self.catalog_for(caller)).await
    }

    /// Each caller gets a fresh harness, so mutations cannot affect each other.
    async fn access_as(caller: Caller, request_code: &str) -> Access {
        let harness = Self::new().await;
        let response = harness.execute_as(caller, request_code).await;

        Access::from_response(response)
    }

    async fn query_access_matrix(selection: &str) -> AccessMatrix {
        Self::access_matrix(&Self::account_query(selection)).await
    }

    async fn mutation_access_matrix(selection: &str) -> AccessMatrix {
        Self::access_matrix(&Self::account_mutation(selection)).await
    }

    async fn access_matrix(request_code: &str) -> AccessMatrix {
        AccessMatrix {
            anonymous: Self::access_as(Caller::Anonymous, request_code).await,
            owner: Self::access_as(Caller::Owner, request_code).await,
            other_account: Self::access_as(Caller::OtherAccount, request_code).await,
            admin: Self::access_as(Caller::Admin, request_code).await,
        }
    }

    async fn account_data(caller: Caller, selection: &str) -> serde_json::Value {
        let harness = Self::new().await;
        let response = harness
            .execute_as(caller, &Self::account_query(selection))
            .await;
        assert!(response.is_ok(), "{response:?}");

        let mut json = response.data.into_json().unwrap();
        json["accounts"]["byName"].take()
    }

    async fn owned_dataset_names(caller: Caller) -> Vec<String> {
        let harness = Self::new().await;
        let response = harness
            .execute_as(
                caller,
                &Self::account_query("ownedDatasets { nodes { name } }"),
            )
            .await;
        assert!(response.is_ok(), "{response:?}");

        let json = response.data.into_json().unwrap();
        json["accounts"]["byName"]["ownedDatasets"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| node["name"].as_str().unwrap().to_string())
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
