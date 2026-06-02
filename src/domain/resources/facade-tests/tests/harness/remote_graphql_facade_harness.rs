// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Extension;
use kamu_accounts::CurrentAccountSubject;
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};
use kamu_resources_facade::{RemoteGraphqlResourceFacadeImpl, ResourceFacade};
use strum::IntoEnumIterator;

use super::facade_harness_trait::{FacadeContractHarness, TestAccount};
use super::local_facade_harness::LocalFacadeHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteGraphqlFacadeHarness {
    local: LocalFacadeHarness,
    server_addr: SocketAddr,
    server_handle: tokio::task::JoinHandle<()>,
}

impl RemoteGraphqlFacadeHarness {
    pub async fn new() -> Self {
        let local = LocalFacadeHarness::new().await;
        let (server_addr, server_handle) = Self::start_test_server(&local).await;

        Self {
            local,
            server_addr,
            server_handle,
        }
    }

    async fn start_test_server(
        local: &LocalFacadeHarness,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let mut token_to_catalog: HashMap<String, dill::Catalog> = HashMap::new();

        for account in TestAccount::iter() {
            let account_name = local.account_name(account);
            let account_id = local.account_id(account);
            let subject = CurrentAccountSubject::logged(account_id, account_name);

            let mut b = dill::CatalogBuilder::new_chained(local.base_catalog());
            b.add_value(subject);
            kamu_resources_facade::register_dependencies(&mut b);
            let catalog = b.build();
            token_to_catalog.insert(test_token(account), catalog);
        }

        let token_map = Arc::new(token_to_catalog);
        let schema = kamu_adapter_graphql::schema_quiet();

        let app = axum::Router::new()
            .route("/graphql", axum::routing::post(test_graphql_handler))
            .layer(Extension(schema))
            .layer(Extension(token_map));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .unwrap();
        });

        (addr, handle)
    }

    fn server_url(&self) -> url::Url {
        url::Url::parse(&format!("http://{}", self.server_addr)).unwrap()
    }

    #[expect(dead_code)]
    pub fn base_catalog(&self) -> &dill::Catalog {
        self.local.base_catalog()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_graphql_handler(
    Extension(schema): Extension<kamu_adapter_graphql::Schema>,
    Extension(token_map): Extension<Arc<HashMap<String, dill::Catalog>>>,
    headers: axum::http::HeaderMap,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    let token = extract_bearer_token(&headers);
    let catalog = token.and_then(|t| token_map.get(&t)).cloned().unwrap();

    let request = req
        .into_inner()
        .data(account_entity_data_loader(&catalog))
        .data(dataset_handle_data_loader(&catalog))
        .data(catalog);

    schema.execute(request).await.into()
}

fn extract_bearer_token(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_token(account: TestAccount) -> String {
    format!("{}-test-token", account.name())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Drop for RemoteGraphqlFacadeHarness {
    fn drop(&mut self) {
        self.server_handle.abort();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FacadeContractHarness for RemoteGraphqlFacadeHarness {
    fn facade_for(&self, account: TestAccount) -> Arc<dyn ResourceFacade> {
        Arc::new(RemoteGraphqlResourceFacadeImpl::new(
            &self.server_url(),
            Some(test_token(account)),
        ))
    }

    fn account_id(&self, account: TestAccount) -> odf::AccountID {
        self.local.account_id(account)
    }

    fn account_name(&self, account: TestAccount) -> odf::AccountName {
        self.local.account_name(account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
