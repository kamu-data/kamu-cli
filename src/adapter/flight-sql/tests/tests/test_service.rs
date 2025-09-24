// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::net::SocketAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::prelude::*;
use futures::TryStreamExt;
use indoc::indoc;
use kamu_accounts::testing::MockAuthenticationService;
use kamu_accounts::{Account, AuthenticationService, GetAccountInfoError};
use kamu_adapter_flight_sql::*;
use kamu_core::{MockQueryService, QueryService};
use odf::utils::data::DataFrameExt;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlightServer {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Drop for FlightServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn run_server() -> FlightServer {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("test", "public"),
    );

    ctx.sql(indoc!(
        "
        create table test (id int not null, name string not null)
        as values (1, 'a'), (2, 'b')
        ",
    ))
    .await
    .unwrap();

    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_account_by_token()
        .with(mockall::predicate::eq("valid-token".to_string()))
        .returning(|_| Ok(Account::dummy()));
    mock_authentication_service
        .expect_account_by_token()
        .with(mockall::predicate::eq("invalid-token".to_string()))
        .returning(|_| {
            Err(GetAccountInfoError::AccessToken(
                kamu_accounts::AccessTokenError::Invalid("foo".into()),
            ))
        });

    let mut query_svc: kamu_core::MockQueryService = kamu_core::MockQueryService::new();
    query_svc
        .expect_create_session()
        .return_once(move || Ok(ctx));

    let mut b = dill::Catalog::builder();

    b.add::<SessionAuthAnonymous>()
        .add_value(SessionAuthConfig {
            allow_anonymous: true,
        })
        .add_value(mock_authentication_service)
        .bind::<dyn AuthenticationService, MockAuthenticationService>()
        .add_value(query_svc)
        .bind::<dyn QueryService, MockQueryService>()
        .add::<SessionManagerSingleton>()
        .add::<SessionManagerSingletonState>()
        .add_value(
            kamu_adapter_flight_sql::sql_info::default_sql_info()
                .build()
                .unwrap(),
        )
        .add::<KamuFlightSqlService>();

    database_common::NoOpDatabasePlugin::init_database_components(&mut b);

    let catalog = b.build();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tracing::info!("Listening on {addr:?}");

    let service = Server::builder()
        .layer(tonic::service::interceptor::InterceptorLayer::new(
            move |mut req: tonic::Request<()>| {
                req.extensions_mut().insert(catalog.clone());
                Ok(req)
            },
        ))
        .layer(AuthenticationLayer::new())
        .add_service(FlightServiceServer::new(KamuFlightSqlServiceWrapper))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

    let task = tokio::task::spawn(service);

    FlightServer { addr, task }
}

async fn get_client(addr: &SocketAddr) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("error connecting");

    FlightSqlServiceClient::new(channel)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_auth_disabled() {
    let server = run_server().await;
    let mut client = get_client(&server.addr).await;

    assert_matches!(client.handshake("admin", "zzz").await, Err(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_invalid_bearer_token() {
    let server = run_server().await;
    let mut client = get_client(&server.addr).await;
    client.set_token("invalid-token".to_string());

    assert_matches!(
        client.execute("select * from test".to_string(), None).await,
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_statement_anonymous() {
    let server = run_server().await;

    let mut client = get_client(&server.addr).await;
    client.handshake("anonymous", "").await.unwrap();

    let fi = client
        .execute("select * from test".to_string(), None)
        .await
        .unwrap();

    let mut record_batches: Vec<_> = client
        .do_get(fi.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(record_batches.len(), 1);

    let ctx = SessionContext::new();
    let df: DataFrameExt = ctx
        .read_batch(record_batches.pop().unwrap())
        .unwrap()
        .into();

    odf::utils::testing::assert_schema_eq(
        df.schema(),
        indoc!(
            "
            message arrow_schema {
              REQUIRED INT32 id;
              REQUIRED BYTE_ARRAY name (STRING);
            }
            "
        ),
    );
    odf::utils::testing::assert_data_eq(
        df,
        indoc!(
            "
            +----+------+
            | id | name |
            +----+------+
            | 1  | a    |
            | 2  | b    |
            +----+------+
            "
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_statement_bearer() {
    let server = run_server().await;

    let mut client = get_client(&server.addr).await;
    client.set_token("valid-token".to_string());

    let fi = client
        .execute("select * from test".to_string(), None)
        .await
        .unwrap();

    let mut record_batches: Vec<_> = client
        .do_get(fi.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(record_batches.len(), 1);

    let ctx = SessionContext::new();
    let df: DataFrameExt = ctx
        .read_batch(record_batches.pop().unwrap())
        .unwrap()
        .into();

    odf::utils::testing::assert_schema_eq(
        df.schema(),
        indoc!(
            "
            message arrow_schema {
              REQUIRED INT32 id;
              REQUIRED BYTE_ARRAY name (STRING);
            }
            "
        ),
    );
    odf::utils::testing::assert_data_eq(
        df,
        indoc!(
            "
            +----+------+
            | id | name |
            +----+------+
            | 1  | a    |
            | 2  | b    |
            +----+------+
            "
        ),
    )
    .await;
}
