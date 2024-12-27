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
use dill::Component;
use futures::TryStreamExt;
use indoc::indoc;
use kamu_adapter_flight_sql::*;
use kamu_core::{MockQueryService, QueryService};
use tokio::net::TcpListener;
use tonic::service::interceptor;
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

    let mut query_svc: kamu_core::MockQueryService = kamu_core::MockQueryService::new();
    query_svc
        .expect_create_session()
        .return_once(move || Ok(ctx));

    let catalog = dill::Catalog::builder()
        .add_builder(
            SessionAuthBasicPredefined::builder()
                .with_accounts_passwords([("admin".to_string(), "password".to_string())].into()),
        )
        .bind::<dyn SessionAuth, SessionAuthBasicPredefined>()
        .add_value(query_svc)
        .bind::<dyn QueryService, MockQueryService>()
        .add::<SessionManagerSingleton>()
        .add::<SessionManagerSingletonState>()
        .build();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tracing::info!("Listening on {addr:?}");

    let service = Server::builder()
        .layer(interceptor(move |mut req: tonic::Request<()>| {
            req.extensions_mut().insert(catalog.clone());
            Ok(req)
        }))
        .add_service(FlightServiceServer::new(
            KamuFlightSqlService::builder().build(),
        ))
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
async fn test_auth_error() {
    let server = run_server().await;
    let mut client = get_client(&server.addr).await;

    assert_matches!(client.handshake("admin", "zzz").await, Err(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_statement() {
    let server = run_server().await;

    let mut client = get_client(&server.addr).await;
    client.handshake("admin", "password").await.unwrap();

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
    let df = ctx.read_batch(record_batches.pop().unwrap()).unwrap();

    kamu_data_utils::testing::assert_schema_eq(
        df.schema(),
        indoc!(
            "
            message arrow_schema {
              OPTIONAL INT32 id;
              OPTIONAL BYTE_ARRAY name (STRING);
            }
            "
        ),
    );
    kamu_data_utils::testing::assert_data_eq(
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
