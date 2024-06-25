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
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::prelude::*;
use futures::TryStreamExt;
use indoc::indoc;
use kamu_adapter_flight_sql::*;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};
use tonic::Status;

struct TestSessionFactory;

#[async_trait::async_trait]
impl SessionFactory for TestSessionFactory {
    async fn authenticate(&self, username: &str, password: &str) -> Result<Token, Status> {
        if username == "admin" && password == "password" {
            Ok("<token>".to_string())
        } else {
            Err(Status::unauthenticated("Invalid credentials!"))
        }
    }

    async fn get_context(&self, _token: &Token) -> Result<Arc<SessionContext>, Status> {
        let cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("test", "public");
        let ctx = SessionContext::new_with_config(cfg);

        ctx.sql(indoc!(
            "
            create table test (id int not null, name string not null)
            as values (1, 'a'), (2, 'b')
            ",
        ))
        .await
        .unwrap();

        Ok(Arc::new(ctx))
    }
}

struct FlightServer {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Drop for FlightServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn run_server(service: KamuFlightSqlService) -> FlightServer {
    let svc = FlightServiceServer::new(service);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tracing::info!("Listening on {addr:?}");

    let service = Server::builder()
        .add_service(svc)
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

////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_auth_error() {
    let service = KamuFlightSqlService::builder()
        .with_session_factory(Arc::new(TestSessionFactory))
        .build();

    let server = run_server(service).await;
    let mut client = get_client(&server.addr).await;

    assert_matches!(client.handshake("admin", "zzz").await, Err(_));
}

#[test_log::test(tokio::test)]
async fn test_statement() {
    let service = KamuFlightSqlService::builder()
        .with_session_factory(Arc::new(TestSessionFactory))
        .build();

    let server = run_server(service).await;

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
