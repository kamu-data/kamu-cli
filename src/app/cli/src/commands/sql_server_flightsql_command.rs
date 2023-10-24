// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use console::style as s;
use datafusion::prelude::SessionContext;
use internal_error::*;
use kamu::domain::QueryService;
use kamu_adapter_flight_sql::{SessionFactory, Token};
use tokio::net::TcpListener;
use tonic::transport::Server;
use tonic::Status;

use super::{CLIError, Command};

pub struct SqlServerFlightSqlCommand {
    address: IpAddr,
    port: u16,
    query_svc: Arc<dyn QueryService>,
}

impl SqlServerFlightSqlCommand {
    pub fn new(address: IpAddr, port: u16, query_svc: Arc<dyn QueryService>) -> Self {
        Self {
            address,
            port,
            query_svc,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlServerFlightSqlCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let kamu_service = kamu_adapter_flight_sql::KamuFlightSqlService::builder()
            .with_server_name(crate::BINARY_NAME, crate::VERSION)
            .with_session_factory(Arc::new(SessionFactoryImpl {
                query_svc: self.query_svc.clone(),
            }))
            .build();

        let listener = TcpListener::bind((self.address, self.port)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tracing::info!("Listening on {addr:?}");

        eprintln!(
            "{} {}",
            s("Flight SQL server is now running on:").green().bold(),
            s(addr).bold(),
        );
        eprintln!(
            "{}",
            s(format!(
                indoc::indoc!(
                    r#"
                    To connect via JDBC:
                      - Get latest driver from https://central.sonatype.com/artifact/org.apache.arrow/flight-sql-jdbc-driver
                      - Install driver in your client application
                      - Connect using URL: jdbc:arrow-flight-sql://{}?useEncryption=false
                      - Use 'kamu' as login and password"#
                ),
                addr
            )).yellow()
        );
        eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());

        Server::builder()
            .add_service(FlightServiceServer::new(kamu_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .int_err()?;

        Ok(())
    }
}

struct SessionFactoryImpl {
    query_svc: Arc<dyn QueryService>,
}

#[async_trait::async_trait]
impl SessionFactory for SessionFactoryImpl {
    async fn authenticate(&self, username: &str, password: &str) -> Result<Token, Status> {
        if username == "kamu" && password == "kamu" {
            Ok(String::new())
        } else {
            Err(Status::unauthenticated("Invalid credentials!"))
        }
    }

    async fn get_context(&self, _token: &Token) -> Result<Arc<SessionContext>, Status> {
        Ok(Arc::new(self.query_svc.create_session().await.unwrap()))
    }
}
