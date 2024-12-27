// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use console::style as s;
use internal_error::*;
use tokio::net::TcpListener;
use tonic::transport::Server;

use super::{CLIError, Command};

pub struct SqlServerFlightSqlCommand {
    catalog: dill::Catalog,
    address: Option<IpAddr>,
    port: Option<u16>,
}

impl SqlServerFlightSqlCommand {
    pub fn new(catalog: dill::Catalog, address: Option<IpAddr>, port: Option<u16>) -> Self {
        Self {
            catalog,
            address,
            port,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlServerFlightSqlCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let listener = TcpListener::bind((
            self.address.unwrap_or("127.0.0.1".parse().unwrap()),
            self.port.unwrap_or(0),
        ))
        .await
        .unwrap();

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

        // This catalog will be attached to every request by the middleware layer
        let catalog = self.catalog.clone();

        Server::builder()
            .layer(tonic::service::interceptor(
                move |mut req: tonic::Request<()>| {
                    req.extensions_mut().insert(catalog.clone());
                    Ok(req)
                },
            ))
            .add_service(FlightServiceServer::new(
                kamu_adapter_flight_sql::KamuFlightSqlService::builder()
                    .with_server_name(crate::BINARY_NAME, crate::VERSION)
                    .build(),
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .int_err()?;

        Ok(())
    }
}
