// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;

use arrow_flight::flight_service_server::FlightServiceServer;
use kamu_adapter_flight_sql::{AuthPolicyLayer, AuthenticationLayer};

use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub struct FlightSqlServiceFactory {
    catalog: dill::Catalog,
}

impl FlightSqlServiceFactory {
    pub async fn start(
        &self,
        address: Option<IpAddr>,
        port: Option<u16>,
        allow_anonymous: bool,
    ) -> Result<FlightSqlService, CLIError> {
        assert_matches!(
            self.catalog
                .get_one::<kamu_accounts::CurrentAccountSubject>(),
            Err(dill::InjectionError::Unregistered(_)),
            "FlightSqlServiceFactory must be constructed from the base catalog"
        );

        let listener = tokio::net::TcpListener::bind((
            address.unwrap_or("127.0.0.1".parse().unwrap()),
            port.unwrap_or(0),
        ))
        .await
        .unwrap();

        let address = listener.local_addr().unwrap();

        // This catalog will be attached to every request by the middleware layer
        let catalog = self.catalog.clone();

        let server_future = tonic::transport::Server::builder()
            .layer(observability::tonic::grpc_layer())
            .layer(tonic::service::interceptor(
                move |mut req: tonic::Request<()>| {
                    req.extensions_mut().insert(catalog.clone());
                    Ok(req)
                },
            ))
            .layer(AuthenticationLayer::new())
            .layer(AuthPolicyLayer::new(allow_anonymous))
            .add_service(FlightServiceServer::new(
                kamu_adapter_flight_sql::KamuFlightSqlServiceWrapper,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

        tracing::info!("FlightSQL is listening on {:?}", address);

        Ok(FlightSqlService {
            address,
            server_future: Box::pin(server_future),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlightSqlService {
    address: SocketAddr,
    server_future: Pin<Box<dyn Future<Output = Result<(), tonic::transport::Error>> + Send>>,
}

impl FlightSqlService {
    pub fn local_addr(&self) -> &SocketAddr {
        &self.address
    }

    pub async fn wait(self) -> Result<(), tonic::transport::Error> {
        self.server_future.await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
