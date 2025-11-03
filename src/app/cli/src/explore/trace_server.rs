// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::response::IntoResponse;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A simple file server that serves trace files on 9001 port for us to open a
/// browser tab with Perfetto UI and immediately display the trace.
/// The server will exit immediately after handing over the file to Perfetto.
pub struct TraceServer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TraceServer {
    // MUST be 9001 to satisfy ui.perfetto.dev Content Security Policy
    const HTTP_PORT: u16 = 9001;

    pub fn new() -> Self {
        Self
    }

    pub fn perfetto_url(&self) -> url::Url {
        url::Url::parse("https://ui.perfetto.dev/#!/?url=http%3A%2F%2F127.0.0.1%3A9001%2F").unwrap()
    }

    pub async fn serve(&self, trace_path: impl Into<PathBuf>) -> Result<(), std::io::Error> {
        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), Self::HTTP_PORT));
        let listener = tokio::net::TcpListener::bind(addr).await?;

        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();

        let app = axum::Router::new()
            .route("/", axum::routing::get(download_trace))
            .layer(
                tower::ServiceBuilder::new().layer(tower_http::trace::TraceLayer::new_for_http()),
            )
            .layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(vec![http::Method::GET, http::Method::POST])
                    .allow_headers(tower_http::cors::Any),
            )
            .with_state(StateInner::new(trace_path, shutdown_sender));

        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(async {
                shutdown_receiver.await.ok();
            })
            .await
    }

    pub async fn maybe_serve_in_browser(
        trace_path: impl Into<PathBuf>,
    ) -> Result<(), std::io::Error> {
        let trace_path = trace_path.into();
        let trace_server = Self::new();
        let srv = trace_server.serve(&trace_path);

        // Try open in a browser
        if webbrowser::open(trace_server.perfetto_url().as_str()).is_ok() {
            eprintln!(
                "{}",
                console::style(format!(
                    "Open this URL in a browser to see the trace (app will exit once the file is \
                     served):\n  {}",
                    trace_server.perfetto_url()
                ))
                .yellow()
                .bold()
            );

            // This will wait for Perfetto UI to fetch the data and will then exit
            srv.await
        } else {
            eprintln!(
                "{}",
                console::style(format!("Trace saved to: {}", trace_path.display()))
                    .yellow()
                    .bold()
            );
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn download_trace(
    axum::extract::State(state): axum::extract::State<State>,
) -> impl IntoResponse {
    let trace_path = {
        let mut state = state.lock().unwrap();

        if let Some(sh) = state.shutdown_sender.take() {
            sh.send(()).unwrap();
        }

        state.trace_path.clone()
    };

    let file = match tokio::fs::File::open(trace_path).await {
        Ok(file) => file,
        Err(err) => {
            return Err((
                http::StatusCode::NOT_FOUND,
                format!("File not found: {err}"),
            ));
        }
    };

    Ok(axum::body::Body::from_stream(
        tokio_util::io::ReaderStream::new(file),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct StateInner {
    trace_path: PathBuf,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

type State = Arc<Mutex<StateInner>>;

impl StateInner {
    fn new(
        trace_path: impl Into<PathBuf>,
        shutdown_sender: tokio::sync::oneshot::Sender<()>,
    ) -> State {
        Arc::new(Mutex::new(Self {
            trace_path: trace_path.into(),
            shutdown_sender: Some(shutdown_sender),
        }))
    }
}
