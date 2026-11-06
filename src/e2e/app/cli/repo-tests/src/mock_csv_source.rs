// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An HTTP source of one CSV file without `ETag` or `Last-Modified`, so a
/// dataset polling it records no source state
pub(crate) struct MockCsvSource {
    server: MockServer,
}

impl MockCsvSource {
    pub(crate) async fn start(csv: &str) -> Self {
        let source = Self {
            server: MockServer::start().await,
        };
        source.mount(csv).await;
        source
    }

    /// Serves `csv` instead of the earlier content
    pub(crate) async fn replace(&self, csv: &str) {
        self.server.reset().await;
        self.mount(csv).await;
    }

    pub(crate) fn url(&self) -> String {
        format!("{}/data.csv", self.server.uri())
    }

    async fn mount(&self, csv: &str) {
        Mock::given(method("GET"))
            .and(path("/data.csv"))
            .respond_with(ResponseTemplate::new(200).set_body_string(csv))
            .mount(&self.server)
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
