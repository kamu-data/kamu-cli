// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct WebhookResponse {
    pub status_code: http::StatusCode,
    pub headers: Vec<(http::header::HeaderName, String)>,
    pub body: String,
    pub finished_at: DateTime<Utc>,
}

impl WebhookResponse {
    pub fn new(
        status_code: http::StatusCode,
        headers: Vec<(http::header::HeaderName, String)>,
        body: String,
        finished_at: DateTime<Utc>,
    ) -> Self {
        Self {
            status_code,
            headers,
            body,
            finished_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
