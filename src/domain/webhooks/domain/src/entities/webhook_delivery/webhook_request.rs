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
pub struct WebhookRequest {
    pub headers: Vec<(http::header::HeaderName, String)>,
    pub started_at: DateTime<Utc>,
}

impl WebhookRequest {
    pub fn new<I>(headers: I, started_at: DateTime<Utc>) -> Self
    where
        I: IntoIterator<Item = (http::header::HeaderName, String)>,
    {
        Self {
            headers: headers.into_iter().collect(),
            started_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
