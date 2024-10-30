// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::Extension;
use axum::response::Json;
use chrono::{DateTime, Utc};
use dill::Catalog;
use http_common::*;
use time_source::SystemTimeSourceStub;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetSystemTimeRequest {
    pub new_system_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "info", skip_all)]
pub async fn set_system_time_handler(
    Extension(catalog): Extension<Catalog>,
    Json(request): Json<SetSystemTimeRequest>,
) -> Result<(), ApiError> {
    let system_time_source_stub = catalog.get_one::<SystemTimeSourceStub>().unwrap();

    system_time_source_stub.set(request.new_system_time);

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
