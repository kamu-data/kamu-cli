// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::EventID;
use internal_error::{InternalError, ResultIntoInternal};
use serde::de::DeserializeOwned;

use crate::{PendingStatusFromSpec, ResourceID, ResourceMetadata};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub resource_id: ResourceID,
    pub kind: String,
    pub api_version: String,
    pub metadata: ResourceMetadata,

    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,

    pub last_reconciled_at: Option<DateTime<Utc>>,
    pub last_event_id: Option<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceSnapshotStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<ResourceSnapshot, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_typed_resource_snapshot<TSpec, TStatus>(
    snapshot: ResourceSnapshot,
) -> Result<(ResourceID, ResourceMetadata, TSpec, TStatus), InternalError>
where
    TSpec: DeserializeOwned,
    TStatus: DeserializeOwned + PendingStatusFromSpec<TSpec>,
{
    let spec = serde_json::from_value(snapshot.spec).int_err()?;
    let status = match snapshot.status {
        Some(status) => serde_json::from_value(status).int_err()?,
        None => TStatus::pending_from_spec(&spec),
    };

    Ok((snapshot.resource_id, snapshot.metadata, spec, status))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
