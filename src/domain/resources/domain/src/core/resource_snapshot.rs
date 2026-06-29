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
use serde::{Deserialize, Serialize};

use crate::{
    PendingStatusFromSpec,
    ResourceHeaders,
    ResourceStatus,
    ResourceStatusLike,
    ResourceUID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub uid: ResourceUID,
    pub kind: String,
    pub api_version: String,
    pub headers: ResourceHeaders,

    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,

    pub last_reconciled_at: Option<DateTime<Utc>>,
    pub last_event_id: Option<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSnapshot {
    pub fn basic_status(&self) -> Option<ResourceStatus> {
        self.status.as_ref().and_then(ResourceStatus::from_json)
    }

    pub fn check_homogeneous(snapshots: &[ResourceSnapshot]) -> bool {
        if snapshots.is_empty() {
            return true;
        }

        let first_kind = &snapshots[0].kind;
        let first_api_version = &snapshots[0].api_version;

        snapshots.iter().all(|snapshot| {
            snapshot.kind == *first_kind && snapshot.api_version == *first_api_version
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceSnapshotStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<ResourceSnapshot, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_typed_resource_snapshot<TSpec, TStatus>(
    snapshot: ResourceSnapshot,
) -> Result<(ResourceUID, ResourceHeaders, TSpec, TStatus), InternalError>
where
    TSpec: DeserializeOwned,
    TStatus: DeserializeOwned + PendingStatusFromSpec<TSpec>,
{
    let spec = serde_json::from_value(snapshot.spec).int_err()?;
    let status = match snapshot.status {
        Some(status) => serde_json::from_value(status).int_err()?,
        None => TStatus::pending_from_spec(&spec),
    };

    Ok((snapshot.uid, snapshot.headers, spec, status))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_typed_resource_snapshot<TSpec, TStatus>(
    uid: ResourceUID,
    kind: &'static str,
    api_version: &'static str,
    headers: ResourceHeaders,
    spec: &TSpec,
    status: &TStatus,
    last_event_id: Option<EventID>,
) -> Result<ResourceSnapshot, InternalError>
where
    TSpec: Serialize,
    TStatus: Serialize + ResourceStatusLike,
{
    let spec = serde_json::to_value(spec).int_err()?;
    let status_json = serde_json::to_value(status).int_err()?;
    let last_reconciled_at = status.resource_status().last_reconciled_at();

    Ok(ResourceSnapshot {
        uid,
        kind: kind.to_string(),
        api_version: api_version.to_string(),
        headers,
        spec,
        status: Some(status_json),
        last_reconciled_at,
        last_event_id,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
