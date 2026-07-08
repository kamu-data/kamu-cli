// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::EventID;
use internal_error::{InternalError, ResultIntoInternal};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{
    ResourceHeaders,
    ResourceID,
    ResourceStatus,
    TypeUri,
    new_pending_resource_status,
    resource_status_from_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub id: ResourceID,
    pub schema: TypeUri,
    #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceHeaders")]
    pub headers: ResourceHeaders,

    pub spec: serde_json::Value,
    #[serde_as(as = "Option<odf::metadata::serde::yaml::resource::ResourceStatus>")]
    pub status: Option<ResourceStatus>,

    pub last_event_id: Option<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSnapshot {
    pub fn basic_status(&self) -> Option<ResourceStatus> {
        self.status.clone()
    }

    pub fn basic_status_from_json(value: &serde_json::Value) -> Option<ResourceStatus> {
        resource_status_from_json(value)
    }

    pub fn check_homogeneous(snapshots: &[ResourceSnapshot]) -> bool {
        if snapshots.is_empty() {
            return true;
        }

        let first_schema = &snapshots[0].schema;

        snapshots
            .iter()
            .all(|snapshot| snapshot.schema == *first_schema)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceSnapshotStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<ResourceSnapshot, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_typed_resource_snapshot<TSpec>(
    snapshot: ResourceSnapshot,
) -> Result<(ResourceID, ResourceHeaders, TSpec, ResourceStatus), InternalError>
where
    TSpec: DeserializeOwned,
{
    let spec = serde_json::from_value(snapshot.spec).int_err()?;
    let status = snapshot.status.unwrap_or_else(new_pending_resource_status);

    Ok((snapshot.id, snapshot.headers, spec, status))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_typed_resource_snapshot<TSpec>(
    id: ResourceID,
    schema: &TypeUri,
    headers: ResourceHeaders,
    spec: &TSpec,
    status: &ResourceStatus,
    last_event_id: Option<EventID>,
) -> Result<ResourceSnapshot, InternalError>
where
    TSpec: Serialize,
{
    let spec = serde_json::to_value(spec).int_err()?;

    Ok(ResourceSnapshot {
        id,
        schema: schema.clone(),
        headers,
        spec,
        status: Some(status.clone()),
        last_event_id,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
