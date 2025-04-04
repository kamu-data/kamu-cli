// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "metadata_event_type")
)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "camelCase")]
pub enum MetadataEventType {
    AddData,
    ExecuteTransform,
    Seed,
    SetPollingSource,
    SetVocab,
    SetAttachments,
    SetInfo,
    SetLicense,
    SetDataSchema,
    SetTransform,
    AddPushSource,
    DisablePushSource,
    DisablePollingSource,
}

impl MetadataEventType {
    pub fn from_metadata_event(event: &odf::MetadataEvent) -> Self {
        match event {
            odf::MetadataEvent::AddData(_) => Self::AddData,
            odf::MetadataEvent::ExecuteTransform(_) => Self::ExecuteTransform,
            odf::MetadataEvent::Seed(_) => Self::Seed,
            odf::MetadataEvent::SetPollingSource(_) => Self::SetPollingSource,
            odf::MetadataEvent::SetVocab(_) => Self::SetVocab,
            odf::MetadataEvent::SetAttachments(_) => Self::SetAttachments,
            odf::MetadataEvent::SetInfo(_) => Self::SetInfo,
            odf::MetadataEvent::SetLicense(_) => Self::SetLicense,
            odf::MetadataEvent::SetDataSchema(_) => Self::SetDataSchema,
            odf::MetadataEvent::SetTransform(_) => Self::SetTransform,
            odf::MetadataEvent::AddPushSource(_) => Self::AddPushSource,
            odf::MetadataEvent::DisablePushSource(_) => Self::DisablePushSource,
            odf::MetadataEvent::DisablePollingSource(_) => Self::DisablePollingSource,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
