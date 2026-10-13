// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::MetadataEvent;

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
    pub fn from_metadata_event(event: &MetadataEvent) -> Self {
        match event {
            MetadataEvent::AddData(_) => Self::AddData,
            MetadataEvent::ExecuteTransform(_) => Self::ExecuteTransform,
            MetadataEvent::Seed(_) => Self::Seed,
            MetadataEvent::SetPollingSource(_) => Self::SetPollingSource,
            MetadataEvent::SetVocab(_) => Self::SetVocab,
            MetadataEvent::SetAttachments(_) => Self::SetAttachments,
            MetadataEvent::SetInfo(_) => Self::SetInfo,
            MetadataEvent::SetLicense(_) => Self::SetLicense,
            MetadataEvent::SetDataSchema(_) => Self::SetDataSchema,
            MetadataEvent::SetTransform(_) => Self::SetTransform,
            MetadataEvent::AddPushSource(_) => Self::AddPushSource,
            MetadataEvent::DisablePushSource(_) => Self::DisablePushSource,
            MetadataEvent::DisablePollingSource(_) => Self::DisablePollingSource,
        }
    }
}

impl From<crate::serde::flatbuffers::proxies_generated::MetadataEvent> for MetadataEventType {
    fn from(value: crate::serde::flatbuffers::proxies_generated::MetadataEvent) -> Self {
        use crate::serde::flatbuffers::proxies_generated::MetadataEvent as Fb;
        debug_assert_eq!(Fb::ENUM_MAX, Fb::DisablePollingSource.0);
        match value {
            Fb::AddData => Self::AddData,
            Fb::ExecuteTransform => Self::ExecuteTransform,
            Fb::Seed => Self::Seed,
            Fb::SetPollingSource => Self::SetPollingSource,
            Fb::SetTransform => Self::SetTransform,
            Fb::SetVocab => Self::SetVocab,
            Fb::SetAttachments => Self::SetAttachments,
            Fb::SetInfo => Self::SetInfo,
            Fb::SetLicense => Self::SetLicense,
            Fb::SetDataSchema => Self::SetDataSchema,
            Fb::AddPushSource => Self::AddPushSource,
            Fb::DisablePushSource => Self::DisablePushSource,
            Fb::DisablePollingSource => Self::DisablePollingSource,
            _ => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
