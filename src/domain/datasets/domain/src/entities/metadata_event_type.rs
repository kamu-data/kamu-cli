// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf::metadata::MetadataEventTypeFlags;
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

    pub fn multiple_from_metadata_event_flags(
        mut flags: odf::metadata::MetadataEventTypeFlags,
    ) -> Vec<Self> {
        let mut event_types = Vec::new();

        if flags.contains(MetadataEventTypeFlags::ADD_DATA) {
            event_types.push(Self::AddData);
            flags -= MetadataEventTypeFlags::ADD_DATA;
        }

        if flags.contains(MetadataEventTypeFlags::EXECUTE_TRANSFORM) {
            event_types.push(Self::ExecuteTransform);
            flags -= MetadataEventTypeFlags::EXECUTE_TRANSFORM;
        }

        if flags.contains(MetadataEventTypeFlags::SEED) {
            event_types.push(Self::Seed);
            flags -= MetadataEventTypeFlags::SEED;
        }

        if flags.contains(MetadataEventTypeFlags::SET_POLLING_SOURCE) {
            event_types.push(Self::SetPollingSource);
            flags -= MetadataEventTypeFlags::SET_POLLING_SOURCE;
        }

        if flags.contains(MetadataEventTypeFlags::SET_VOCAB) {
            event_types.push(Self::SetVocab);
            flags -= MetadataEventTypeFlags::SET_VOCAB;
        }

        if flags.contains(MetadataEventTypeFlags::SET_ATTACHMENTS) {
            event_types.push(Self::SetAttachments);
            flags -= MetadataEventTypeFlags::SET_ATTACHMENTS;
        }

        if flags.contains(MetadataEventTypeFlags::SET_INFO) {
            event_types.push(Self::SetInfo);
            flags -= MetadataEventTypeFlags::SET_INFO;
        }

        if flags.contains(MetadataEventTypeFlags::SET_LICENSE) {
            event_types.push(Self::SetLicense);
            flags -= MetadataEventTypeFlags::SET_LICENSE;
        }

        if flags.contains(MetadataEventTypeFlags::SET_DATA_SCHEMA) {
            event_types.push(Self::SetDataSchema);
            flags -= MetadataEventTypeFlags::SET_DATA_SCHEMA;
        }

        if flags.contains(MetadataEventTypeFlags::SET_TRANSFORM) {
            event_types.push(Self::SetTransform);
            flags -= MetadataEventTypeFlags::SET_TRANSFORM;
        }

        if flags.contains(MetadataEventTypeFlags::ADD_PUSH_SOURCE) {
            event_types.push(Self::AddPushSource);
            flags -= MetadataEventTypeFlags::ADD_PUSH_SOURCE;
        }

        if flags.contains(MetadataEventTypeFlags::DISABLE_PUSH_SOURCE) {
            event_types.push(Self::DisablePushSource);
            flags -= MetadataEventTypeFlags::DISABLE_PUSH_SOURCE;
        }

        if flags.contains(MetadataEventTypeFlags::DISABLE_POLLING_SOURCE) {
            event_types.push(Self::DisablePollingSource);
            flags -= MetadataEventTypeFlags::DISABLE_POLLING_SOURCE;
        }

        assert!(flags.is_empty(), "Unrecognized flags: {flags:?}");

        event_types
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
