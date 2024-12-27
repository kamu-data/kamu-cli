// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use bitflags::bitflags;

use crate::MetadataEvent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct MetadataEventTypeFlags: u32 {
        const ADD_DATA = 1 << 0;
        const EXECUTE_TRANSFORM = 1 << 1;
        const SEED = 1 << 2;
        const SET_POLLING_SOURCE = 1 << 3;
        const SET_TRANSFORM = 1 << 4;
        const SET_VOCAB = 1 << 5;
        const SET_ATTACHMENTS = 1 << 6;
        const SET_INFO = 1 << 7;
        const SET_LICENSE = 1 << 8;
        const SET_DATA_SCHEMA = 1 << 9;
        const ADD_PUSH_SOURCE = 1 << 10;
        const DISABLE_PUSH_SOURCE = 1 << 11;
        const DISABLE_POLLING_SOURCE = 1 << 12;
    }
}

impl From<&MetadataEvent> for MetadataEventTypeFlags {
    fn from(v: &MetadataEvent) -> Self {
        match v {
            MetadataEvent::AddData(_) => Self::ADD_DATA,
            MetadataEvent::ExecuteTransform(_) => Self::EXECUTE_TRANSFORM,
            MetadataEvent::Seed(_) => Self::SEED,
            MetadataEvent::SetPollingSource(_) => Self::SET_POLLING_SOURCE,
            MetadataEvent::SetTransform(_) => Self::SET_TRANSFORM,
            MetadataEvent::SetVocab(_) => Self::SET_VOCAB,
            MetadataEvent::SetAttachments(_) => Self::SET_ATTACHMENTS,
            MetadataEvent::SetInfo(_) => Self::SET_INFO,
            MetadataEvent::SetLicense(_) => Self::SET_LICENSE,
            MetadataEvent::SetDataSchema(_) => Self::SET_DATA_SCHEMA,
            MetadataEvent::AddPushSource(_) => Self::ADD_PUSH_SOURCE,
            MetadataEvent::DisablePushSource(_) => Self::DISABLE_PUSH_SOURCE,
            MetadataEvent::DisablePollingSource(_) => Self::DISABLE_POLLING_SOURCE,
        }
    }
}
