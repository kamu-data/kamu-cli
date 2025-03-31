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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
