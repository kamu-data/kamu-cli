// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::enum_variants::*;
use crate::dtos::*;

impl_enum_with_variants!(MetadataEvent);

impl_enum_variant!(MetadataEvent, AddData);
impl_enum_variant!(MetadataEvent, ExecuteQuery);
impl_enum_variant!(MetadataEvent, Seed);
impl_enum_variant!(MetadataEvent, SetAttachments);
impl_enum_variant!(MetadataEvent, SetInfo);
impl_enum_variant!(MetadataEvent, SetLicense);
impl_enum_variant!(MetadataEvent, SetPollingSource);
impl_enum_variant!(MetadataEvent, SetTransform);
impl_enum_variant!(MetadataEvent, SetVocab);
impl_enum_variant!(MetadataEvent, SetWatermark);
