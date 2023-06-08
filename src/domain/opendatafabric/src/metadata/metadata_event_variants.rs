// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enum_variants::*;

use crate::dtos::*;

impl_enum_with_variants!(MetadataEvent);

impl_enum_variant!(MetadataEvent::AddData(AddData));
impl_enum_variant!(MetadataEvent::ExecuteQuery(ExecuteQuery));
impl_enum_variant!(MetadataEvent::Seed(Seed));
impl_enum_variant!(MetadataEvent::SetAttachments(SetAttachments));
impl_enum_variant!(MetadataEvent::SetInfo(SetInfo));
impl_enum_variant!(MetadataEvent::SetLicense(SetLicense));
impl_enum_variant!(MetadataEvent::SetPollingSource(SetPollingSource));
impl_enum_variant!(MetadataEvent::SetTransform(SetTransform));
impl_enum_variant!(MetadataEvent::SetVocab(SetVocab));
impl_enum_variant!(MetadataEvent::SetWatermark(SetWatermark));
