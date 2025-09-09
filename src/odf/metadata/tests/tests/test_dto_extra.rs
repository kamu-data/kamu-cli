// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_metadata_event_type_flags_consts() {
    use opendatafabric_metadata::MetadataEventTypeFlags;

    assert_eq!(0, MetadataEventTypeFlags::DATA_BLOCK.bits());
    assert_eq!(0, MetadataEventTypeFlags::KEY_BLOCK.bits());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
