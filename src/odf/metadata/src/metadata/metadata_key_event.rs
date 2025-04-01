// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::MetadataEvent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extension trait for `MetadataEvent` to classify events.
pub trait MetadataEventExt {
    /// Returns `true` if the event is a key event, otherwise `false`.
    fn is_key_event(&self) -> bool;
}

impl MetadataEventExt for MetadataEvent {
    fn is_key_event(&self) -> bool {
        !matches!(
            self,
            MetadataEvent::AddData(_) | MetadataEvent::ExecuteTransform(_)
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
