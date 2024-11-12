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

// Version of outbox message which will be bumped only if the structure contains
// breaking changes in the message structure
pub const OUTBOX_MESSAGE_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
pub trait Message: Clone + Serialize + for<'a> Deserialize<'a> + Send + Sync {
    fn version(&self) -> u32 {
        OUTBOX_MESSAGE_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
