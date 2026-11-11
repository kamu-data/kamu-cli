// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use wakeup_listener::WakeupListener;

use crate::{ResourceControllerName, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wakes the controller runtime when resources of a schema may need
/// attention. A hint carries no data: the runtime re-reads storage, so a
/// missed or spurious hint costs only latency or a needless pass.
///
/// Hints stay inside the process. They come from outbox consumers, which run
/// in the same process as the runtime on every storage backend.
pub trait ResourceControllerHints: Send + Sync {
    /// Signals that a resource of this schema changed
    fn hint(&self, schema: &TypeUri);

    /// A listener for the hints of one schema, for one controller loop. Its
    /// heartbeat is labelled with the controller's name.
    fn new_wakeup_listener(
        &self,
        schema: &'static str,
        controller_name: &ResourceControllerName,
    ) -> Box<dyn WakeupListener>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
