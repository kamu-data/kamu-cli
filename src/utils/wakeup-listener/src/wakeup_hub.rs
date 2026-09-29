// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::WakeupListenerMetrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A per-process source of change signals for a storage backend, shared by
/// all listeners through [`HubWakeupListener`](crate::HubWakeupListener)
/// handles. See `docs/internal/wakeup-listeners.md`.
pub trait WakeupHub: Send + Sync + 'static {
    /// What a listener watches, e.g. a Postgres channel name
    type Channel: Copy + Eq + Hash + Debug + Send + Sync + 'static;

    /// Registers a slot, signaled once the channel is watched and on every
    /// change afterwards. Must be called within a Tokio runtime.
    fn subscribe(&self, channel: Self::Channel) -> Arc<Notify>;

    /// Where listeners report their waits
    fn metrics(&self) -> &WakeupListenerMetrics;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
