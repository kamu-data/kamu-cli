// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, Weak};

use futures::FutureExt as _;
use tokio::sync::Notify;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Subscriber registry of a [`WakeupHub`](crate::WakeupHub): routes signals
/// to the slots of listener handles by channel.
pub struct WakeupSubscribers<C> {
    // Weak, so a dropped handle simply stops receiving signals and is pruned on
    // the next signal
    slots: Mutex<HashMap<C, Vec<Weak<Notify>>>>,
    // Tells the hub's task that the channel set changed. `Notify` stores a
    // permit, so a subscription made while the task is busy is not lost.
    changed: Notify,
}

impl<C: Copy + Eq + Hash> WakeupSubscribers<C> {
    pub fn new() -> Self {
        Self {
            slots: Mutex::default(),
            changed: Notify::new(),
        }
    }

    pub fn add(&self, channel: C) -> Arc<Notify> {
        let slot = Arc::new(Notify::new());
        self.slots
            .lock()
            .unwrap()
            .entry(channel)
            .or_default()
            .push(Arc::downgrade(&slot));

        self.changed.notify_one();
        slot
    }

    /// Resolves when channels were added since the previous call
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    /// Consumes a pending change permit without waiting
    pub fn take_changed(&self) {
        let _ = self.changed.notified().now_or_never();
    }

    pub fn channels(&self) -> Vec<C> {
        self.slots.lock().unwrap().keys().copied().collect()
    }

    pub fn signal<Q>(&self, channel: &Q)
    where
        C: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut slots = self.slots.lock().unwrap();
        if let Some(channel_slots) = slots.get_mut(channel) {
            Self::signal_slots(channel_slots);
        }
    }

    pub fn signal_all(&self) {
        let mut slots = self.slots.lock().unwrap();
        for channel_slots in slots.values_mut() {
            Self::signal_slots(channel_slots);
        }
    }

    // `notify_one` stores a permit if the handle isn't waiting right now, so the
    // signal is picked up by its next `wait_wake`
    fn signal_slots(slots: &mut Vec<Weak<Notify>>) {
        slots.retain(|slot| match slot.upgrade() {
            Some(slot) => {
                slot.notify_one();
                true
            }
            None => false,
        });
    }
}

impl<C: Copy + Eq + Hash> Default for WakeupSubscribers<C> {
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
