// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Singleton, component, interface, scope};
use tokio::sync::Notify;
use wakeup_listener::{
    HubWakeupListener,
    WakeupHub,
    WakeupListener,
    WakeupListenerMetrics,
    WakeupSubscribers,
};

use crate::domain::{ResourceControllerHints, ResourceControllerName, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The in-process hub behind `ResourceControllerHints`, with one channel per
/// schema. Registered on every storage backend: unlike the hubs of the
/// storage crates, it is fed by code running in the same process, not by the
/// database.
pub struct ResourceControllerHintHub {
    // Shared with every listener handle, which holds the hub by `Arc`
    channels: Arc<ResourceControllerHintChannels>,
}

#[component(pub)]
#[interface(dyn ResourceControllerHints)]
#[scope(Singleton)]
impl ResourceControllerHintHub {
    pub fn new(metrics: Arc<WakeupListenerMetrics>) -> Self {
        Self {
            channels: Arc::new(ResourceControllerHintChannels {
                subscribers: WakeupSubscribers::new(),
                metrics,
            }),
        }
    }
}

impl ResourceControllerHints for ResourceControllerHintHub {
    fn hint(&self, schema: &TypeUri) {
        self.channels.subscribers.signal(schema.as_str());
    }

    fn new_wakeup_listener(
        &self,
        schema: &'static str,
        controller_name: &ResourceControllerName,
    ) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(
            self.channels.clone(),
            schema,
            controller_name.as_str(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceControllerHintChannels {
    subscribers: WakeupSubscribers<&'static str>,
    metrics: Arc<WakeupListenerMetrics>,
}

impl WakeupHub for ResourceControllerHintChannels {
    type Channel = &'static str;

    fn subscribe(&self, schema: &'static str) -> Arc<Notify> {
        let slot = self.subscribers.add(schema);

        // Subscription is lazy, and a hint to a schema nobody listens to is
        // not kept, so a change made before this call would otherwise be missed
        slot.notify_one();

        slot
    }

    fn metrics(&self) -> &WakeupListenerMetrics {
        &self.metrics
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
