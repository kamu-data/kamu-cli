// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use dill::{Singleton, component, interface, scope};
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowActivationLinkRepository {
    links: Arc<Mutex<BTreeMap<(FlowID, FlowID), FlowActivationLink>>>,
}

#[component(pub)]
#[interface(dyn FlowActivationLinkRepository)]
#[scope(Singleton)]
impl InMemoryFlowActivationLinkRepository {
    pub fn new() -> Self {
        Self {
            links: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowActivationLinkRepository for InMemoryFlowActivationLinkRepository {
    async fn save_link(&self, link: &FlowActivationLink) -> Result<(), InternalError> {
        let mut links = self.links.lock().unwrap();
        links
            .entry((link.upstream_flow_id, link.downstream_flow_id))
            .or_insert_with(|| link.clone());
        Ok(())
    }

    async fn get_downstream_links(
        &self,
        upstream_flow_ids: &[FlowID],
    ) -> Result<Vec<FlowActivationLink>, InternalError> {
        let links = self.links.lock().unwrap();

        let mut result: Vec<_> = links
            .values()
            .filter(|link| upstream_flow_ids.contains(&link.upstream_flow_id))
            .cloned()
            .collect();
        result.sort_by_key(|link| {
            (
                link.upstream_flow_id,
                link.activated_at,
                link.downstream_flow_id,
            )
        });

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
