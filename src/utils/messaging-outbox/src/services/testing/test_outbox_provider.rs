// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum OutboxProvider {
    #[default]
    Dummy,
    Mock(MockOutbox),
    Immediate {
        force_immediate: bool,
    },
    Dispatching,
}

impl OutboxProvider {
    pub fn embed_into_catalog(self, target_catalog_builder: &mut dill::CatalogBuilder) {
        match self {
            OutboxProvider::Dummy => {
                target_catalog_builder.add::<DummyOutboxImpl>();
            }
            OutboxProvider::Mock(mock_outbox) => {
                target_catalog_builder
                    .add_value(mock_outbox)
                    .bind::<dyn Outbox, MockOutbox>();
            }
            OutboxProvider::Immediate { force_immediate } => {
                target_catalog_builder
                    .add_builder(OutboxImmediateImpl::builder(if force_immediate {
                        ConsumerFilter::AllConsumers
                    } else {
                        ConsumerFilter::ImmediateConsumers
                    }))
                    .bind::<dyn crate::Outbox, OutboxImmediateImpl>();
            }
            OutboxProvider::Dispatching => {
                // The most complete setup, but it still needs bindings for repositories.
                // Not adding those here to avoid cyclic build dependencies.
                target_catalog_builder
                    .add_builder(OutboxImmediateImpl::builder(
                        ConsumerFilter::ImmediateConsumers,
                    ))
                    .add::<OutboxTransactionalImpl>()
                    .add::<OutboxDispatchingImpl>()
                    .bind::<dyn crate::Outbox, OutboxDispatchingImpl>()
                    .add::<OutboxAgentImpl>()
                    .add_value(OutboxConfig::default())
                    .add::<OutboxAgentMetrics>();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
