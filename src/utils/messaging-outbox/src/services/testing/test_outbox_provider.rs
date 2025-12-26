// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Component;

use crate::{DummyOutboxImpl, MockOutbox, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum OutboxProvider {
    #[default]
    Dummy,
    Mock(MockOutbox),
    Immediate,
    PauseableImmediate,
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
            OutboxProvider::Immediate => {
                use crate::{ConsumerFilter, OutboxImmediateImpl};
                target_catalog_builder
                    .add_builder(
                        OutboxImmediateImpl::builder()
                            .with_consumer_filter(ConsumerFilter::AllConsumers),
                    )
                    .bind::<dyn crate::Outbox, OutboxImmediateImpl>();
            }
            OutboxProvider::PauseableImmediate => {
                use crate::{ConsumerFilter, OutboxImmediateImpl, PausableImmediateOutboxImpl};
                target_catalog_builder
                    .add_builder(
                        OutboxImmediateImpl::builder()
                            .with_consumer_filter(ConsumerFilter::AllConsumers),
                    )
                    .add::<PausableImmediateOutboxImpl>()
                    .bind::<dyn Outbox, PausableImmediateOutboxImpl>();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
