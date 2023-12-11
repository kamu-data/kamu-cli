// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::EventStore;
use kamu_core::DatasetIDStream;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationEventStore: EventStore<FlowConfigurationState> {
    /// Returns all unique values of dataset IDs associated with update configs
    // TODO: re-consider performance impact
    fn list_all_dataset_ids<'a>(&'a self) -> DatasetIDStream<'a>;
}

/////////////////////////////////////////////////////////////////////////////////////////
