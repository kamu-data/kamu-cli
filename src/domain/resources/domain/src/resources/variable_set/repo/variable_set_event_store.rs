// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use event_sourcing::EventStore;
use internal_error::InternalError;

use crate::{ResourceName, VariableSetID, VariableSetState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait VariableSetEventStore: EventStore<VariableSetState> {
    async fn new_variable_set_id(&self) -> Result<VariableSetID, InternalError>;

    async fn get_variable_set_id_by_name(
        &self,
        name: &ResourceName,
    ) -> Result<Option<VariableSetID>, InternalError>;

    fn list_variable_sets(&self, pagination: PaginationOpts) -> VariableSetIDStream<'_>;

    async fn get_count_variable_sets(&self) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetIDStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<VariableSetID, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
