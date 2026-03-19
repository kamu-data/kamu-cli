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

use crate::{ResourceName, SecretSetID, SecretSetState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SecretSetEventStore: EventStore<SecretSetState> {
    async fn new_secret_set_id(&self) -> Result<SecretSetID, InternalError>;

    async fn get_secret_set_id_by_name(
        &self,
        name: &ResourceName,
    ) -> Result<Option<SecretSetID>, InternalError>;

    fn list_secret_sets(&self, pagination: PaginationOpts) -> SecretSetIDStream<'_>;

    async fn get_count_secret_sets(&self) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SecretSetIDStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<SecretSetID, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
