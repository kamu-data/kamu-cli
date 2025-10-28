// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{NewOutboxMessage, OutboxMessage, OutboxMessageID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait OutboxMessageRepository: Send + Sync {
    async fn push_message(&self, message: NewOutboxMessage) -> Result<(), InternalError>;

    fn get_messages(
        &self,
        above_boundaries_by_producer: Vec<(String, OutboxMessageID)>,
        batch_size: usize,
    ) -> OutboxMessageStream<'_>;

    async fn get_latest_message_ids_by_producer(
        &self,
    ) -> Result<Vec<(String, OutboxMessageID)>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type OutboxMessageStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<OutboxMessage, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
