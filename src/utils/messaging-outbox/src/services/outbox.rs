// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::Message;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait Outbox: Send + Sync {
    async fn post_message_as_json(
        &self,
        producer_name: &str,
        content_json: &serde_json::Value,
        version: u32,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait OutboxExt {
    async fn post_message<M: Message>(
        &self,
        producer_name: &str,
        message: M,
    ) -> Result<(), InternalError>;
}

#[async_trait::async_trait]
impl<T: Outbox + ?Sized> OutboxExt for T {
    #[inline]
    async fn post_message<M: Message>(
        &self,
        producer_name: &str,
        message: M,
    ) -> Result<(), InternalError> {
        let message_as_json = serde_json::to_value(&message).unwrap();
        self.post_message_as_json(producer_name, &message_as_json, M::version())
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
