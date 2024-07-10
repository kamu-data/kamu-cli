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
        publisher_name: &str,
        message_type: &str,
        content_json: serde_json::Value,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub async fn post_outbox_message<M: Message>(
    outbox: &dyn Outbox,
    publisher_name: &str,
    message: M,
) -> Result<(), InternalError> {
    let message_as_json = serde_json::to_value(&message).unwrap();
    outbox
        .post_message_as_json(publisher_name, message.type_name(), message_as_json)
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
