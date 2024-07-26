// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface};
use internal_error::InternalError;

use crate::{MessageRelevance, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn Outbox)]
pub struct DummyOutboxImpl {}

#[async_trait::async_trait]
impl Outbox for DummyOutboxImpl {
    async fn post_message_as_json(
        &self,
        _publisher_name: &str,
        _message_type: &str,
        _content_json: serde_json::Value,
        _relevance: MessageRelevance,
    ) -> Result<(), InternalError> {
        // We are happy
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
