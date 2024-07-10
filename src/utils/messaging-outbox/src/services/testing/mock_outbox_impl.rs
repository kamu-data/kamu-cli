// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::Outbox;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub Outbox {}

    #[async_trait::async_trait]
    impl Outbox for Outbox {
        async fn post_message_as_json(
            &self,
            publisher_name: &str,
            message_type: &str,
            content_json: serde_json::Value,
        ) -> Result<(), InternalError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
