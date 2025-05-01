// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::WebhookEventType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookEventTypeCatalog;

impl WebhookEventTypeCatalog {
    pub const TEST: &str = "TEST";

    pub const DATASET_HEAD_UPDATED: &str = "DATASET.HEAD.UPDATED";

    pub fn test() -> WebhookEventType {
        WebhookEventType::try_new(Self::TEST).unwrap()
    }

    pub fn dataset_head_updated() -> WebhookEventType {
        WebhookEventType::try_new(Self::DATASET_HEAD_UPDATED).unwrap()
    }

    pub fn all_non_test() -> Vec<WebhookEventType> {
        vec![Self::dataset_head_updated()]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
