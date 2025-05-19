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

    pub const DATASET_REF_UPDATED: &str = "DATASET.REF.UPDATED";

    pub fn test() -> WebhookEventType {
        WebhookEventType::try_new(Self::TEST).unwrap()
    }

    pub fn dataset_ref_updated() -> WebhookEventType {
        WebhookEventType::try_new(Self::DATASET_REF_UPDATED).unwrap()
    }

    pub fn all_non_test() -> Vec<WebhookEventType> {
        vec![Self::dataset_ref_updated()]
    }

    pub fn all_non_test_as_str() -> Vec<&'static str> {
        vec![Self::DATASET_REF_UPDATED]
    }

    pub fn is_valid_type(event_type: &WebhookEventType) -> bool {
        event_type.as_ref() == Self::TEST || event_type.as_ref() == Self::DATASET_REF_UPDATED
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
