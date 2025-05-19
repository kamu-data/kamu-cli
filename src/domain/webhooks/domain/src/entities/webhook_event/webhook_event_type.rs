// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use nutype::nutype;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype(
    sanitize(trim, uppercase),
    validate(not_empty),
    derive(
        Debug,
        Display,
        AsRef,
        Clone,
        Ord,
        PartialOrd,
        Eq,
        PartialEq,
        Serialize,
        Deserialize
    )
)]
pub struct WebhookEventType(String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
