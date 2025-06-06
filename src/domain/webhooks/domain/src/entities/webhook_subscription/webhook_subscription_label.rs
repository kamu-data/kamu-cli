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

// Note: empty label is fine
#[nutype(
    sanitize(trim),
    validate(len_char_max = 100,),
    derive(Debug, Display, AsRef, Clone, Eq, PartialEq, Serialize, Deserialize)
)]
pub struct WebhookSubscriptionLabel(String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
