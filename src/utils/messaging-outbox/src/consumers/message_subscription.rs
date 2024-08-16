// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct MessageSubscription {
    pub producer_name: &'static str,
    pub consumer_name: &'static str,
}

impl MessageSubscription {
    pub(crate) fn new(producer_name: &'static str, consumer_name: &'static str) -> Self {
        Self {
            producer_name,
            consumer_name,
        }
    }
}

impl std::fmt::Display for MessageSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ----> {}", self.producer_name, self.consumer_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
