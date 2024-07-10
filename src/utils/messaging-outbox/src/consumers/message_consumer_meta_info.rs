// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MessageConsumerMetaInfoRecord {
    pub producer_name: &'static str,
    pub message_type: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MessageConsumerMetaInfo {
    pub consumer_name: &'static str,
    pub records: Vec<MessageConsumerMetaInfoRecord>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MessageConsumerMetaInfoRow {
    pub producer_name: &'static str,
    pub consumer_name: &'static str,
    pub message_type: &'static str,
}

impl std::fmt::Display for MessageConsumerMetaInfoRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} --{}--> {}",
            self.producer_name, self.message_type, self.consumer_name
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
