// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct LinkProtocolDesc {
    pub url: String,
}

#[derive(SimpleObject, Debug)]
pub struct CliProtocolDesc {
    pub pull_command: String,
    pub push_command: String,
}

#[derive(SimpleObject, Debug)]
pub struct RestProtocolDesc {
    pub tail_url: String,
    pub query_url: String,
    pub push_url: String,
}

#[derive(SimpleObject, Debug)]
pub struct FlightSqlDesc {
    pub url: String,
}

#[derive(SimpleObject, Debug)]
pub struct JdbcDesc {
    pub url: String,
}

#[derive(SimpleObject, Debug)]
pub struct PostgreSqlDesl {
    pub url: String,
}

#[derive(SimpleObject, Debug)]
pub struct KafkaProtocolDesc {
    pub url: String,
}

#[derive(SimpleObject, Debug)]
pub struct WebSocketProtocolDesc {
    pub url: String,
}

#[derive(SimpleObject, Debug)]
pub struct OdataProtocolDesc {
    pub service_url: String,
    pub collection_url: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
