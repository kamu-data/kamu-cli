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

/// Link to a dataset page in the web UI
#[derive(SimpleObject, Debug)]
pub struct LinkProtocolDesc {
    /// URL of the dataset page
    pub url: String,
}

/// Kamu CLI commands to sync a dataset with this node
#[derive(SimpleObject, Debug)]
pub struct CliProtocolDesc {
    /// Command that pulls the dataset from this node
    pub pull_command: String,
    /// Command that pushes a local dataset to this node
    pub push_command: String,
}

/// REST API endpoints for a dataset
#[derive(SimpleObject, Debug)]
pub struct RestProtocolDesc {
    /// Example URL fetching the latest records of the dataset
    pub tail_url: String,
    /// Example URL of the SQL query endpoint
    pub query_url: String,
    /// URL of the endpoint that ingests data into the dataset
    pub push_url: String,
}

/// Arrow Flight SQL endpoint
#[derive(SimpleObject, Debug)]
pub struct FlightSqlDesc {
    /// Arrow Flight SQL server URL
    pub url: String,
}

/// JDBC connection details
#[derive(SimpleObject, Debug)]
pub struct JdbcDesc {
    /// JDBC connection URL for the Arrow Flight SQL driver
    pub url: String,
}

/// PostgreSQL protocol endpoint
#[derive(SimpleObject, Debug)]
pub struct PostgreSqlDesl {
    /// PostgreSQL connection URL
    pub url: String,
}

/// Kafka protocol endpoint
#[derive(SimpleObject, Debug)]
pub struct KafkaProtocolDesc {
    /// Kafka connection URL
    pub url: String,
}

/// WebSocket endpoint
#[derive(SimpleObject, Debug)]
pub struct WebSocketProtocolDesc {
    /// WebSocket connection URL
    pub url: String,
}

/// `OData` API endpoints for a dataset
#[derive(SimpleObject, Debug)]
pub struct OdataProtocolDesc {
    /// URL of the `OData` service of the dataset owner
    pub service_url: String,
    /// URL of the `OData` collection of the dataset
    pub collection_url: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
