// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The boundary a request came through. Each boundary registers it next to the
/// `Principal` it authenticates; background work registers none and states
/// its origin where it records one
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestOrigin {
    /// A request to the node's API
    Api(ApiKind),
    /// A local CLI command
    Cli,
    /// Work of the node itself
    Internal {
        /// Stable name of the producing component
        producer: String,
        /// Defined by the producer, such as the ID of the flow it serves
        detail: Option<String>,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The API a request came through
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKind {
    GraphQl,
    /// Any HTTP route other than GraphQL, the ODF protocol included
    Rest,
    FlightSql,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
