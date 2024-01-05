// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::DatasetFlowType")]
pub enum DatasetFlowType {
    Ingest,
    ExecuteQuery,
    Compaction,
}

/////////////////////////////////////////////////////////////////////////////////////////
