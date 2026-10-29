// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Server side of the smart transfer protocol
#[derive(Debug, Clone)]
pub struct SmartTransferProtocolServerConfig {
    /// How often a push asks the client for upload progress while objects
    /// are uploaded; never zero
    pub upload_progress_interval: Duration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
