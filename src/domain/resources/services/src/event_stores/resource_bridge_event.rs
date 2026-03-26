// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::domain::{ReconcilableResourceEvent, ResourceID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceBridgeEvent {
    fn resource_id(&self) -> &ResourceID;
    fn event_time(&self) -> DateTime<Utc>;
    fn typename(&self) -> &'static str;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec, TSuccess, TFailureDetails> ResourceBridgeEvent
    for ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>
{
    fn resource_id(&self) -> &ResourceID {
        Self::resource_id(self)
    }

    fn event_time(&self) -> DateTime<Utc> {
        Self::event_time(self)
    }

    fn typename(&self) -> &'static str {
        Self::typename(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
