// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use chrono::{DateTime, Utc};

/////////////////////////////////////////////////////////////////////////////////////////

/// Abstracts the system time source
pub trait SystemTimeSource: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub struct SystemTimeSourceDefault;

impl SystemTimeSource for SystemTimeSourceDefault {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemTimeSourceMock {
    t: Mutex<DateTime<Utc>>,
}

impl SystemTimeSourceMock {
    pub fn new(t: DateTime<Utc>) -> Self {
        Self { t: Mutex::new(t) }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        *self.t.lock().unwrap() = t;
    }
}

impl SystemTimeSource for SystemTimeSourceMock {
    fn now(&self) -> DateTime<Utc> {
        (*self.t.lock().unwrap()).clone()
    }
}
