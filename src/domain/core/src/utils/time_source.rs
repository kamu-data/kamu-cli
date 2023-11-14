// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

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

#[derive(Debug, Clone)]
pub struct SystemTimeSourceStub {
    t: Arc<Mutex<Option<DateTime<Utc>>>>,
}

impl SystemTimeSourceStub {
    pub fn new() -> Self {
        Self {
            t: Arc::new(Mutex::new(None)),
        }
    }

    pub fn new_set(t: DateTime<Utc>) -> Self {
        Self {
            t: Arc::new(Mutex::new(Some(t))),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        *self.t.lock().unwrap() = Some(t);
    }

    pub fn unset(&self) {
        *self.t.lock().unwrap() = None;
    }
}

impl SystemTimeSource for SystemTimeSourceStub {
    fn now(&self) -> DateTime<Utc> {
        match *self.t.lock().unwrap() {
            None => Utc::now(),
            Some(ref t) => t.clone(),
        }
    }
}
