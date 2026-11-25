// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::Mutex;

use kamu_resources::ResourceNameSuffixGenerator;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Yields the given suffixes in order, and panics when asked for more
#[derive(Default)]
pub struct ScriptedNameSuffixGenerator {
    suffixes: Mutex<VecDeque<&'static str>>,
}

impl ScriptedNameSuffixGenerator {
    pub fn new(suffixes: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            suffixes: Mutex::new(suffixes.into_iter().collect()),
        }
    }

    pub fn assert_all_used(&self) {
        let unused = self.suffixes.lock().unwrap();
        assert!(unused.is_empty(), "unused name suffixes: {unused:?}");
    }
}

impl ResourceNameSuffixGenerator for ScriptedNameSuffixGenerator {
    fn generate_suffix(&self) -> String {
        self.suffixes
            .lock()
            .unwrap()
            .pop_front()
            .expect("more name suffixes requested than scripted")
            .to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
