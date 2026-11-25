// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceNameSuffixGenerator;
use random_strings::get_random_name;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const GENERATED_NAME_SUFFIX_LENGTH: usize = 6;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceNameSuffixGenerator)]
pub struct ResourceNameSuffixGeneratorImpl;

impl ResourceNameSuffixGenerator for ResourceNameSuffixGeneratorImpl {
    fn generate_suffix(&self) -> String {
        get_random_name(None, GENERATED_NAME_SUFFIX_LENGTH).to_ascii_lowercase()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
