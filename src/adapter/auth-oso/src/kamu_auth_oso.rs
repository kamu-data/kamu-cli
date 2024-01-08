// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use oso::{Oso, OsoError, PolarClass};

use crate::dataset_resource::*;
use crate::user_actor::*;

///////////////////////////////////////////////////////////////////////////////

pub struct KamuAuthOso {
    pub oso: Arc<Oso>,
}

#[component(pub)]
impl KamuAuthOso {
    pub fn new() -> Self {
        let oso = match KamuAuthOso::load_oso() {
            Ok(oso) => oso,
            Err(e) => {
                panic!("Failed to initialize OSO: {e:?}");
            }
        };

        Self { oso: Arc::new(oso) }
    }

    fn load_oso() -> Result<Oso, OsoError> {
        let mut oso = Oso::new();

        oso.register_class(DatasetResource::get_polar_class())?;
        oso.register_class(UserActor::get_polar_class())?;

        oso.load_str(include_str!("schema.polar"))?;

        Ok(oso)
    }
}

///////////////////////////////////////////////////////////////////////////////
