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
                panic!("Failed to initialize OSO: {:?}", e);
            }
        };

        Self { oso: Arc::new(oso) }
    }

    fn load_oso() -> Result<Oso, OsoError> {
        let mut oso = Oso::new();

        oso.register_class(DatasetResource::get_polar_class())?;
        oso.register_class(UserActor::get_polar_class())?;
    
        oso.load_str(
            r#"
            actor UserActor {}
    
            resource DatasetResource {
                permissions = ["read", "write"];
            }
    
            has_permission(actor: UserActor, "read", dataset: DatasetResource) if
                dataset.allows_public_read or 
                dataset.created_by == actor.name or (
                    actor_name = actor.name and
                    dataset.authorized_users.(actor_name) in ["Reader", "Editor"]
                );
    
            has_permission(actor: UserActor, "write", dataset: DatasetResource) if
                dataset.created_by == actor.name or (
                    actor_name = actor.name and
                    dataset.authorized_users.(actor_name) == "Editor"
                );
    
            allow(actor: UserActor, action: String, dataset: DatasetResource) if
                has_permission(actor, action, dataset);
        "#,
        )?;
    
        Ok(oso)
    }
}

///////////////////////////////////////////////////////////////////////////////
