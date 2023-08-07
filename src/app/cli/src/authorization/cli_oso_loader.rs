// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use oso::{Oso, OsoError, PolarClass};

use crate::{DatasetResource, UserActor};

///////////////////////////////////////////////////////////////////////////////

pub fn cli_oso() -> Result<Oso, OsoError> {
    let mut oso = Oso::new();

    oso.register_class(DatasetResource::get_polar_class())?;
    oso.register_class(UserActor::get_polar_class())?;

    if let Err(e) = oso.load_str(r#"
        actor UserActor {}

        resource DatasetResource {
            permissions = ["read", "write"];
        }
        
        has_permission(actor: UserActor, "read", dataset: DatasetResource) if
            dataset.allows_public_read or actor.name in dataset.authorized_readers;
        
        has_permission(actor: UserActor, "write", dataset: DatasetResource) if
            dataset.created_by == actor.name or actor.name in dataset.authorized_editors;
        
        allow(actor: UserActor, action: String, dataset: DatasetResource) if
            has_permission(actor, action, dataset);    
    "#) {
            println!("{}", e);
            return Err(e);
        }

    Ok(oso)
}
    

///////////////////////////////////////////////////////////////////////////////
