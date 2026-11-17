// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use crate::auth::ResourceOwnerRef;
use crate::formats::Did;
use crate::resources::{IntoResourceRef, ResourceID, ResourceName, ResourceRef, ResourceType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reference to a resource of type `Res`.
///
/// The typed counterpart of [`ResourceRef`]: the target type is fixed by `Res`
/// instead of being carried as a value, so it cannot name any other type.
pub struct ResourceRefT<Res: ResourceType> {
    /// Reference to an account that owns the target resource.
    pub account: Option<ResourceOwnerRef>,
    /// Type of the target resource, which exists only at compile time.
    pub r#type: PhantomData<fn() -> Res>,
    /// ID of the resource within a node.
    pub id: Option<ResourceID>,
    /// DID of the resource.
    pub did: Option<Did>,
    /// Name of a resource.
    pub name: Option<ResourceName>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<Res: ResourceType> ResourceRefT<Res> {
    /// Contructs typed reference without ensuring that the types are the same
    pub fn new_from_ref_unchecked(r: ResourceRef) -> Self {
        Self {
            account: r.account,
            r#type: PhantomData,
            id: r.id,
            did: r.did,
            name: r.name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Implemented by hand: derives would require `Res` itself to implement each
// trait, though no value of `Res` is stored.

impl<Res: ResourceType> Clone for ResourceRefT<Res> {
    fn clone(&self) -> Self {
        Self {
            account: self.account.clone(),
            id: self.id,
            did: self.did.clone(),
            r#type: PhantomData,
            name: self.name.clone(),
        }
    }
}

impl<Res: ResourceType> PartialEq for ResourceRefT<Res> {
    fn eq(&self, other: &Self) -> bool {
        self.account == other.account
            && self.id == other.id
            && self.did == other.did
            && self.name == other.name
    }
}

impl<Res: ResourceType> Eq for ResourceRefT<Res> {}

impl<Res: ResourceType> Default for ResourceRefT<Res> {
    fn default() -> Self {
        Self {
            account: None,
            id: None,
            did: None,
            r#type: PhantomData,
            name: None,
        }
    }
}

impl<Res: ResourceType> std::fmt::Debug for ResourceRefT<Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceRefT")
            .field("account", &self.account)
            .field("type", &Res::type_uri_str())
            .field("id", &self.id)
            .field("did", &self.did)
            .field("name", &self.name)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<Res: ResourceType> From<ResourceRefT<Res>> for ResourceRef {
    fn from(value: ResourceRefT<Res>) -> Self {
        let ResourceRefT {
            account,
            id,
            did,
            r#type: _,
            name,
        } = value;
        Self {
            account,
            id,
            did,
            r#type: Some(Res::type_uri().cheap_clone().into()),
            name,
        }
    }
}

impl<Res: ResourceType> IntoResourceRef for ResourceRefT<Res> {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
