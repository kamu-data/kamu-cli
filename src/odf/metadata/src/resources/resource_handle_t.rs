// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use crate::auth::ResourceOwnerHandle;
use crate::formats::Did;
use crate::resources::{
    IntoResourceRef,
    ResourceHandle,
    ResourceID,
    ResourceName,
    ResourceRef,
    ResourceRefT,
    ResourceType,
    TypeMismatchError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Link to a resolved resource of type `Res`.
///
/// The typed counterpart of [`ResourceHandle`]: the target type is fixed by
/// `Res` instead of being carried as a value, so it cannot name any other
/// type.
pub struct ResourceHandleT<Res: ResourceType> {
    /// Account that owns the target resource.
    pub account: ResourceOwnerHandle,
    /// Type of the target resource, which exists only at compile time.
    pub r#type: PhantomData<fn() -> Res>,
    /// ID of the resource within a node.
    pub id: ResourceID,
    /// DID of the resource, if applicable.
    pub did: Option<Did>,
    /// Name of a resource.
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Implemented by hand: derives would require `Res` itself to implement each
// trait, though no value of `Res` is stored.

impl<Res: ResourceType> Clone for ResourceHandleT<Res> {
    fn clone(&self) -> Self {
        Self {
            account: self.account.clone(),
            r#type: PhantomData,
            id: self.id,
            did: self.did.clone(),
            name: self.name.clone(),
        }
    }
}

impl<Res: ResourceType> PartialEq for ResourceHandleT<Res> {
    fn eq(&self, other: &Self) -> bool {
        self.account == other.account
            && self.id == other.id
            && self.did == other.did
            && self.name == other.name
    }
}

impl<Res: ResourceType> Eq for ResourceHandleT<Res> {}

impl<Res: ResourceType> std::fmt::Debug for ResourceHandleT<Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceHandleT")
            .field("account", &self.account)
            .field("type", &Res::type_uri_str())
            .field("id", &self.id)
            .field("did", &self.did)
            .field("name", &self.name)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<Res: ResourceType> From<ResourceHandleT<Res>> for ResourceHandle {
    fn from(value: ResourceHandleT<Res>) -> Self {
        let ResourceHandleT {
            account,
            r#type: _,
            id,
            did,
            name,
        } = value;
        Self {
            account,
            r#type: Res::type_uri().cheap_clone(),
            id,
            did,
            name,
        }
    }
}

/// Fails if the handle points to a type other than `Res`.
impl<Res: ResourceType> TryFrom<ResourceHandle> for ResourceHandleT<Res> {
    type Error = TypeMismatchError;

    fn try_from(value: ResourceHandle) -> Result<Self, Self::Error> {
        let ResourceHandle {
            account,
            r#type,
            id,
            did,
            name,
        } = value;

        if r#type != *Res::type_uri() {
            return Err(TypeMismatchError {
                expected: Res::type_uri().cheap_clone(),
                actual: r#type.into(),
            });
        }

        Ok(Self {
            account,
            r#type: PhantomData,
            id,
            did,
            name,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<Res: ResourceType> From<ResourceHandleT<Res>> for ResourceRefT<Res> {
    fn from(value: ResourceHandleT<Res>) -> Self {
        let ResourceHandleT {
            account,
            r#type: _,
            id,
            did,
            name,
        } = value;
        Self {
            account: Some(account.into()),
            id: Some(id),
            did,
            r#type: PhantomData,
            name: Some(name),
        }
    }
}

impl<Res: ResourceType> From<ResourceHandleT<Res>> for ResourceRef {
    fn from(value: ResourceHandleT<Res>) -> Self {
        ResourceRefT::from(value).into()
    }
}

impl<Res: ResourceType> IntoResourceRef for ResourceHandleT<Res> {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
