// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceRegistry {
    fn resolve_type(&self, typ: &TypeRef) -> Result<TypeUri, UnregisteredTypeError>;

    fn ref_into_typed<Res: ResourceType>(
        &self,
        r: ResourceRef,
    ) -> Result<ResourceRefT<Res>, IntoTypedRefError> {
        let ResourceRef {
            account,
            id,
            did,
            r#type,
            name,
        } = r;

        if let Some(typ) = r#type {
            let typ = self.resolve_type(&typ)?;
            if typ != *Res::type_uri() {
                return Err(TypeMismatchError {
                    expected: Res::type_uri().cheap_clone(),
                    actual: typ.into(),
                }
                .into());
            }
        }

        Ok(ResourceRefT {
            account,
            id,
            did,
            r#type: std::marker::PhantomData,
            name,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Unregistered type: {0}")]
pub struct UnregisteredTypeError(TypeRef);

#[derive(thiserror::Error, Debug)]
#[error("Expected a resource of type `{expected}`, got `{actual}`")]
pub struct TypeMismatchError {
    pub expected: TypeUri,
    pub actual: TypeRef,
}

#[derive(thiserror::Error, Debug)]
pub enum IntoTypedRefError {
    #[error(transparent)]
    Unregistered(#[from] UnregisteredTypeError),

    #[error(transparent)]
    TypeMismatch(#[from] TypeMismatchError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
