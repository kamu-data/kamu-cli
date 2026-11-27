// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};

use internal_error::{InternalError, ResultIntoInternal};

use crate::ResourceDispatcherFactory;
use crate::domain::{
    ApplyResourceRejection,
    ApplyResourceRejectionCategory,
    GenericResourceQueryService,
    InvalidOwnerReferenceError,
    ResourceAccountAccess,
    ResourceAccountOperation,
    ResourceHandle,
    ResourceHandleRow,
    ResourceHeadersValidationError,
    ResourceID,
    ResourceName,
    ResourceOperationError,
    ResourceRef,
    TypeRef,
    TypeUri,
    resource_type_name,
    validate_owner_reference,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves the owners named by an apply. A new owner the principal may not
/// read, as the guard of its type decides, is not found, so a rejection says
/// nothing about it
pub(crate) struct OwnerReferenceResolver<'a> {
    pub generic_resource_query_service: &'a dyn GenericResourceQueryService,
    pub resource_dispatcher_factory: &'a ResourceDispatcherFactory,
    /// The principal of the apply
    pub principal: &'a odf::metadata::auth::PrincipalHandle,
    /// Account of the owned resource, which its owners share
    pub account: &'a odf::ResourceOwnerHandle,
    /// `None` for a resource to create
    pub owned_id: Option<ResourceID>,
    /// References the owned resource already has. They stay as they are,
    /// whatever became of the owner, so their names may be out of date.
    pub current_owners: &'a [ResourceHandle],
}

impl OwnerReferenceResolver<'_> {
    pub async fn resolve(
        &self,
        owner_references: &[ResourceRef],
    ) -> Result<Vec<ResourceHandle>, ResolveOwnerReferencesError> {
        let mut owner_ids = Vec::with_capacity(owner_references.len());
        let mut seen_ids = HashSet::new();

        for owner_reference in owner_references {
            validate_owner_reference(owner_reference).map_err(|err| {
                ResourceHeadersValidationError::from(InvalidOwnerReferenceError::from(err))
            })?;

            owner_ids.push(self.resolve_id(owner_reference).await?);
        }

        let mut rows_by_id = self.find_new_owners(&owner_ids).await?;
        self.retain_readable(&mut rows_by_id).await?;

        owner_references
            .iter()
            .zip(owner_ids)
            .map(|(owner_reference, id)| {
                if self.owned_id == Some(id) {
                    return Err(OwnerReferenceRejectedError::OwnsItself { id }.into());
                }

                // An unreadable owner is not found before it is a duplicate,
                // which would reveal the ID its name resolves to
                match (self.current_owner(id), rows_by_id.get(&id)) {
                    (Some(handle), _) => {
                        Self::ensure_first_reference(&mut seen_ids, id)?;
                        self.check_account_and_type(owner_reference, id, &handle.r#type)?;
                        Ok(handle.clone())
                    }
                    (None, Some(row)) => {
                        Self::ensure_first_reference(&mut seen_ids, id)?;
                        let handle = Self::into_live_owner(row.clone())?;
                        self.check_account_and_type(owner_reference, id, &handle.r#type)?;
                        Self::check_name(owner_reference, &handle)?;
                        Ok(handle)
                    }
                    (None, None) => {
                        Err(OwnerReferenceRejectedError::not_found(owner_reference).into())
                    }
                }
            })
            .collect()
    }

    async fn resolve_id(
        &self,
        owner_reference: &ResourceRef,
    ) -> Result<ResourceID, ResolveOwnerReferencesError> {
        let (name, r#type) = match owner_reference {
            ResourceRef { id: Some(id), .. } => return Ok(*id),
            ResourceRef {
                name: Some(name),
                r#type: Some(r#type),
                ..
            } => (name, r#type),
            ResourceRef { .. } => unreachable!("the reference is valid"),
        };

        let TypeRef::Uri(schema) = r#type else {
            return Err(ResourceHeadersValidationError::from(
                InvalidOwnerReferenceError::UnknownType(r#type.clone()),
            )
            .into());
        };

        self.ensure_same_account(owner_reference)?;

        self.generic_resource_query_service
            .find_resource_id_by_name(self.account_id(), schema, name)
            .await?
            .ok_or_else(|| OwnerReferenceRejectedError::not_found(owner_reference).into())
    }

    async fn find_new_owners(
        &self,
        owner_ids: &[ResourceID],
    ) -> Result<HashMap<ResourceID, ResourceHandleRow>, InternalError> {
        let new_ids = owner_ids
            .iter()
            .copied()
            .filter(|id| self.current_owner(*id).is_none())
            .collect::<Vec<_>>();
        if new_ids.is_empty() {
            return Ok(HashMap::new());
        }

        Ok(self
            .generic_resource_query_service
            .find_resource_handles_by_ids(self.account_id(), &new_ids)
            .await?
            .into_iter()
            .map(|row| (ResourceID::new(row.id), row))
            .collect())
    }

    /// Drops the owners the principal may not read
    async fn retain_readable(
        &self,
        rows_by_id: &mut HashMap<ResourceID, ResourceHandleRow>,
    ) -> Result<(), InternalError> {
        let mut ids_by_schema = BTreeMap::<String, Vec<ResourceID>>::new();
        for (id, row) in rows_by_id.iter() {
            ids_by_schema
                .entry(row.schema.clone())
                .or_default()
                .push(*id);
        }

        for (schema, ids) in ids_by_schema {
            // A stored resource's type has a dispatcher
            let dispatcher = self
                .resource_dispatcher_factory
                .crud_dispatcher_for_trusted_schema(&schema)?;

            let results = match dispatcher
                .check_account_access(self.principal, self.account, ResourceAccountOperation::Read)
                .await
            {
                Ok(ResourceAccountAccess::Allowed) => continue,
                Ok(ResourceAccountAccess::PerResource) => {
                    dispatcher
                        .check_resource_operations(
                            self.principal,
                            self.account,
                            ResourceAccountOperation::Read,
                            &ids,
                        )
                        .await?
                }
                Err(ResourceOperationError::Access(_) | ResourceOperationError::NotFound(_)) => {
                    for id in &ids {
                        rows_by_id.remove(id);
                    }
                    continue;
                }
                Err(ResourceOperationError::Internal(err)) => return Err(err),
            };

            for (id, result) in ids.into_iter().zip(results) {
                match result {
                    Ok(()) => {}
                    Err(
                        ResourceOperationError::Access(_) | ResourceOperationError::NotFound(_),
                    ) => {
                        rows_by_id.remove(&id);
                    }
                    Err(ResourceOperationError::Internal(err)) => return Err(err),
                }
            }
        }

        Ok(())
    }

    fn ensure_first_reference(
        seen_ids: &mut HashSet<ResourceID>,
        id: ResourceID,
    ) -> Result<(), OwnerReferenceRejectedError> {
        if !seen_ids.insert(id) {
            return Err(OwnerReferenceRejectedError::Duplicate { id });
        }
        Ok(())
    }

    fn into_live_owner(
        row: ResourceHandleRow,
    ) -> Result<ResourceHandle, OwnerReferenceRejectedError> {
        // Being deleted or deleted
        if row.deletion_requested_at.is_some() {
            return Err(OwnerReferenceRejectedError::NotLive {
                id: ResourceID::new(row.id),
            });
        }
        Ok(row.into_handle())
    }

    /// Neither ever changes, so a current owner is checked without looking it
    /// up
    fn check_account_and_type(
        &self,
        owner_reference: &ResourceRef,
        id: ResourceID,
        actual_type: &TypeUri,
    ) -> Result<(), ResolveOwnerReferencesError> {
        self.ensure_same_account(owner_reference)?;

        if let Some(expected) = &owner_reference.r#type {
            let type_matches = match expected {
                TypeRef::Uri(expected) => expected == actual_type,
                TypeRef::Name(expected) => {
                    *expected == resource_type_name(actual_type).int_err()?
                }
            };
            if !type_matches {
                return Err(OwnerReferenceRejectedError::TypeMismatch {
                    id,
                    expected: expected.clone(),
                    actual: actual_type.clone(),
                }
                .into());
            }
        }

        Ok(())
    }

    fn check_name(
        owner_reference: &ResourceRef,
        owner: &ResourceHandle,
    ) -> Result<(), OwnerReferenceRejectedError> {
        if let Some(expected) = &owner_reference.name
            && *expected != owner.name
        {
            return Err(OwnerReferenceRejectedError::NameMismatch {
                id: owner.id,
                expected: expected.clone(),
                actual: owner.name.clone(),
            });
        }
        Ok(())
    }

    fn current_owner(&self, id: ResourceID) -> Option<&ResourceHandle> {
        self.current_owners.iter().find(|handle| handle.id == id)
    }

    fn account_id(&self) -> &odf::AccountID {
        &self.account.did
    }

    /// Rejects a reference unless every given part of its account is that of
    /// the owned resource's account, which is the default
    fn ensure_same_account(
        &self,
        owner_reference: &ResourceRef,
    ) -> Result<(), OwnerReferenceRejectedError> {
        let Some(account) = &owner_reference.account else {
            return Ok(());
        };

        fn given_matches<T: PartialEq>(given: Option<&T>, actual: Option<&T>) -> bool {
            given.is_none() || given == actual
        }

        if given_matches(account.id.as_ref(), Some(&self.account.id))
            && given_matches(account.did.as_ref(), Some(&self.account.did))
            && given_matches(account.name.as_ref(), Some(&self.account.name))
        {
            Ok(())
        } else {
            Err(OwnerReferenceRejectedError::OtherAccount {
                reference: describe_reference(owner_reference),
            })
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn describe_reference(owner_reference: &ResourceRef) -> String {
    match owner_reference {
        ResourceRef { id: Some(id), .. } => id.to_string(),
        ResourceRef {
            name: Some(name),
            r#type: Some(r#type),
            ..
        } => {
            let type_name = match r#type {
                TypeRef::Uri(schema) => resource_type_name(schema).ok(),
                TypeRef::Name(type_name) => Some(type_name.clone()),
            };
            match type_name {
                Some(type_name) => format!("{type_name}:{name}"),
                None => format!("{type}:{name}"),
            }
        }
        ResourceRef { .. } => unreachable!("the reference is valid"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveOwnerReferencesError {
    #[error(transparent)]
    InvalidHeaders(#[from] ResourceHeadersValidationError),

    #[error(transparent)]
    Rejected(#[from] OwnerReferenceRejectedError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub(crate) enum OwnerReferenceRejectedError {
    #[error("Owner '{reference}' is not found in the account of the resource")]
    NotFound { reference: String },

    #[error("Owner '{reference}' belongs to another account than the resource")]
    OtherAccount { reference: String },

    #[error("Owner '{id}' is deleted or being deleted")]
    NotLive { id: ResourceID },

    #[error("Owner '{id}' is of type '{actual}', not '{expected}'")]
    TypeMismatch {
        id: ResourceID,
        expected: TypeRef,
        actual: TypeUri,
    },

    #[error("Owner '{id}' has the name '{actual}', not '{expected}'")]
    NameMismatch {
        id: ResourceID,
        expected: ResourceName,
        actual: ResourceName,
    },

    #[error("Resource '{id}' cannot own itself")]
    OwnsItself { id: ResourceID },

    #[error("Owner '{id}' is referenced more than once")]
    Duplicate { id: ResourceID },
}

impl OwnerReferenceRejectedError {
    fn not_found(owner_reference: &ResourceRef) -> Self {
        Self::NotFound {
            reference: describe_reference(owner_reference),
        }
    }

    fn category(&self) -> ApplyResourceRejectionCategory {
        match self {
            Self::NotFound { .. } | Self::NotLive { .. } => {
                ApplyResourceRejectionCategory::ReferencedObjectMissing
            }
            Self::OtherAccount { .. }
            | Self::TypeMismatch { .. }
            | Self::NameMismatch { .. }
            | Self::OwnsItself { .. }
            | Self::Duplicate { .. } => ApplyResourceRejectionCategory::BusinessValidationFailed,
        }
    }
}

impl From<OwnerReferenceRejectedError> for ApplyResourceRejection {
    fn from(err: OwnerReferenceRejectedError) -> Self {
        Self {
            category: err.category(),
            message: err.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
