// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use domain::{DeleteDatasetError, RenameDatasetError};
use kamu_core::{self as domain};
use opendatafabric as odf;

use super::DatasetMetadataMut;
use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct DatasetMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    /// Access to the mutable metadata of the dataset
    async fn metadata(&self) -> DatasetMetadataMut {
        DatasetMetadataMut::new(self.dataset_handle.clone())
    }

    /// Rename the dataset
    async fn rename(&self, ctx: &Context<'_>, new_name: DatasetName) -> Result<RenameResult> {
        if self
            .dataset_handle
            .alias
            .dataset_name
            .as_str()
            .eq(new_name.as_str())
        {
            return Ok(RenameResult::NoChanges(RenameResultNoChanges {
                preserved_name: new_name.into(),
            }));
        }

        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        match dataset_repo
            .rename_dataset(&self.dataset_handle.as_local_ref(), &new_name)
            .await
        {
            Ok(_) => Ok(RenameResult::Success(RenameResultSuccess {
                old_name: self.dataset_handle.alias.dataset_name.clone().into(),
                new_name: new_name.into(),
            })),
            Err(RenameDatasetError::NameCollision(e)) => {
                Ok(RenameResult::NameCollision(RenameResultNameCollision {
                    colliding_alias: e.alias.into(),
                }))
            }
            // "Not found" should not be reachable, since we've just resolved the dataset by ID
            Err(RenameDatasetError::NotFound(e)) => Err(e.int_err().into()),
            Err(RenameDatasetError::Access(e)) => Err(e.int_err().into()),
            Err(RenameDatasetError::Internal(e)) => Err(e.into()),
        }
    }

    /// Delete the dataset
    async fn delete(&self, ctx: &Context<'_>) -> Result<DeleteResult> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        match dataset_repo
            .delete_dataset(&self.dataset_handle.as_local_ref())
            .await
        {
            Ok(_) => Ok(DeleteResult::Success(DeleteResultSuccess {
                deleted_dataset: self.dataset_handle.alias.clone().into(),
            })),
            Err(DeleteDatasetError::DanglingReference(e)) => Ok(DeleteResult::DanglingReference(
                DeleteResultDanglingReference {
                    not_deleted_dataset: self.dataset_handle.alias.clone().into(),
                    dangling_child_refs: e
                        .children
                        .iter()
                        .map(|child_dataset| child_dataset.as_local_ref().into())
                        .collect(),
                },
            )),
            // "Not found" should not be reachable, since we've just resolved the dataset by ID
            Err(DeleteDatasetError::NotFound(e)) => Err(e.int_err().into()),
            Err(DeleteDatasetError::Access(e)) => Err(e.int_err().into()),
            Err(DeleteDatasetError::Internal(e)) => Err(e.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RenameResult {
    Success(RenameResultSuccess),
    NoChanges(RenameResultNoChanges),
    NameCollision(RenameResultNameCollision),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct RenameResultSuccess {
    pub old_name: DatasetName,
    pub new_name: DatasetName,
}

#[ComplexObject]
impl RenameResultSuccess {
    async fn message(&self) -> String {
        format!("Success")
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct RenameResultNoChanges {
    pub preserved_name: DatasetName,
}

#[ComplexObject]
impl RenameResultNoChanges {
    async fn message(&self) -> String {
        format!("No changes")
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct RenameResultNameCollision {
    pub colliding_alias: DatasetAlias,
}

#[ComplexObject]
impl RenameResultNameCollision {
    async fn message(&self) -> String {
        format!("Dataset '{}' already exists", self.colliding_alias)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DeleteResult {
    Success(DeleteResultSuccess),
    DanglingReference(DeleteResultDanglingReference),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct DeleteResultSuccess {
    pub deleted_dataset: DatasetAlias,
}

#[ComplexObject]
impl DeleteResultSuccess {
    async fn message(&self) -> String {
        format!("Success")
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct DeleteResultDanglingReference {
    pub not_deleted_dataset: DatasetAlias,
    pub dangling_child_refs: Vec<DatasetRef>,
}

#[ComplexObject]
impl DeleteResultDanglingReference {
    async fn message(&self) -> String {
        format!(
            "Dataset '{}' has {} dangling reference(s)",
            self.not_deleted_dataset,
            self.dangling_child_refs.len()
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////
