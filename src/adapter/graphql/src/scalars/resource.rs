// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_string_scalar!(ResourceID, odf::metadata::resource::ResourceID);
simple_string_scalar!(ResourceName, odf::metadata::resource::ResourceName);
simple_string_scalar!(TypeName, odf::metadata::resource::TypeName);
simple_string_scalar!(TypeUri, odf::metadata::resource::TypeUri);
simple_string_scalar!(TypeRef, odf::metadata::resource::TypeRef);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceRef(odf::metadata::resource::ResourceRef);

#[Object]
impl ResourceRef {
    pub async fn account(&self) -> Option<AccountRef> {
        // TODO: PERF: Avoid cloning
        self.0.account().cloned().map(Into::into)
    }

    pub async fn r#type(&self) -> TypeRef<'_> {
        self.0.typ().into()
    }

    pub async fn id(&self) -> Option<ResourceID<'_>> {
        self.0.id().map(Into::into)
    }

    pub async fn name(&self) -> Option<ResourceName<'_>> {
        self.0.name().map(Into::into)
    }
}

impl From<odf::metadata::resource::ResourceRef> for ResourceRef {
    fn from(value: odf::metadata::resource::ResourceRef) -> Self {
        Self(value)
    }
}

impl From<ResourceRef> for odf::metadata::resource::ResourceRef {
    fn from(value: ResourceRef) -> Self {
        value.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceSelector(odf::metadata::resource::ResourceSelector);

#[Object]
impl ResourceSelector {
    pub async fn account(&self) -> Option<AccountRef> {
        // TODO: PERF: Avoid cloning
        self.0.account().cloned().map(Into::into)
    }
}

impl From<odf::metadata::resource::ResourceSelector> for ResourceSelector {
    fn from(value: odf::metadata::resource::ResourceSelector) -> Self {
        Self(value)
    }
}

impl From<ResourceSelector> for odf::metadata::resource::ResourceSelector {
    fn from(value: ResourceSelector) -> Self {
        value.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetSelector(odf::metadata::dataset::DatasetSelector);

#[Object]
impl DatasetSelector {
    pub async fn account(&self) -> Option<AccountRef> {
        // TODO: PERF: Avoid cloning
        self.0.account().cloned().map(Into::into)
    }
}

impl From<odf::metadata::dataset::DatasetSelector> for DatasetSelector {
    fn from(value: odf::metadata::dataset::DatasetSelector) -> Self {
        Self(value)
    }
}

impl From<DatasetSelector> for odf::metadata::dataset::DatasetSelector {
    fn from(value: DatasetSelector) -> Self {
        value.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ValueRef(odf::metadata::config::ValueRef);

#[Object]
impl ValueRef {
    pub async fn account(&self) -> Option<AccountRef> {
        // TODO: PERF: Avoid cloning
        self.0.account().cloned().map(Into::into)
    }

    pub async fn r#type(&self) -> TypeRef<'_> {
        self.0.typ().into()
    }

    pub async fn id(&self) -> Option<ResourceID<'_>> {
        self.0.id().map(Into::into)
    }

    pub async fn name(&self) -> Option<ResourceName<'_>> {
        self.0.name().map(Into::into)
    }

    pub async fn path(&self) -> Option<&str> {
        self.0.path.as_deref()
    }
}

impl From<odf::metadata::config::ValueRef> for ValueRef {
    fn from(value: odf::metadata::config::ValueRef) -> Self {
        Self(value)
    }
}

impl From<ValueRef> for odf::metadata::config::ValueRef {
    fn from(value: ValueRef) -> Self {
        value.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct PersistentVolumeRef(odf::metadata::storage::PersistentVolumeRef);

#[Object]
impl PersistentVolumeRef {
    pub async fn account(&self) -> Option<AccountRef> {
        // TODO: PERF: Avoid cloning
        self.0.account().cloned().map(Into::into)
    }

    pub async fn r#type(&self) -> TypeRef<'_> {
        self.0.typ().into()
    }

    pub async fn id(&self) -> Option<ResourceID<'_>> {
        self.0.id().map(Into::into)
    }

    pub async fn name(&self) -> Option<ResourceName<'_>> {
        self.0.name().map(Into::into)
    }
}

impl From<odf::metadata::storage::PersistentVolumeRef> for PersistentVolumeRef {
    fn from(value: odf::metadata::storage::PersistentVolumeRef) -> Self {
        Self(value)
    }
}

impl From<PersistentVolumeRef> for odf::metadata::storage::PersistentVolumeRef {
    fn from(value: PersistentVolumeRef) -> Self {
        value.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
