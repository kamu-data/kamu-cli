// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;

use super::ManagedOperationImpl;
use crate::{ManagedOperationFactory, ManagedOperationRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ManagedOperationFactoryImpl {
    catalog: Catalog,
}

#[component(pub)]
#[interface(dyn ManagedOperationFactory)]
impl ManagedOperationFactoryImpl {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ManagedOperationFactory for ManagedOperationFactoryImpl {
    async fn create_operation(&self) -> ManagedOperationRef {
        ManagedOperationRef::new(Arc::new(ManagedOperationImpl::new(self.catalog.clone())))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
