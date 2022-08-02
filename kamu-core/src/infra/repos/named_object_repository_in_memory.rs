// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::repos::named_object_repository::{DeleteError, GetError, SetError};
use crate::domain::*;

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Mutex;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryInMemory {
    objects_by_name: Mutex<HashMap<String, Bytes>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl NamedObjectRepositoryInMemory {
    pub fn new() -> Self {
        Self {
            objects_by_name: Mutex::new(HashMap::new()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryInMemory {
    /// Resolves reference to the object hash it's pointing to
    async fn get(&self, name: &str) -> Result<Bytes, GetError> {
        let objects_by_name = self.objects_by_name.lock().unwrap();
        let res = objects_by_name.get(name);
        match res {
            Some(bytes) => Ok(bytes.clone()),
            None => Err(GetError::NotFound(NotFoundError {
                name: String::from(name),
            })),
        }
    }

    /// Update referece to point at the specified object hash
    async fn set(&self, name: &str, data: &[u8]) -> Result<(), SetError> {
        let mut objects_by_name = self.objects_by_name.lock().unwrap();
        objects_by_name.insert(String::from(name), Bytes::copy_from_slice(data));
        Ok(())
    }

    /// Deletes specified reference
    async fn delete(&self, name: &str) -> Result<(), DeleteError> {
        let mut objects_by_name = self.objects_by_name.lock().unwrap();
        objects_by_name.remove(name);
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
