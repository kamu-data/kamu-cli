// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskDefinition {
    inner: Box<dyn TaskDefinitionInner>,
}

impl TaskDefinition {
    pub fn new<T: TaskDefinitionInner + 'static>(inner: T) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

    pub fn task_type(&self) -> &'static str {
        self.inner.task_type()
    }

    /// Shared reference downcast
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.as_any().downcast_ref::<T>()
    }

    /// Owned downcast (consumes self). Returns the definition unchanged when
    /// it is not a `T`.
    pub fn downcast<T: 'static>(self) -> Result<Box<T>, Self> {
        // Once converted to `Box<dyn Any>`, the definition cannot be turned back
        // into a `TaskDefinitionInner`, so the type is checked before converting
        if !self.inner.as_any().is::<T>() {
            return Err(self);
        }

        Ok(self
            .inner
            .into_any()
            .downcast::<T>()
            .unwrap_or_else(|_| unreachable!("Type was checked before the downcast")))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskDefinitionInner: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn into_any(self: Box<Self>) -> Box<dyn Any>;

    fn task_type(&self) -> &'static str;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
