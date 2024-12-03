// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use super::AcceptVisitorError;
use crate::{Infallible, MetadataChainVisitor};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetCurrentStateAcceptor: Send + Sync {
    /// A method of accepting Visitors ([MetadataChainVisitor]) that allows us
    /// to go through the metadata chain once and, if desired,
    /// bypassing blocks of no interest.
    async fn accept(
        &self,
        visitors: &mut [&mut dyn MetadataChainVisitor<Error = Infallible>],
    ) -> Result<(), AcceptVisitorError<InternalError>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetSimpleAcceptorExt: DatasetCurrentStateAcceptor {
    /// An auxiliary method that simplifies the work if only one Visitor is
    /// used.
    async fn accept_one<V>(&self, mut visitor: V) -> Result<V, AcceptVisitorError<InternalError>>
    where
        V: MetadataChainVisitor<Error = Infallible>,
    {
        match self.accept(&mut [&mut visitor]).await {
            Ok(_) => {}
            Err(_) => unreachable!(),
        };

        Ok(visitor)
    }
}

impl<T> DatasetSimpleAcceptorExt for T where T: DatasetCurrentStateAcceptor + ?Sized {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
