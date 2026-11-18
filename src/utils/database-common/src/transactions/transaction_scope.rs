// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use tracing::Instrument;
use tracing::instrument::Instrumented;

use crate::{DatabaseTransactionManager, TransactionRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A transaction opened for one block of code. Components resolved from its
/// catalog take part in the transaction; [`TransactionScope::finish`] commits it
/// on `Ok` and rolls it back on `Err`. Write blocks with
/// [`transaction!`](crate::transaction) rather than with this type directly
pub struct TransactionScope {
    catalog: dill::Catalog,
    maybe_open_transaction: Option<OpenTransaction>,
}

struct OpenTransaction {
    transaction_manager: Arc<dyn DatabaseTransactionManager>,
    transaction_ref: TransactionRef,
}

impl TransactionScope {
    /// Opens a transaction with the catalog's `DatabaseTransactionManager`.
    /// When `enabled` is false, no transaction is opened and the scope works on
    /// `catalog` as given
    pub async fn begin(
        catalog: &impl TransactionBaseCatalog,
        enabled: bool,
    ) -> Result<Self, InternalError> {
        let catalog = catalog.base_catalog();
        if !enabled {
            return Ok(Self {
                catalog,
                maybe_open_transaction: None,
            });
        }

        let transaction_manager = catalog
            .get_one::<dyn DatabaseTransactionManager>()
            .int_err()?;
        let transaction_ref = transaction_manager.make_transaction_ref().await?;

        let mut b = catalog.builder_chained();
        transaction_ref.register(&mut b);

        Ok(Self {
            catalog: b.build(),
            maybe_open_transaction: Some(OpenTransaction {
                transaction_manager,
                transaction_ref,
            }),
        })
    }

    /// The catalog that resolves components inside the transaction
    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn resolve<Iface>(&self) -> Result<Arc<Iface>, InternalError>
    where
        Iface: 'static + ?Sized + Send + Sync,
    {
        self.catalog.get_one::<Iface>().int_err()
    }

    /// Commits the transaction when `result` is `Ok` and rolls it back when it
    /// is `Err`. A failed commit or rollback replaces `result`
    pub async fn finish<T, E>(self, result: Result<T, E>) -> Result<T, E>
    where
        E: From<InternalError>,
    {
        let Self {
            catalog,
            maybe_open_transaction,
        } = self;

        // The transaction can end only once nothing else refers to it
        drop(catalog);

        let Some(OpenTransaction {
            transaction_manager,
            transaction_ref,
        }) = maybe_open_transaction
        else {
            return result;
        };

        match result {
            Ok(value) => {
                tracing::debug!("Transaction COMMIT");
                transaction_manager
                    .commit_transaction(transaction_ref)
                    .await?;
                Ok(value)
            }
            Err(e) => {
                tracing::warn!("Transaction ROLLBACK");
                transaction_manager
                    .rollback_transaction(transaction_ref)
                    .await?;
                Err(e)
            }
        }
    }

    pub fn instrument<F: Future>(block: F, span: tracing::Span) -> Instrumented<F> {
        block.instrument(span)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A catalog a transaction can be opened from: the transaction catalog is
/// chained on it
pub trait TransactionBaseCatalog {
    fn base_catalog(&self) -> dill::Catalog;
}

impl TransactionBaseCatalog for dill::Catalog {
    fn base_catalog(&self) -> dill::Catalog {
        self.clone()
    }
}

/// For singletons, which hold their catalog weakly
impl TransactionBaseCatalog for dill::CatalogWeakRef {
    fn base_catalog(&self) -> dill::Catalog {
        self.upgrade()
    }
}

impl<T: TransactionBaseCatalog + ?Sized> TransactionBaseCatalog for &T {
    fn base_catalog(&self) -> dill::Catalog {
        (**self).base_catalog()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
