// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::pin::Pin;

use futures::Stream;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct PaginationOpts {
    pub limit: usize,
    pub offset: usize,
}

impl PaginationOpts {
    pub fn from_page(page: usize, per_page: usize) -> Self {
        Self {
            offset: page * per_page,
            limit: per_page,
        }
    }

    pub fn safe_limit(&self, total: usize) -> usize {
        let rest = total.saturating_sub(self.offset);

        self.limit.min(rest)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct EntityPageListing<Entity> {
    pub list: Vec<Entity>,
    pub total_count: usize,
}

// Entity may be a type that doesn't implement Default, so we implement it
// manually
impl<Entity> Default for EntityPageListing<Entity> {
    fn default() -> Self {
        Self {
            list: Vec::new(),
            total_count: 0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EntityPageStream<'a, Entity> =
    Pin<Box<dyn Stream<Item = Result<Entity, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `EntityPageStreamer` is a helper hiding the pagination logic.
/// The default page size is `100` rows, but this value can be changed if
/// desired.
///
/// The primary method is [`EntityPageStreamer::into_stream()`].
///
/// # Examples
/// ```
/// // Usage without arguments for page callback:
/// let dataset_handles_stream = EntityPageStreamer::default().into_stream(
///     || async { Ok(()) },
///     |_, pagination| self.list_all_dataset_handles(pagination),
/// );
///
/// // An example use case with passing some value to each page callback:
/// let dataset_handles_by_owner_name_stream = EntityPageStreamer::default().into_stream(
///     move || async move {
///         let owner_id = self
///             .resolve_account_id_by_maybe_name(Some(&owner_name))
///             .await?;
///         Ok(Arc::new(owner_id))
///     },
///     move |owner_id, pagination| async move {
///         self.list_all_dataset_handles_by_owner_name(&owner_id, pagination)
///             .await
///     },
/// )
///
/// // More examples can be found in unit tests.
/// ```
pub struct EntityPageStreamer {
    start_offset: usize,
    page_limit: usize,
}

impl Default for EntityPageStreamer {
    fn default() -> Self {
        Self {
            start_offset: 0,
            page_limit: 100,
        }
    }
}

impl EntityPageStreamer {
    pub fn new(start_offset: usize, page_limit: usize) -> Self {
        Self {
            start_offset,
            page_limit,
        }
    }

    /// # Arguments
    /// * `get_args_callback` - a function to generating arguments for
    ///   `next_entities_callback`. Note, it is only called once.
    /// * `next_entities_callback` - a function that will be called for each
    ///   page.
    ///
    /// # Examples
    /// You can find examples of use in [`EntityPageStreamer`].
    pub fn into_stream<'a, Entity, Args, HInitArgs, HInitArgsFut, HListing, HListingFut>(
        self,
        get_args_callback: HInitArgs,
        next_entities_callback: HListing,
    ) -> EntityPageStream<'a, Entity>
    where
        Entity: Send + 'a,
        Args: Clone + Send + 'a,
        HInitArgs: FnOnce() -> HInitArgsFut + Send + 'a,
        HInitArgsFut: Future<Output = Result<Args, InternalError>> + Send + 'a,
        HListing: Fn(Args, PaginationOpts) -> HListingFut + Send + 'a,
        HListingFut: Future<Output = Result<EntityPageListing<Entity>, InternalError>> + Send + 'a,
    {
        let init_offset = self.start_offset;
        let init_limit = self.page_limit;

        Box::pin(async_stream::try_stream! {
            // Init arguments
            let args = get_args_callback().await?;

            // Tracking pagination progress
            let mut offset = init_offset;
            let limit = init_limit;

            loop {
                // Load a page of dataset entities
                let entities_page =
                    next_entities_callback(args.clone(), PaginationOpts { limit, offset })
                        .await?;

                // Actually read entities
                let loaded_entries_count = entities_page.list.len();

                // Stream the entities
                for entity in entities_page.list {
                    yield entity;
                }

                // Next page
                offset += loaded_entries_count;
                if offset >= entities_page.total_count {
                    break;
                }
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
pub struct EventModel {
    pub event_id: i64,
    pub event_payload: sqlx::types::JsonValue,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct BatchLookup<T, Id, Err> {
    pub found: Vec<T>,
    pub not_found: Vec<(Id, Err)>,
}

pub struct BatchLookupCreateOptions<T, Id, Err, FoundByFn, NotFoundErrFn, FoundItemsComparator>
where
    Id: Clone + Hash + Eq,
    FoundByFn: FnOnce(&Vec<T>) -> HashSet<Id>,
    NotFoundErrFn: Fn(&Id) -> Err,
    // Based on Vec::<T>::sort_by_key() type signature.
    FoundItemsComparator: FnMut(&T, &T) -> std::cmp::Ordering,
{
    pub found_ids_fn: FoundByFn,
    pub not_found_err_fn: NotFoundErrFn,
    pub maybe_found_items_comparator: Option<FoundItemsComparator>,
    pub _phantom: PhantomData<T>,
}

impl<T, Id, Err> BatchLookup<T, Id, Err> {
    pub fn from_found_items<FoundByFn, NotFoundErrFn, FoundItemsComparator>(
        mut found: Vec<T>,
        ids: &[&Id],
        options: BatchLookupCreateOptions<
            T,
            Id,
            Err,
            FoundByFn,
            NotFoundErrFn,
            FoundItemsComparator,
        >,
    ) -> BatchLookup<T, Id, Err>
    where
        Id: Clone + Hash + Eq,
        FoundByFn: FnOnce(&Vec<T>) -> HashSet<Id>,
        NotFoundErrFn: Fn(&Id) -> Err,
        FoundItemsComparator: FnMut(&T, &T) -> std::cmp::Ordering,
    {
        if let Some(comparator) = options.maybe_found_items_comparator {
            found.sort_by(comparator);
        }

        let found_ids_set = (options.found_ids_fn)(&found);
        let mut not_found = Vec::with_capacity(ids.len() - found.len());

        for id in ids {
            if !found_ids_set.contains(*id) {
                let cloned_id = (*id).clone();
                let not_found_err = (options.not_found_err_fn)(*id);

                not_found.push((cloned_id, not_found_err));
            }
        }

        BatchLookup { found, not_found }
    }
}

#[test]
fn test_batch_lookup_from_found_items() {
    use pretty_assertions::assert_eq;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Item {
        id: u32,
    }

    let ids_to_search = vec![&0, &1, &2, &3, &4, &5, &6, &7];
    // Found items have mixed order (simulating a database query result).
    let found_items = vec![
        Item { id: 2 },
        Item { id: 1 },
        Item { id: 3 },
        Item { id: 5 },
    ];

    let lookup = BatchLookup::<Item, u32, String>::from_found_items(
        found_items.clone(),
        &ids_to_search,
        BatchLookupCreateOptions {
            found_ids_fn: |found_items| found_items.iter().map(|item| item.id).collect(),
            not_found_err_fn: |id| format!("{id} not found"),
            maybe_found_items_comparator: None::<fn(&_, &_) -> _>,
            _phantom: PhantomData,
        },
    );

    assert_eq!(
        found_items, // Mixed order is preserved
        lookup.found
    );
    assert_eq!(
        [
            (0, "0 not found".to_string()),
            (4, "4 not found".to_string()),
            (6, "6 not found".to_string()),
            (7, "7 not found".to_string())
        ],
        *lookup.not_found
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
