// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::marker::PhantomData;

use internal_error::{ErrorIntoInternal, InternalError};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Aggregate wraps the state reconstructed by [`crate::Projection`] from series
/// of events and provides ability to mutate this state through commands.
/// Updates can then be saved into a persistent storage represented by
/// [`crate::EventStore`].
///
/// To define your own aggregate use `Aggregate` derive macro as:
/// ```ignore
/// #[derive(Aggregate)]
/// struct Account(Aggregate<AccountState, AccountEventStore>);
/// ```
pub struct Aggregate<Proj, Store>
where
    Proj: Projection,
    Store: EventStore<Proj> + ?Sized,
{
    query: Proj::Query,
    state: Option<Proj>, // Safe to unwrap everywhere - will only be None if Proj::apply() panics
    pending_events: DropEmptyVec<Proj::Event>,
    last_stored_event_id: Option<EventID>,
    _store: PhantomData<Store>,
}

impl<Proj, Store> Aggregate<Proj, Store>
where
    Proj: Projection,
    Store: EventStore<Proj> + ?Sized,
{
    pub fn new(
        query: Proj::Query,
        genesis_event: impl Into<Proj::Event>,
    ) -> Result<Self, ProjectionError<Proj>> {
        let genesis_event = genesis_event.into();
        Ok(Self {
            query,
            state: Some(Proj::apply(None, genesis_event.clone())?),
            pending_events: vec![genesis_event].into(),
            last_stored_event_id: None,
            _store: PhantomData,
        })
    }

    /// Initializes the aggregate from first event in the stream
    pub fn from_stored_event(
        query: Proj::Query,
        event_id: EventID,
        event: Proj::Event,
    ) -> Result<Self, ProjectionError<Proj>> {
        Ok(Self {
            query,
            state: Some(Proj::apply(None, event)?),
            pending_events: DropEmptyVec::new(),
            last_stored_event_id: Some(event_id),
            _store: PhantomData,
        })
    }

    /// Initializes an aggregate from a state snapshot
    pub fn from_stored_snapshot(query: Proj::Query, event_id: EventID, state: Proj) -> Self {
        Self {
            query,
            state: Some(state),
            pending_events: DropEmptyVec::new(),
            last_stored_event_id: Some(event_id),
            _store: PhantomData,
        }
    }

    /// Checks whether an aggregate has pending updates that need to be saved
    pub fn has_updates(&self) -> bool {
        !self.pending_events.is_empty()
    }

    /// Returns the last event ID in an event store to which this aggregate was
    /// synchronized
    pub fn last_stored_event_id(&self) -> Option<EventID> {
        self.last_stored_event_id
    }

    /// Initializes an aggregate from event history
    #[inline]
    pub async fn load<Q>(query: Q, event_store: &Store) -> Result<Self, LoadError<Proj>>
    where
        Q: std::borrow::Borrow<Proj::Query> + std::fmt::Debug,
    {
        Self::load_ext(query, event_store, LoadOpts::default()).await
    }

    /// Attempt initializing an aggregate from event history, but allow the not
    /// found case
    #[inline]
    pub async fn try_load<Q>(
        query: Q,
        event_store: &Store,
    ) -> Result<Option<Self>, TryLoadError<Proj>>
    where
        Q: std::borrow::Borrow<Proj::Query> + std::fmt::Debug,
    {
        match Self::load_ext(query, event_store, LoadOpts::default()).await {
            Ok(a) => Ok(Some(a)),
            Err(e) => match e {
                LoadError::NotFound(_) => Ok(None),
                LoadError::Internal(e) => Err(TryLoadError::Internal(e)),
                LoadError::ProjectionError(e) => Err(TryLoadError::ProjectionError(e)),
            },
        }
    }

    /// Loads multiple aggregations
    ///
    /// Returns either collection of aggregation results or an error, when
    /// failed to read from source stream.
    ///
    /// "Ok" vector contains results for every item from `queries` argument.
    /// Order is preserved.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            agg_type = %std::any::type_name::<Proj>(),
            agg_queries_cnt = ?queries.len(),
        )
    )]
    pub async fn load_multi(
        queries: &[Proj::Query],
        event_store: &Store,
    ) -> Result<Vec<Result<Self, LoadError<Proj>>>, GetEventsError> {
        use tokio_stream::StreamExt;

        let mut event_stream = event_store.get_events_multi(queries);
        let mut agg_results: HashMap<Proj::Query, Result<Self, LoadError<Proj>>> = HashMap::new();

        while let Some(res) = event_stream.next().await {
            // When failed to read at least one event from source stream,
            // function returns error result immediately
            let (query, event_id, event) = res?;
            if let Some(agg_result) = agg_results.get_mut(&query)
                && let Ok(agg) = agg_result
            {
                if let Err(err) = agg.apply_stored(event_id, event) {
                    *agg_result = Err(err.into());
                }
            } else {
                let agg_result =
                    Self::from_stored_event(query.clone(), event_id, event).map_err(Into::into);
                agg_results.insert(query, agg_result);
            }
        }

        let mut result: Vec<Result<Self, LoadError<Proj>>> = vec![];
        for query in queries {
            let item = match agg_results.remove(query) {
                None => Err(AggregateNotFoundError::new(query.clone()).into()),
                Some(agg) => agg,
            };

            match &item {
                Ok(agg) => {
                    tracing::debug!(
                        last_stored_event_id = %agg.last_stored_event_id.unwrap(),
                        "Loaded aggregate",
                    );
                }
                Err(err) => {
                    tracing::error!(error = ?err, error_msg = %err, "Failed to load aggregate",);
                }
            }

            result.push(item);
        }
        Ok(result)
    }

    /// Same as `load_multi()` but returns a vector of loaded states and single
    /// error
    pub async fn load_multi_simple(
        queries: &[Proj::Query],
        event_store: &Store,
    ) -> Result<Vec<Self>, InternalError> {
        Self::load_multi(queries, event_store)
            .await
            .int_err()?
            .into_iter()
            .map(|res| res.map_err(ErrorIntoInternal::int_err))
            .collect()
    }

    /// Same as [`Aggregate::load()`] but with extra control knobs
    #[tracing::instrument(
        level = "debug",
        name = "load",
        skip_all,
        fields(
            agg_type = %std::any::type_name::<Proj>(),
            agg_query = ?query_ref,
        )
    )]
    pub async fn load_ext<Q>(
        query_ref: Q,
        event_store: &Store,
        opts: LoadOpts,
    ) -> Result<Self, LoadError<Proj>>
    where
        Q: std::borrow::Borrow<Proj::Query> + std::fmt::Debug,
    {
        use tokio_stream::StreamExt;

        let query = query_ref.borrow();

        let mut event_stream = event_store.get_events(
            query,
            GetEventsOpts {
                from: None,
                to: opts.as_of_event,
            },
        );

        let (event_id, event) = match event_stream.next().await {
            Some(Ok(v)) => v,
            Some(Err(GetEventsError::Internal(err))) => return Err(err.into()),
            None => return Err(AggregateNotFoundError::new(query.clone()).into()),
        };

        let mut agg = Self::from_stored_event(query.clone(), event_id, event)?;

        let mut num_events = 1;
        while let Some(res) = event_stream.next().await {
            let (event_id, event) = res?;
            agg.apply_stored(event_id, event)?;
            num_events += 1;
        }

        tracing::debug!(
            num_events,
            last_stored_event_id = %agg.last_stored_event_id.unwrap(),
            "Loaded aggregate",
        );

        Ok(agg)
    }

    /// Updates the state of an aggregate with events that happened since the
    /// last load.
    ///
    /// Will panic if the aggregate has pending updates
    #[inline]
    pub async fn update(&mut self, event_store: &Store) -> Result<(), UpdateError<Proj>> {
        self.update_ext(event_store, LoadOpts::default()).await
    }

    /// Same as [`Aggregate::update()`] but with extra control knobs
    #[tracing::instrument(
        level = "debug",
        name = "update",
        skip_all,
        fields(
            agg_type = %std::any::type_name::<Proj>(),
            agg_query = ?self.query,
        )
    )]
    pub async fn update_ext(
        &mut self,
        event_store: &Store,
        opts: LoadOpts,
    ) -> Result<(), UpdateError<Proj>> {
        use tokio_stream::StreamExt;

        assert!(!self.has_updates());

        let prev_stored_event_id = self.last_stored_event_id;

        let mut event_stream = event_store.get_events(
            &self.query,
            GetEventsOpts {
                from: prev_stored_event_id,
                to: opts.as_of_event,
            },
        );

        let mut num_events = 1;

        while let Some(res) = event_stream.next().await {
            let (event_id, event) = res?;
            self.apply_stored(event_id, event)?;
            num_events += 1;
        }

        tracing::debug!(
            num_events,
            prev_stored_event_id = %prev_stored_event_id.unwrap(),
            last_stored_event_id = %self.last_stored_event_id.unwrap(),
            "Updated aggregate",
        );

        Ok(())
    }

    /// Persists pending aggregate events
    #[tracing::instrument(
        level = "debug",
        name = "save",
        skip_all, fields(
            agg_type = %std::any::type_name::<Proj>(),
            agg_query = ?self.query,
        )
    )]
    pub async fn save(&mut self, event_store: &Store) -> Result<(), SaveError> {
        // Extra check to avoid taking a vec with an allocated buffer
        if self.pending_events.is_empty() {
            return Ok(());
        }

        let events = self.pending_events.take_inner();

        if !events.is_empty() {
            let num_events = events.len();
            let prev_stored_event_id = self.last_stored_event_id;

            let last_stored_event_id = event_store
                .save_events(&self.query, prev_stored_event_id, events)
                .await?;
            self.last_stored_event_id = Some(last_stored_event_id);

            tracing::debug!(
                num_events,
                ?prev_stored_event_id,
                %last_stored_event_id,
                "Saved aggregate",
            );
        }

        Ok(())
    }

    /// Updates the state projection and adds the event to pending updates list
    pub fn apply(&mut self, event: Proj::Event) -> Result<(), ProjectionError<Proj>> {
        match Proj::apply(self.state.take(), event.clone()) {
            Ok(state) => {
                self.state = Some(state);
                self.pending_events.push(event);
                Ok(())
            }
            Err(err) => {
                // Restore the state pre error
                self.state.clone_from(&err.inner.state);
                Err(err)
            }
        }
    }

    /// Apply the event already stored in event store
    fn apply_stored(
        &mut self,
        event_id: EventID,
        event: Proj::Event,
    ) -> Result<(), ProjectionError<Proj>> {
        if let Some(last_stored_event_id) = self.last_stored_event_id {
            assert!(
                last_stored_event_id < event_id,
                "Attempting to mutate with event {event_id} while state is already synced to \
                 {last_stored_event_id}",
            );
        }

        match Proj::apply(self.state.take(), event) {
            Ok(state) => {
                self.state = Some(state);
                Ok(())
            }
            Err(err) => {
                // Restore the state pre error
                self.state.clone_from(&err.inner.state);
                Err(err)
            }
        }?;

        self.last_stored_event_id = Some(event_id);
        Ok(())
    }

    pub fn as_state(&self) -> &Proj {
        self.state.as_ref().unwrap()
    }

    pub fn into_state(self) -> Proj {
        self.state.unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<Proj, Store> std::fmt::Debug for Aggregate<Proj, Store>
where
    Proj: Projection,
    Store: EventStore<Proj> + ?Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aggregate")
            .field("query", &self.query)
            .field("state", self.state.as_ref().unwrap())
            .field("pending_events", &self.pending_events)
            .field("last_stored_event_id", &self.last_stored_event_id)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct LoadOpts {
    /// Only considers a subset of events (inclusive upper bound)
    pub as_of_event: Option<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum LoadError<Proj: Projection> {
    #[error(transparent)]
    NotFound(#[from] AggregateNotFoundError<Proj::Query>),
    #[error(transparent)]
    ProjectionError(ProjectionError<Proj>),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum TryLoadError<Proj: Projection> {
    #[error(transparent)]
    ProjectionError(ProjectionError<Proj>),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl<Proj: Projection> From<GetEventsError> for LoadError<Proj> {
    fn from(value: GetEventsError) -> Self {
        match value {
            GetEventsError::Internal(err) => Self::Internal(err),
        }
    }
}

impl<Proj: Projection> From<UpdateError<Proj>> for LoadError<Proj> {
    fn from(value: UpdateError<Proj>) -> Self {
        match value {
            UpdateError::ProjectionError(err) => Self::ProjectionError(err),
            UpdateError::Internal(err) => Self::Internal(err),
        }
    }
}

// Transitive From
impl<Proj: Projection, Err: Into<ProjectionError<Proj>>> From<Err> for LoadError<Proj> {
    fn from(value: Err) -> Self {
        Self::ProjectionError(value.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum UpdateError<Proj: Projection> {
    #[error(transparent)]
    ProjectionError(ProjectionError<Proj>),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl<Proj: Projection> From<GetEventsError> for UpdateError<Proj> {
    fn from(value: GetEventsError) -> Self {
        match value {
            GetEventsError::Internal(err) => Self::Internal(err),
        }
    }
}

// Transitive From
impl<Proj: Projection, Err: Into<ProjectionError<Proj>>> From<Err> for UpdateError<Proj> {
    fn from(value: Err) -> Self {
        Self::ProjectionError(value.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SaveError {
    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<SaveEventsError> for SaveError {
    fn from(value: SaveEventsError) -> Self {
        match value {
            e @ SaveEventsError::NothingToSave => SaveError::Internal(e.int_err()),
            SaveEventsError::ConcurrentModification(err) => Self::ConcurrentModification(err),
            SaveEventsError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Aggregate {query:?} not found")]
pub struct AggregateNotFoundError<Query> {
    pub query: Query,
}

impl<Query> AggregateNotFoundError<Query> {
    pub fn new(query: Query) -> Self {
        Self { query }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DropEmptyVec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct DropEmptyVec<T: std::fmt::Debug>(Vec<T>);

impl<T: std::fmt::Debug> DropEmptyVec<T> {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn take_inner(&mut self) -> Vec<T> {
        std::mem::take(&mut self.0)
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for DropEmptyVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: std::fmt::Debug> From<Vec<T>> for DropEmptyVec<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T: std::fmt::Debug> std::ops::Deref for DropEmptyVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: std::fmt::Debug> std::ops::DerefMut for DropEmptyVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Drop for DropEmptyVec<T>
where
    T: std::fmt::Debug,
{
    fn drop(&mut self) {
        if !self.0.is_empty() {
            tracing::error!(
                pending_events = ?self.0,
                "Aggregate is dropped with unsaved events",
            );
        }
    }
}
