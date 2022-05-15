// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::metadata_chain::IterBlocksError;
use opendatafabric::*;

use futures::{future, Future, Stream, TryStreamExt};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/////////////////////////////////////////////////////////////////////////////////////////

pub type DynMetadataStream<'a> = Pin<Box<dyn MetadataStream<'a> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

/// Stream combinators specific to metadata chain.
///
/// These combinators can be implemented differently by various metadata chains
/// to make certain operations more efficient, for example:
/// - Filters can use the raw block representations (e.g. flatbuffers)
///   to skip undesired blocks without constructing DTOs
/// - Implementations can use skip lists and lookup tables to traverse the chain faster.
pub trait MetadataStream<'a>:
    Stream<Item = Result<(Multihash, MetadataBlock), IterBlocksError>>
{
    fn filter_data_stream_blocks(
        self: Pin<Box<Self>>,
    ) -> Pin<
        Box<dyn Stream<Item = Result<(Multihash, MetadataBlockDataStream), IterBlocksError>> + 'a>,
    >;
}

// TODO: This implementation should be moved to `infra` part of the crate
impl<'a, T> MetadataStream<'a> for T
where
    T: Stream<Item = Result<(Multihash, MetadataBlock), IterBlocksError>>,
    T: 'a,
{
    fn filter_data_stream_blocks(
        self: Pin<Box<Self>>,
    ) -> Pin<
        Box<dyn Stream<Item = Result<(Multihash, MetadataBlockDataStream), IterBlocksError>> + 'a>,
    > {
        Box::pin(
            self.try_filter_map(|(h, b)| {
                future::ready(Ok(b.into_data_stream_block().map(|b| (h, b))))
            }),
        )
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Generic Stream extensions
/////////////////////////////////////////////////////////////////////////////////////////

pub trait TryStreamExtExt: Stream
where
    Self: Sized,
{
    fn filter_map_ok<F, T1, T2, E>(self, f: F) -> FilterMapOk<Self, F>
    where
        Self: Stream<Item = Result<T1, E>>,
        F: FnMut(T1) -> Option<T2>,
    {
        FilterMapOk::new(self, f)
    }

    fn try_first(self) -> TryFirst<Self> {
        TryFirst::new(self)
    }
}

impl<S, T, E> TryStreamExtExt for S where S: Stream<Item = Result<T, E>> {}

/////////////////////////////////////////////////////////////////////////////////////////
// Combinators
/////////////////////////////////////////////////////////////////////////////////////////

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct FilterMapOk<S, F> {
    #[pin]
    inner: S,
    f: F,
}

impl<S, F, T1, T2, E> FilterMapOk<S, F>
where
    S: Stream<Item = Result<T1, E>>,
    F: FnMut(T1) -> Option<T2>,
{
    fn new(inner: S, f: F) -> Self {
        Self { inner, f }
    }
}

impl<S, F, T1, T2, E> Stream for FilterMapOk<S, F>
where
    S: Stream<Item = Result<T1, E>>,
    F: FnMut(T1) -> Option<T2>,
{
    type Item = Result<T2, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(v))) => {
                if let Some(v2) = (this.f)(v) {
                    Poll::Ready(Some(Ok(v2)))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct TryFirst<S> {
    #[pin]
    inner: S,
}

impl<S> TryFirst<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, T, E> Future for TryFirst<S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Output = Result<Option<T>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.project().inner;
        match inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Ok(None)),
            Poll::Ready(Some(Ok(v))) => Poll::Ready(Ok(Some(v))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
        }
    }
}
