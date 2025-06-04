// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Future, Stream, TryStreamExt, future};
use odf_metadata::*;
use pin_project::pin_project;

use super::metadata_chain::IterBlocksError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type DynMetadataStream<'a> = Pin<Box<dyn MetadataStream<'a> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type HashedMetadataBlock = (Multihash, MetadataBlock);
pub type HashedMetadataBlockRef<'a> = (&'a Multihash, &'a MetadataBlock);

type MetadataStreamItem = Result<HashedMetadataBlock, IterBlocksError>;
type FilteredDataStreamBlocksStreamItem =
    Result<(Multihash, MetadataBlockDataStream), IterBlocksError>;
type FilteredDataStreamBlocksStream<'a> =
    Pin<Box<dyn Stream<Item = FilteredDataStreamBlocksStreamItem> + Send + 'a>>;

/// Stream combinators specific to metadata chain.
///
/// These combinators can be implemented differently by various metadata chains
/// to make certain operations more efficient, for example:
/// - Filters can use the raw block representations (e.g. flatbuffers) to skip
///   undesired blocks without constructing DTOs
/// - Implementations can use skip lists and lookup tables to traverse the chain
///   faster.
pub trait MetadataStream<'a>: Stream<Item = MetadataStreamItem> {
    // TODO: Reconsider this method as it may result in incorrect logic of
    // checkpoint propagation. In cases when AddData is followed by SetWatermark
    // the client may incorrectly assume that the checkpoint is missing when
    // inspecting SetWatermark event, while in fact they should've scanned the
    // chain further.
    fn filter_data_stream_blocks(self: Pin<Box<Self>>) -> FilteredDataStreamBlocksStream<'a>;
}

// TODO: This implementation should be moved to `infra` part of the crate
impl<'a, T> MetadataStream<'a> for T
where
    T: Stream<Item = MetadataStreamItem>,
    T: Send + 'a,
{
    fn filter_data_stream_blocks(self: Pin<Box<Self>>) -> FilteredDataStreamBlocksStream<'a> {
        Box::pin(
            self.try_filter_map(|(h, b)| {
                future::ready(Ok(b.into_data_stream_block().map(|b| (h, b))))
            }),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Generic Stream extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait TryStreamExtExt: Stream
where
    Self: Sized,
{
    fn filter_ok<F, T, E>(self, f: F) -> FilterOk<Self, F>
    where
        Self: Stream<Item = Result<T, E>>,
        F: FnMut(&T) -> bool,
    {
        FilterOk::new(self, f)
    }

    fn filter_map_ok<F, T1, T2, E>(self, f: F) -> FilterMapOk<Self, F>
    where
        Self: Stream<Item = Result<T1, E>>,
        F: FnMut(T1) -> Option<T2>,
    {
        FilterMapOk::new(self, f)
    }

    fn take_while_ok<F, T, E>(self, f: F) -> TakeWhileOk<Self, F>
    where
        Self: Stream<Item = Result<T, E>>,
        F: FnMut(&T) -> bool,
    {
        TakeWhileOk::new(self, f)
    }

    fn flatten_ok<T, IT, I, E>(self) -> FlattenOk<Self, IT>
    where
        Self: Stream<Item = Result<T, E>>,
        T: IntoIterator<Item = I, IntoIter = IT>,
        IT: Iterator<Item = I>,
    {
        FlattenOk::new(self)
    }

    fn any_ok<F, T, E>(self, predicate: F) -> AnyOk<Self, F>
    where
        Self: Stream<Item = Result<T, E>>,
        F: FnMut(&T) -> bool,
    {
        AnyOk::new(self, predicate)
    }

    fn try_first(self) -> TryFirst<Self> {
        TryFirst::new(self)
    }

    fn try_last<T, E>(self) -> TryLast<Self, T>
    where
        Self: Stream<Item = Result<T, E>>,
    {
        TryLast::new(self)
    }

    fn try_count(self) -> TryCount<Self> {
        TryCount::new(self)
    }
}

impl<S, T, E> TryStreamExtExt for S where S: Stream<Item = Result<T, E>> {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Combinators
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct FilterOk<S, F> {
    #[pin]
    inner: S,
    f: F,
}

impl<S, F, T, E> FilterOk<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    fn new(inner: S, f: F) -> Self {
        Self { inner, f }
    }
}

impl<S, F, T, E> Stream for FilterOk<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(None) => break Poll::Ready(None),
                Poll::Ready(Some(Ok(v))) => {
                    if (this.f)(&v) {
                        break Poll::Ready(Some(Ok(v)));
                    }
                }
                Poll::Ready(Some(Err(e))) => break Poll::Ready(Some(Err(e))),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        let mut this = self.project();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(None) => break Poll::Ready(None),
                Poll::Ready(Some(Ok(v))) => {
                    if let Some(v2) = (this.f)(v) {
                        break Poll::Ready(Some(Ok(v2)));
                    }
                }
                Poll::Ready(Some(Err(e))) => break Poll::Ready(Some(Err(e))),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct TakeWhileOk<S, F> {
    #[pin]
    inner: S,
    f: F,
    done_taking: bool,
}

impl<S, F, T, E> TakeWhileOk<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    fn new(inner: S, f: F) -> Self {
        Self {
            inner,
            f,
            done_taking: false,
        }
    }
}

impl<S, F, T, E> Stream for TakeWhileOk<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done_taking {
            return Poll::Ready(None);
        }
        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(v))) => {
                if (this.f)(&v) {
                    Poll::Ready(Some(Ok(v)))
                } else {
                    *this.done_taking = true;
                    Poll::Ready(None)
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct FlattenOk<S, T> {
    #[pin]
    inner: S,
    iter: Option<T>,
}

impl<S, T, IT, I, E> FlattenOk<S, IT>
where
    S: Stream<Item = Result<T, E>>,
    T: IntoIterator<Item = I, IntoIter = IT>,
    IT: Iterator<Item = I>,
{
    fn new(inner: S) -> Self {
        Self { inner, iter: None }
    }
}

impl<S, T, IT, I, E> Stream for FlattenOk<S, IT>
where
    S: Stream<Item = Result<T, E>>,
    T: IntoIterator<Item = I, IntoIter = IT>,
    IT: Iterator<Item = I>,
{
    type Item = Result<I, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(iter) = this.iter.as_mut() {
                if let Some(next) = iter.next() {
                    break Poll::Ready(Some(Ok(next)));
                }

                *this.iter = None;
            }
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(None) => break Poll::Ready(None),
                Poll::Ready(Some(Ok(v))) => {
                    *this.iter = Some(v.into_iter());
                }
                Poll::Ready(Some(Err(e))) => break Poll::Ready(Some(Err(e))),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct AnyOk<S, F> {
    #[pin]
    inner: S,
    f: F,
    accum: Option<bool>,
}

impl<S, F, T, E> AnyOk<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    fn new(inner: S, f: F) -> Self {
        Self {
            inner,
            f,
            accum: Some(false),
        }
    }
}

impl<S, F, T, E> Future for AnyOk<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Output = Result<bool, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(None) => {
                    if let Some(v) = this.accum.take() {
                        break Poll::Ready(Ok(v));
                    }

                    panic!("Any polled after completion")
                }
                Poll::Ready(Some(Ok(v))) => {
                    let acc = (this.f)(&v) || this.accum.unwrap();
                    *this.accum = Some(acc);
                }
                Poll::Ready(Some(Err(e))) => break Poll::Ready(Err(e)),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct TryLast<S, T> {
    #[pin]
    inner: S,
    last_seen: Option<T>,
}

impl<S, T, E> TryLast<S, T>
where
    S: Stream<Item = Result<T, E>>,
{
    fn new(inner: S) -> Self {
        Self {
            inner,
            last_seen: None,
        }
    }
}

impl<S, T, E> Future for TryLast<S, T>
where
    S: Stream<Item = Result<T, E>>,
{
    type Output = Result<Option<T>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(None) => break Poll::Ready(Ok(this.last_seen.take())),
                Poll::Ready(Some(Ok(v))) => *this.last_seen = Some(v),
                Poll::Ready(Some(Err(e))) => break Poll::Ready(Err(e)),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct TryCount<S> {
    #[pin]
    inner: S,
    accum: Option<usize>,
}

impl<S> TryCount<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            accum: Some(0),
        }
    }
}

impl<S, T, E> Future for TryCount<S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Output = Result<usize, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(None) => {
                    if let Some(v) = this.accum.take() {
                        break Poll::Ready(Ok(v));
                    }

                    panic!("Any polled after completion")
                }
                Poll::Ready(Some(Ok(_))) => {
                    *this.accum = Some(this.accum.unwrap() + 1);
                }
                Poll::Ready(Some(Err(e))) => break Poll::Ready(Err(e)),
            }
        }
    }
}
