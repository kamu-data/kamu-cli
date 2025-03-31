// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Range;

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A simple wrapper on top of [`ObjectStore`] to add tracing
pub(crate) struct ObjectStoreWithTracing<S>(S);

impl<S> ObjectStoreWithTracing<S> {
    pub fn new(inner: S) -> Self {
        Self(inner)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl<S> ObjectStore for ObjectStoreWithTracing<S>
where
    S: ObjectStore,
{
    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::put",
        skip_all,
        fields(%location, kind = std::any::type_name::<S>()),
    )]
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.0.put(location, payload).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::put",
        skip_all,
        fields(%location, kind = std::any::type_name::<S>()),
    )]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::put_multipart",
        skip_all,
        fields(%location, kind = std::any::type_name::<S>()),
    )]
    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart(location).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::put_multipart",
        skip_all,
        fields(%location, kind = std::any::type_name::<S>()),
    )]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::get",
        skip_all,
        fields(%location, kind = std::any::type_name::<S>()),
    )]
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.0.get(location).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::get",
        skip_all,
        fields(%location, kind = std::any::type_name::<S>()),
    )]
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::get_range",
        skip_all,
        fields(%location, ?range, kind = std::any::type_name::<S>()),
    )]
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.0.get_range(location, range).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::get_rages",
        skip_all,
        fields(%location, ?ranges, kind = std::any::type_name::<S>()),
    )]
    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.0.get_ranges(location, ranges).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::head",
        skip_all,
        fields(%location, kind = std::any::type_name::<S>()),
    )]
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.0.head(location).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::delete",
        skip_all,
        fields(%location, kind = std::any::type_name::<S>()),
    )]
    async fn delete(&self, location: &Path) -> Result<()> {
        self.0.delete(location).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::delete_stream",
        skip_all,
        fields(kind = std::any::type_name::<S>()),
    )]
    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        self.0.delete_stream(locations)
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::list",
        skip_all,
        fields(?prefix, kind = std::any::type_name::<S>()),
    )]
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::list_with_offset",
        skip_all,
        fields(?prefix, %offset, kind = std::any::type_name::<S>()),
    )]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        self.0.list_with_offset(prefix, offset)
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::list_with_delimiter",
        skip_all,
        fields(?prefix, kind = std::any::type_name::<S>()),
    )]
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::copy",
        skip_all,
        fields(%from, %to, kind = std::any::type_name::<S>()),
    )]
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.0.copy(from, to).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::rename",
        skip_all,
        fields(%from, %to, kind = std::any::type_name::<S>()),
    )]
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.0.rename(from, to).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::copy_if_not_exists",
        skip_all,
        fields(%from, %to, kind = std::any::type_name::<S>()),
    )]
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.0.copy_if_not_exists(from, to).await
    }

    #[tracing::instrument(
        level = "debug",
        name = "ObjectStore::rename_if_not_exists",
        skip_all,
        fields(%from, %to, kind = std::any::type_name::<S>()),
    )]
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.0.rename_if_not_exists(from, to).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<S> std::fmt::Debug for ObjectStoreWithTracing<S>
where
    S: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ObjectStoreWithTracing")
            .field(&self.0)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<S> std::fmt::Display for ObjectStoreWithTracing<S>
where
    S: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStoreWithTracing({})", self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
