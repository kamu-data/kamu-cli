// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SearchService: Send + Sync {
    async fn health(&self, ctx: SearchContext<'_>) -> Result<serde_json::Value, InternalError>;

    async fn listing_search(
        &self,
        ctx: SearchContext<'_>,
        req: ListingSearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn text_search(
        &self,
        ctx: SearchContext<'_>,
        req: TextSearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        req: VectorSearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn hybrid_search(
        &self,
        ctx: SearchContext<'_>,
        req: HybridSearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn find_document_by_id(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError>;

    async fn bulk_update(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
pub struct SearchContext<'a> {
    pub catalog: &'a dill::Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum SearchIndexUpdateOperation {
    Index {
        id: SearchEntityId,
        doc: serde_json::Value,
    },
    Update {
        id: SearchEntityId,
        doc: serde_json::Value,
    },
    Delete {
        id: SearchEntityId,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents the state of a field extraction in incremental indexing
pub enum SearchFieldUpdate<T> {
    /// Field was not present in the interval (no corresponding event)
    Absent,
    /// Field was present but empty (event exists, but data cleared)
    Cleared,
    /// Field was present with data
    Present(T),
}

impl<T> SearchFieldUpdate<T> {
    pub fn is_present(&self) -> bool {
        matches!(self, SearchFieldUpdate::Present(_))
    }

    pub fn is_absent(&self) -> bool {
        matches!(self, SearchFieldUpdate::Absent)
    }

    /// Use the new value if present/empty, otherwise fetch and reuse existing
    /// value. The callback is only invoked if this field is Absent.
    pub fn or_existing<F>(self, get_existing: F) -> SearchFieldUpdate<T>
    where
        F: FnOnce() -> Option<T>,
    {
        match self {
            SearchFieldUpdate::Absent => match get_existing() {
                Some(v) => SearchFieldUpdate::Present(v),
                None => SearchFieldUpdate::Absent,
            },
            SearchFieldUpdate::Cleared => SearchFieldUpdate::Cleared,
            SearchFieldUpdate::Present(v) => SearchFieldUpdate::Present(v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Helper to conditionally insert field based on update state
pub fn insert_search_incremental_update_field<T: serde::Serialize>(
    incremental_update: &mut serde_json::Map<String, serde_json::Value>,
    field_path: SearchFieldPath,
    field_update: SearchFieldUpdate<T>,
) {
    match field_update {
        SearchFieldUpdate::Absent => {}
        SearchFieldUpdate::Cleared => {
            incremental_update.insert(field_path.to_string(), serde_json::json!(null));
        }
        SearchFieldUpdate::Present(v) => {
            incremental_update.insert(field_path.to_string(), serde_json::json!(v));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Semantic embedings computation helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_semantic_embeddings_document(
    embeddings_chunker: &dyn EmbeddingsChunker,
    embeddings_encoder: &dyn EmbeddingsEncoder,
    fields: &[&dyn TextFieldEmbeddingsContributor],
) -> Result<SearchFieldUpdate<Vec<serde_json::Value>>, InternalError> {
    // If all fields have been cleared, we need to clear embeddings as well
    let anything_present = fields.iter().any(|field| field.is_present());
    let anything_cleared = fields.iter().any(|field| field.is_cleared());
    if !anything_present {
        if anything_cleared {
            return Ok(SearchFieldUpdate::Cleared);
        }
        return Ok(SearchFieldUpdate::Absent);
    }

    // Aggregate all contributing text parts
    let mut texts = Vec::new();
    for field in fields {
        field.contribute_texts(&mut texts);
    }
    if texts.is_empty() {
        return Ok(SearchFieldUpdate::Absent);
    }

    // Split parts into chunks
    let chunks = embeddings_chunker.chunk(texts).await?;
    if chunks.is_empty() {
        return Ok(SearchFieldUpdate::Absent);
    }

    // Encode chunks into embedding vectors
    let vectors = embeddings_encoder.encode(chunks).await?;
    if vectors.is_empty() {
        return Ok(SearchFieldUpdate::Absent);
    }

    #[derive(serde::Serialize)]
    struct ChunkDoc<'a> {
        chunk_id: String,
        embedding: &'a [f32], // flat per chunk
    }

    // Prepare final document
    let chunk_docs = vectors
        .iter()
        .enumerate()
        .map(|(i, vector)| ChunkDoc {
            chunk_id: format!("chunk_{i}"),
            embedding: vector,
        })
        .map(|doc| serde_json::to_value(doc).unwrap())
        .collect::<Vec<_>>();

    Ok(SearchFieldUpdate::Present(chunk_docs))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Helper trait for text contribution to semantic embeddings
pub trait TextFieldEmbeddingsContributor: Send + Sync {
    fn is_present(&self) -> bool;
    fn is_cleared(&self) -> bool;
    fn contribute_texts(&self, texts: &mut Vec<String>);
}

impl TextFieldEmbeddingsContributor for SearchFieldUpdate<String> {
    fn is_present(&self) -> bool {
        matches!(self, SearchFieldUpdate::Present(_))
    }

    fn is_cleared(&self) -> bool {
        matches!(self, SearchFieldUpdate::Cleared)
    }

    fn contribute_texts(&self, texts: &mut Vec<String>) {
        if let SearchFieldUpdate::Present(s) = self {
            texts.push(s.clone());
        }
    }
}

impl TextFieldEmbeddingsContributor for SearchFieldUpdate<Vec<String>> {
    fn is_present(&self) -> bool {
        matches!(self, SearchFieldUpdate::Present(_))
    }

    fn is_cleared(&self) -> bool {
        matches!(self, SearchFieldUpdate::Cleared)
    }

    fn contribute_texts(&self, texts: &mut Vec<String>) {
        if let SearchFieldUpdate::Present(vec) = self {
            texts.extend_from_slice(vec);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
