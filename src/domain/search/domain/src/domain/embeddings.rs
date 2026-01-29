// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModelKey {
    pub provider: &'static str,
    pub name: String,
    pub revision: Option<&'static str>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModelRow {
    pub id: i64,
    pub key: EmbeddingModelKey,
    pub dims: usize,
    pub created_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmbeddingCacheKey {
    pub model_id: i64,
    pub input_hash: [u8; 32], // sha256
    pub input_text: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Stored as packed f32 in LITTLE-ENDIAN order.
/// (You can wrap with a newtype if you want.)
pub type EmbeddingBytes = Vec<u8>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EmbeddingCacheRow {
    pub key: EmbeddingCacheKey,
    pub embedding: EmbeddingBytes,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub hit_count: i64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
