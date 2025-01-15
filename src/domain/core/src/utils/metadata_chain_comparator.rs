// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use internal_error::*;
use opendatafabric::{MetadataBlock, Multihash};
use thiserror::Error;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainComparator {}

// TODO: This comparator explores the chains eagerly and may not be optimal for
// really long chains. We should explore alternatives such as:
// - making comparator streaming
// - adding `MetadataChain::nth_block(head, sequence_number)` function that can
//   skip through long chains faster
impl MetadataChainComparator {
    pub async fn compare_chains(
        lhs_chain: &dyn MetadataChain,
        lhs_head: &Multihash,
        rhs_chain: &dyn MetadataChain,
        rhs_head: Option<&Multihash>,
        listener: &dyn CompareChainsListener,
    ) -> Result<CompareChainsResult, CompareChainsError> {
        // When source and destination point to the same block, chains are equal, no
        // further scanning required
        if Some(&lhs_head) == rhs_head.as_ref() {
            return Ok(CompareChainsResult::Equal);
        }

        let lhs_chain = MetadataChainWithStats::new(
            lhs_chain,
            |n| {
                listener.on_lhs_expected_reads(n);
            },
            |n| {
                listener.on_lhs_read(n);
            },
        );
        let rhs_chain = MetadataChainWithStats::new(
            rhs_chain,
            |n| {
                listener.on_rhs_expected_reads(n);
            },
            |n| {
                listener.on_rhs_read(n);
            },
        );

        // Extract sequence numbers of head blocks
        lhs_chain.expecting_to_read_blocks(1);
        let lhs_sequence_number = lhs_chain.get_block(lhs_head).await?.sequence_number;

        let Some(rhs_head) = rhs_head else {
            // LHS chain is unconditionally ahead - simply return all of its blocks
            lhs_chain.expecting_to_read_blocks(lhs_sequence_number + 1);
            return Ok(CompareChainsResult::LhsAhead {
                lhs_ahead_blocks: lhs_chain
                    .iter_blocks_interval(lhs_head, None, false)
                    .try_collect()
                    .await?,
            });
        };

        rhs_chain.expecting_to_read_blocks(1);
        let rhs_sequence_number = rhs_chain.get_block(rhs_head).await?.sequence_number;

        use std::cmp::Ordering;

        match lhs_sequence_number.cmp(&rhs_sequence_number) {
            // If numbers are equal, it's a guaranteed divergence, as we've checked blocks
            // for equality above
            Ordering::Equal => {
                let last_common_sequence_number = Self::find_common_ancestor_sequence_number(
                    &lhs_chain,
                    lhs_head,
                    lhs_sequence_number,
                    &rhs_chain,
                    rhs_head,
                    rhs_sequence_number,
                )
                .await?;

                Ok(Self::describe_divergence(
                    lhs_sequence_number,
                    rhs_sequence_number,
                    last_common_sequence_number,
                ))
            }
            // Source ahead
            Ordering::Greater => {
                let convergence_check = Self::check_expected_common_ancestor(
                    &lhs_chain,
                    lhs_sequence_number,
                    lhs_head,
                    &rhs_chain,
                    rhs_sequence_number,
                    rhs_head,
                )
                .await?;

                match convergence_check {
                    CommonAncestorCheck::Success { ahead_blocks } => {
                        Ok(CompareChainsResult::LhsAhead {
                            lhs_ahead_blocks: ahead_blocks,
                        })
                    }
                    CommonAncestorCheck::Failure {
                        common_ancestor_sequence_number: last_common_sequence_number,
                    } => Ok(Self::describe_divergence(
                        lhs_sequence_number,
                        rhs_sequence_number,
                        last_common_sequence_number,
                    )),
                }
            }
            // Destination ahead
            Ordering::Less => {
                let convergence_check = Self::check_expected_common_ancestor(
                    &rhs_chain,
                    rhs_sequence_number,
                    rhs_head,
                    &lhs_chain,
                    lhs_sequence_number,
                    lhs_head,
                )
                .await?;

                match convergence_check {
                    CommonAncestorCheck::Success { ahead_blocks } => {
                        Ok(CompareChainsResult::LhsBehind {
                            rhs_ahead_blocks: ahead_blocks,
                        })
                    }
                    CommonAncestorCheck::Failure {
                        common_ancestor_sequence_number: last_common_sequence_number,
                    } => Ok(Self::describe_divergence(
                        lhs_sequence_number,
                        rhs_sequence_number,
                        last_common_sequence_number,
                    )),
                }
            }
        }
    }

    async fn check_expected_common_ancestor(
        ahead_chain: &MetadataChainWithStats<'_>,
        ahead_sequence_number: u64,
        ahead_head: &Multihash,
        reference_chain: &MetadataChainWithStats<'_>,
        expected_common_sequence_number: u64,
        expected_common_ancestor_hash: &Multihash,
    ) -> Result<CommonAncestorCheck, CompareChainsError> {
        let ahead_size = ahead_sequence_number - expected_common_sequence_number;
        ahead_chain.expecting_to_read_blocks(ahead_size);

        let ahead_blocks: Vec<HashedMetadataBlock> = ahead_chain
            .iter_blocks_interval(ahead_head, None, false)
            .take(usize::try_from(ahead_size).unwrap())
            .try_collect()
            .await?;

        // If last read block points to the previous hash that is identical to earlier
        // head, there is no divergence
        let boundary_ahead_block_data = ahead_blocks.last().map(|el| &(el.1)).unwrap();
        let boundary_block_prev_hash = boundary_ahead_block_data.prev_block_hash.as_ref();
        if boundary_block_prev_hash.is_some()
            && boundary_block_prev_hash != Some(expected_common_ancestor_hash)
        {
            let common_ancestor_sequence_number = Self::find_common_ancestor_sequence_number(
                ahead_chain,
                boundary_block_prev_hash.unwrap(),
                ahead_sequence_number - ahead_size,
                reference_chain,
                expected_common_ancestor_hash,
                expected_common_sequence_number,
            )
            .await?;
            Ok(CommonAncestorCheck::Failure {
                common_ancestor_sequence_number,
            })
        } else {
            Ok(CommonAncestorCheck::Success { ahead_blocks })
        }
    }

    fn describe_divergence(
        lhs_sequence_number: u64,
        rhs_sequence_number: u64,
        last_common_sequence_number: Option<u64>,
    ) -> CompareChainsResult {
        if let Some(last_common_sequence_number) = last_common_sequence_number {
            CompareChainsResult::Divergence {
                uncommon_blocks_in_lhs: lhs_sequence_number - last_common_sequence_number,
                uncommon_blocks_in_rhs: rhs_sequence_number - last_common_sequence_number,
            }
        } else {
            CompareChainsResult::Divergence {
                uncommon_blocks_in_lhs: lhs_sequence_number + 1,
                uncommon_blocks_in_rhs: rhs_sequence_number + 1,
            }
        }
    }

    async fn find_common_ancestor_sequence_number(
        lhs_chain: &MetadataChainWithStats<'_>,
        lhs_head: &Multihash,
        lhs_start_block_sequence_number: u64,
        rhs_chain: &MetadataChainWithStats<'_>,
        rhs_head: &Multihash,
        rhs_start_block_sequence_number: u64,
    ) -> Result<Option<u64>, CompareChainsError> {
        if lhs_start_block_sequence_number > rhs_start_block_sequence_number {
            lhs_chain.expecting_to_read_blocks(
                lhs_start_block_sequence_number - rhs_start_block_sequence_number,
            );
        } else {
            rhs_chain.expecting_to_read_blocks(
                rhs_start_block_sequence_number - lhs_start_block_sequence_number,
            );
        }

        let mut lhs_stream = lhs_chain.iter_blocks_interval(lhs_head, None, false);
        let mut rhs_stream = rhs_chain.iter_blocks_interval(rhs_head, None, false);

        let mut curr_lhs_block_sequence_number = lhs_start_block_sequence_number;
        while curr_lhs_block_sequence_number > rhs_start_block_sequence_number {
            lhs_stream.try_next().await.int_err()?;
            curr_lhs_block_sequence_number -= 1;
        }

        let mut curr_rhs_block_sequence_number = rhs_start_block_sequence_number;
        while curr_rhs_block_sequence_number > lhs_start_block_sequence_number {
            rhs_stream.try_next().await.int_err()?;
            curr_rhs_block_sequence_number -= 1;
        }

        assert_eq!(
            curr_lhs_block_sequence_number,
            curr_rhs_block_sequence_number
        );

        let mut curr_block_sequence_number = curr_lhs_block_sequence_number;
        loop {
            lhs_chain.expecting_to_read_blocks(1);
            rhs_chain.expecting_to_read_blocks(1);

            let (lhs_block_hash, _) = lhs_stream.try_next().await.int_err()?.unwrap();
            let (rhs_block_hash, _) = rhs_stream.try_next().await.int_err()?.unwrap();

            if lhs_block_hash == rhs_block_hash {
                return Ok(Some(curr_block_sequence_number));
            }
            if curr_block_sequence_number == 0 {
                return Ok(None);
            }
            curr_block_sequence_number -= 1;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum CompareChainsResult {
    Equal,
    LhsAhead {
        lhs_ahead_blocks: Vec<HashedMetadataBlock>,
    },
    LhsBehind {
        rhs_ahead_blocks: Vec<HashedMetadataBlock>,
    },
    Divergence {
        uncommon_blocks_in_lhs: u64,
        uncommon_blocks_in_rhs: u64,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum CommonAncestorCheck {
    Success {
        ahead_blocks: Vec<HashedMetadataBlock>,
    },
    Failure {
        common_ancestor_sequence_number: Option<u64>,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CompareChainsError {
    #[error(transparent)]
    Corrupted(#[from] CorruptedSourceError),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetBlockError> for CompareChainsError {
    fn from(v: GetBlockError) -> Self {
        match v {
            GetBlockError::NotFound(e) => Self::Corrupted(CorruptedSourceError {
                message: "Metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }),
            GetBlockError::BlockVersion(e) => Self::Corrupted(CorruptedSourceError {
                message: "Metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }),
            GetBlockError::BlockMalformed(e) => Self::Corrupted(CorruptedSourceError {
                message: "Metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }),
            GetBlockError::Access(e) => Self::Access(e),
            GetBlockError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<IterBlocksError> for CompareChainsError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            IterBlocksError::RefNotFound(e) => CompareChainsError::Internal(e.int_err()),
            IterBlocksError::BlockNotFound(e) => {
                CompareChainsError::Corrupted(CorruptedSourceError {
                    message: "Metadata chain is broken".to_owned(),
                    source: Some(e.into()),
                })
            }
            IterBlocksError::BlockVersion(e) => {
                CompareChainsError::Corrupted(CorruptedSourceError {
                    message: "Metadata chain is broken".to_owned(),
                    source: Some(e.into()),
                })
            }
            IterBlocksError::BlockMalformed(e) => {
                CompareChainsError::Corrupted(CorruptedSourceError {
                    message: "Metadata chain is broken".to_owned(),
                    source: Some(e.into()),
                })
            }
            IterBlocksError::InvalidInterval(_) => unreachable!(),
            IterBlocksError::Access(e) => CompareChainsError::Access(e),
            IterBlocksError::Internal(e) => CompareChainsError::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MetadataChainWithStats<'a> {
    chain: &'a dyn MetadataChain,
    on_expected: Box<dyn Fn(u64) + Send + Sync + 'a>,
    on_read: Box<dyn Fn(u64) + Send + Sync + 'a>,
}

impl<'a> MetadataChainWithStats<'a> {
    fn new(
        chain: &'a dyn MetadataChain,
        on_expected: impl Fn(u64) + Send + Sync + 'a,
        on_read: impl Fn(u64) + Send + Sync + 'a,
    ) -> Self {
        Self {
            chain,
            on_expected: Box::new(on_expected),
            on_read: Box::new(on_read),
        }
    }

    fn expecting_to_read_blocks(&self, num_blocks: u64) {
        (self.on_expected)(num_blocks);
    }
}

#[async_trait]
impl MetadataChain for MetadataChainWithStats<'_> {
    async fn resolve_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        self.chain.resolve_ref(r).await
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        (self.on_read)(1);
        self.chain.get_block(hash).await
    }

    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError> {
        (self.on_read)(1);
        self.chain.contains_block(hash).await
    }

    fn iter_blocks_interval<'b>(
        &'b self,
        head: &'b Multihash,
        tail: Option<&'b Multihash>,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'b> {
        Box::pin(
            self.chain
                .iter_blocks_interval(head, tail, ignore_missing_tail)
                .map(|v| {
                    (self.on_read)(1);
                    v
                }),
        )
    }

    fn iter_blocks_interval_inclusive<'b>(
        &'b self,
        head: &'b Multihash,
        tail: &'b Multihash,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'b> {
        Box::pin(
            self.chain
                .iter_blocks_interval_inclusive(head, tail, ignore_missing_tail)
                .map(|v| {
                    (self.on_read)(1);
                    v
                }),
        )
    }

    fn iter_blocks_interval_ref<'b>(
        &'b self,
        head: &'b BlockRef,
        tail: Option<&'b BlockRef>,
    ) -> DynMetadataStream<'b> {
        Box::pin(self.chain.iter_blocks_interval_ref(head, tail).map(|v| {
            (self.on_read)(1);
            v
        }))
    }

    async fn set_ref<'b>(
        &'b self,
        r: &BlockRef,
        hash: &Multihash,
        opts: SetRefOpts<'b>,
    ) -> Result<(), SetRefError> {
        self.chain.set_ref(r, hash, opts).await
    }

    async fn append<'b>(
        &'b self,
        block: MetadataBlock,
        opts: AppendOpts<'b>,
    ) -> Result<Multihash, AppendError> {
        self.chain.append(block, opts).await
    }

    fn as_reference_repo(&self) -> &dyn ReferenceRepository {
        self.chain.as_reference_repo()
    }

    fn as_metadata_block_repository(&self) -> &dyn MetadataBlockRepository {
        self.chain.as_metadata_block_repository()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait CompareChainsListener: Send + Sync {
    fn on_lhs_expected_reads(&self, _num_blocks: u64) {}
    fn on_lhs_read(&self, _num_blocks: u64) {}
    fn on_rhs_expected_reads(&self, _num_blocks: u64) {}
    fn on_rhs_read(&self, _num_blocks: u64) {}
}
pub struct NullCompareChainsListener;
impl CompareChainsListener for NullCompareChainsListener {}
