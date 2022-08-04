// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::{MetadataBlock, Multihash};

use thiserror::Error;
use tokio_stream::StreamExt;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainComparator {}

impl MetadataChainComparator {
    pub async fn compare_chains(
        lhs_chain: &dyn MetadataChain,
        lhs_head: &Multihash,
        rhs_chain: &dyn MetadataChain,
        rhs_head: Option<&Multihash>,
    ) -> Result<ChainsComparison, CompareChainsError> {
        // When source and destination point to the same block, chains are equal, no further scanning required
        if Some(&lhs_head) == rhs_head.as_ref() {
            return Ok(ChainsComparison::Equal);
        }

        // Extract sequence number of head blocks
        let lhs_sequence_number = lhs_chain.get_block(&lhs_head).await?.sequence_number;
        let rhs_sequence_number = if rhs_head.is_some() {
            rhs_chain
                .get_block(rhs_head.as_ref().unwrap())
                .await?
                .sequence_number
        } else {
            -1
        };

        // If numbers are equal, it's a guaranteed divergence, as we've checked blocks for equality above
        if lhs_sequence_number == rhs_sequence_number {
            let last_common_sequence_number = Self::find_common_ancestor_sequence_number(
                lhs_chain,
                lhs_head,
                lhs_sequence_number,
                rhs_chain,
                rhs_head,
                rhs_sequence_number,
            )
            .await?;
            return Ok(Self::describe_divergence(
                lhs_sequence_number,
                rhs_sequence_number,
                last_common_sequence_number,
            ));
        }
        // Source ahead
        else if lhs_sequence_number > rhs_sequence_number {
            let convergence_check = Self::check_expected_common_ancestor(
                lhs_chain,
                lhs_sequence_number,
                &lhs_head,
                rhs_chain,
                rhs_sequence_number,
                rhs_head,
            )
            .await?;
            match convergence_check {
                CommonAncestorCheck::Success { ahead_blocks } => {
                    return Ok(ChainsComparison::LhsAhead {
                        lhs_ahead_blocks: ahead_blocks,
                    })
                }
                CommonAncestorCheck::Failure {
                    common_ancestor_sequence_number: last_common_sequence_number,
                } => {
                    return Ok(Self::describe_divergence(
                        lhs_sequence_number,
                        rhs_sequence_number,
                        last_common_sequence_number,
                    ));
                }
            }
        }
        // Destination ahead
        else {
            let convergence_check = Self::check_expected_common_ancestor(
                rhs_chain,
                rhs_sequence_number,
                rhs_head.as_ref().unwrap(),
                lhs_chain,
                lhs_sequence_number,
                Some(&lhs_head),
            )
            .await?;
            match convergence_check {
                CommonAncestorCheck::Success { ahead_blocks } => {
                    return Ok(ChainsComparison::LhsBehind {
                        rhs_ahead_blocks: ahead_blocks,
                    })
                }
                CommonAncestorCheck::Failure {
                    common_ancestor_sequence_number: last_common_sequence_number,
                } => {
                    return Ok(Self::describe_divergence(
                        lhs_sequence_number,
                        rhs_sequence_number,
                        last_common_sequence_number,
                    ));
                }
            }
        }
    }

    async fn check_expected_common_ancestor(
        ahead_chain: &dyn MetadataChain,
        ahead_sequence_number: i32,
        ahead_head: &Multihash,
        reference_chain: &dyn MetadataChain,
        expected_common_sequence_number: i32,
        expected_common_ancestor_hash: Option<&Multihash>,
    ) -> Result<CommonAncestorCheck, CompareChainsError> {
        use futures::TryStreamExt;
        let ahead_size: usize = (ahead_sequence_number - expected_common_sequence_number) as usize;
        let ahead_blocks: Vec<(Multihash, MetadataBlock)> = ahead_chain
            .iter_blocks_interval(ahead_head, None, false)
            .take(ahead_size)
            .try_collect()
            .await?;

        // If last read block points to the previous hash that is identical to earlier head, there is no divergence
        let boundary_ahead_block_data = ahead_blocks.last().map(|el| &(el.1)).unwrap();
        let boundary_block_prev_hash = boundary_ahead_block_data.prev_block_hash.as_ref();
        if expected_common_ancestor_hash.is_some()
            && expected_common_ancestor_hash != boundary_block_prev_hash
        {
            let common_ancestor_sequence_number = Self::find_common_ancestor_sequence_number(
                ahead_chain,
                ahead_head,
                ahead_sequence_number - ahead_size as i32,
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
        lhs_sequence_number: i32,
        rhs_sequence_number: i32,
        last_common_sequence_number: i32,
    ) -> ChainsComparison {
        ChainsComparison::Divergence {
            uncommon_blocks_in_lhs: (lhs_sequence_number - last_common_sequence_number) as usize,
            uncommon_blocks_in_rhs: (rhs_sequence_number - last_common_sequence_number) as usize,
        }
    }

    async fn find_common_ancestor_sequence_number(
        lhs_chain: &dyn MetadataChain,
        lhs_head: &Multihash,
        lhs_start_block_sequence_number: i32,
        rhs_chain: &dyn MetadataChain,
        rhs_head: Option<&Multihash>,
        rhs_start_block_sequence_number: i32,
    ) -> Result<i32, CompareChainsError> {
        if rhs_head.is_none() {
            return Ok(-1);
        }

        let mut lhs_stream = lhs_chain.iter_blocks_interval(lhs_head, None, false);
        let mut rhs_stream = rhs_chain.iter_blocks_interval(rhs_head.unwrap(), None, false);

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
        while curr_block_sequence_number >= 0 {
            let (lhs_block_hash, _) = lhs_stream.try_next().await.int_err()?.unwrap();
            let (rhs_block_hash, _) = rhs_stream.try_next().await.int_err()?.unwrap();
            if lhs_block_hash == rhs_block_hash {
                return Ok(curr_block_sequence_number);
            }
            curr_block_sequence_number -= 1;
        }

        Ok(-1)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub enum ChainsComparison {
    Equal,
    LhsAhead {
        lhs_ahead_blocks: Vec<(Multihash, MetadataBlock)>,
    },
    LhsBehind {
        rhs_ahead_blocks: Vec<(Multihash, MetadataBlock)>,
    },
    Divergence {
        uncommon_blocks_in_lhs: usize,
        uncommon_blocks_in_rhs: usize,
    },
}

/////////////////////////////////////////////////////////////////////////////////////////

enum CommonAncestorCheck {
    Success {
        ahead_blocks: Vec<(Multihash, MetadataBlock)>,
    },
    Failure {
        common_ancestor_sequence_number: i32,
    },
}

/////////////////////////////////////////////////////////////////////////////////////////

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
            GetBlockError::Access(e) => Self::Access(e),
            GetBlockError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<IterBlocksError> for CompareChainsError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            IterBlocksError::RefNotFound(e) => CompareChainsError::Internal(e.int_err()),
            IterBlocksError::BlockNotFound(e) => CompareChainsError::Corrupted(
                CorruptedSourceError {
                    message: "Metadata chain is broken".to_owned(),
                    source: Some(e.into()),
                }
                .into(),
            ),
            IterBlocksError::BlockVersion(e) => CompareChainsError::Corrupted(
                CorruptedSourceError {
                    message: "Metadata chain is broken".to_owned(),
                    source: Some(e.into()),
                }
                .into(),
            ),
            IterBlocksError::InvalidInterval(_) => unreachable!(),
            IterBlocksError::Access(e) => CompareChainsError::Access(e),
            IterBlocksError::Internal(e) => CompareChainsError::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
