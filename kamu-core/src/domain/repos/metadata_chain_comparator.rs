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
        src_chain: &dyn MetadataChain,
        src_head: &Multihash,
        dst_chain: &dyn MetadataChain,
        dst_head: Option<&Multihash>,
    ) -> Result<ChainsComparison, CompareChainsError> {
        // When source and destination point to the same block, chains are equal, no synchronization required
        if Some(&src_head) == dst_head.as_ref() {
            return Ok(ChainsComparison::Equal);
        }

        // Extract sequence number of head blocks
        let src_sequence_number = src_chain.get_block(&src_head).await?.sequence_number;
        let dst_sequence_number = if dst_head.is_some() {
            dst_chain
                .get_block(dst_head.as_ref().unwrap())
                .await?
                .sequence_number
        } else {
            -1
        };

        // If numbers are equal, it's a guaranteed divergence, as we've checked blocks for equality above
        if src_sequence_number == dst_sequence_number {
            let last_common_sequence_number = Self::locate_last_common_block(
                src_chain,
                src_head,
                src_sequence_number,
                dst_chain,
                dst_head,
                dst_sequence_number,
            )
            .await?;
            return Ok(Self::describe_divergence(
                dst_sequence_number,
                src_sequence_number,
                last_common_sequence_number,
            ));
        }
        // Source ahead
        else if src_sequence_number > dst_sequence_number {
            let convergence_check = Self::detect_convergence(
                src_sequence_number,
                src_chain,
                &src_head,
                dst_sequence_number,
                dst_chain,
                dst_head,
            )
            .await?;
            match convergence_check {
                ConvergenceCheck::Converged { ahead_blocks } => {
                    return Ok(ChainsComparison::SourceAhead {
                        src_ahead_blocks: ahead_blocks,
                    })
                }
                ConvergenceCheck::Diverged {
                    last_common_sequence_number,
                } => {
                    return Ok(Self::describe_divergence(
                        dst_sequence_number,
                        src_sequence_number,
                        last_common_sequence_number,
                    ));
                }
            }
        }
        // Destination ahead
        else {
            let convergence_check = Self::detect_convergence(
                    dst_sequence_number,
                    dst_chain,
                    dst_head.as_ref().unwrap(),
                    src_sequence_number,
                    src_chain,
                    Some(&src_head),
                )
                .await?;
            match convergence_check {
                ConvergenceCheck::Converged { ahead_blocks } => {
                    return Ok(ChainsComparison::DestinationAhead {
                        dst_ahead_size: ahead_blocks.len(),
                    })
                }
                ConvergenceCheck::Diverged {
                    last_common_sequence_number,
                } => {
                    return Ok(Self::describe_divergence(
                        dst_sequence_number,
                        src_sequence_number,
                        last_common_sequence_number,
                    ));
                }
            }
        }
    }

    async fn detect_convergence(
        ahead_sequence_number: i32,
        ahead_chain: &dyn MetadataChain,
        ahead_head: &Multihash,
        baseline_sequence_number: i32,
        baseline_chain: &dyn MetadataChain,
        baseline_head: Option<&Multihash>,
    ) -> Result<ConvergenceCheck, CompareChainsError> {
        let ahead_size: usize = (ahead_sequence_number - baseline_sequence_number)
            .try_into()
            .unwrap();
        let ahead_blocks = ahead_chain.take_n_blocks(ahead_head, ahead_size).await?;
        // If last read block points to the previous hash that is identical to earlier head, there is no divergence
        let boundary_ahead_block_data = ahead_blocks.last().map(|el| &(el.1)).unwrap();
        let boundary_block_prev_hash = boundary_ahead_block_data.prev_block_hash.as_ref();
        if baseline_head.is_some() && baseline_head != boundary_block_prev_hash {
            let last_common_sequence_number = Self::locate_last_common_block(
                    ahead_chain,
                    ahead_head,
                    ahead_sequence_number - ahead_size as i32,
                    baseline_chain,
                    baseline_head,
                    baseline_sequence_number,
                )
                .await?;
            Ok(ConvergenceCheck::Diverged {
                last_common_sequence_number,
            })
        } else {
            Ok(ConvergenceCheck::Converged { ahead_blocks })
        }
    }

    fn describe_divergence(
        dst_sequence_number: i32,
        src_sequence_number: i32,
        last_common_sequence_number: i32,
    ) -> ChainsComparison {
        ChainsComparison::Divergence {
            uncommon_blocks_in_dst: (dst_sequence_number - last_common_sequence_number)
                .try_into()
                .unwrap(),
            uncommon_blocks_in_src: (src_sequence_number - last_common_sequence_number)
                .try_into()
                .unwrap(),
        }
    }

    async fn locate_last_common_block(
        src_chain: &dyn MetadataChain,
        src_head: &Multihash,
        src_start_block_sequence_number: i32,
        dst_chain: &dyn MetadataChain,
        dst_head: Option<&Multihash>,
        dst_start_block_sequence_number: i32,
    ) -> Result<i32, CompareChainsError> {
        if dst_head.is_none() {
            return Ok(-1);
        }

        let mut src_stream = src_chain.iter_blocks_interval(src_head, None, false);
        let mut dst_stream = dst_chain.iter_blocks_interval(dst_head.unwrap(), None, false);

        let mut curr_src_block_sequence_number = src_start_block_sequence_number;
        while curr_src_block_sequence_number > dst_start_block_sequence_number {
            src_stream.try_next().await.int_err()?;
            curr_src_block_sequence_number -= 1;
        }

        let mut curr_dst_block_sequence_number = dst_start_block_sequence_number;
        while curr_dst_block_sequence_number > src_start_block_sequence_number {
            dst_stream.try_next().await.int_err()?;
            curr_dst_block_sequence_number -= 1;
        }

        assert_eq!(
            curr_src_block_sequence_number,
            curr_dst_block_sequence_number
        );

        let mut curr_block_sequence_number = curr_src_block_sequence_number;
        while curr_block_sequence_number >= 0 {
            let (src_block_hash, _) = src_stream.try_next().await.int_err()?.unwrap();
            let (dst_block_hash, _) = dst_stream.try_next().await.int_err()?.unwrap();
            if src_block_hash == dst_block_hash {
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
    SourceAhead {
        src_ahead_blocks: Vec<(Multihash, MetadataBlock)>,
    },
    DestinationAhead {
        dst_ahead_size: usize,
    },
    Divergence {
        uncommon_blocks_in_dst: usize,
        uncommon_blocks_in_src: usize,
    },
}

/////////////////////////////////////////////////////////////////////////////////////////

enum ConvergenceCheck {
    Converged {
        ahead_blocks: Vec<(Multihash, MetadataBlock)>,
    },
    Diverged {
        last_common_sequence_number: i32,
    },
}

/////////////////////////////////////////////////////////////////////////////////////////
///
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
                message: "Source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }),
            GetBlockError::BlockVersion(e) => Self::Corrupted(CorruptedSourceError {
                message: "Source metadata chain is broken".to_owned(),
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
                    message: "Source metadata chain is broken".to_owned(),
                    source: Some(e.into()),
                }
                .into(),
            ),
            IterBlocksError::BlockVersion(e) => CompareChainsError::Corrupted(
                CorruptedSourceError {
                    message: "Source metadata chain is broken".to_owned(),
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
