/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import java.time.Instant

import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.core.manifests.{DatasetID, MetadataBlock}
import dev.kamu.core.utils.Clock
import org.apache.logging.log4j.LogManager

class AssignWatermarkCommand(
  systemClock: Clock,
  metadataRepository: MetadataRepository,
  ids: Seq[String],
  watermark: Instant
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val datasetIDs = ids.map(DatasetID)

    val numUpdated = datasetIDs.map(assignWatermark).count(b => b)

    logger.info(s"Updated $numUpdated datasets")
  }

  def assignWatermark(datasetID: DatasetID): Boolean = {
    val metaChain = metadataRepository.getMetadataChain(datasetID)

    val newBlock = metaChain.append(
      MetadataBlock(
        blockHash = "",
        prevBlockHash = metaChain.getBlocks().last.blockHash,
        systemTime = systemClock.instant(),
        outputWatermark = Some(watermark)
      )
    )

    logger.info(
      s"Committed new block: $datasetID (${newBlock.blockHash})"
    )

    true
  }
}
