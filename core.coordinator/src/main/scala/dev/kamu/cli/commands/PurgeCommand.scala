/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.UsageException
import dev.kamu.cli.metadata._
import dev.kamu.core.manifests.DatasetID
import org.apache.log4j.LogManager

class PurgeCommand(
  metadataRepository: MetadataRepository,
  ids: Seq[String],
  all: Boolean,
  recursive: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {
    val datasetIDs = {
      if (all) metadataRepository.getAllDatasets()
      else ids.map(DatasetID)
    }

    val plan = try {
      metadataRepository
        .getDatasetsInReverseDependencyOrder(
          datasetIDs,
          recursive || all // All implies recursive, which is more efficient
        )
    } catch {
      case e: DoesNotExistException =>
        throw new UsageException(e.getMessage)
    }

    val ghostSnapshots = plan.map(id => {
      logger.info(s"Purging dataset: ${id.toString}")
      val snapshot = metadataRepository.getMetadataChain(id).getSnapshot()
      try {
        metadataRepository.deleteDataset(id)
      } catch {
        case e: DanglingReferenceException =>
          throw new UsageException(e.getMessage)
      }
      snapshot
    })

    ghostSnapshots.reverse.foreach(metadataRepository.addDataset)

    logger.info(s"Purged ${ghostSnapshots.size} datasets")
  }
}
