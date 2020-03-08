/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.external.VolumeOperatorFactory
import dev.kamu.cli.{
  DoesNotExistException,
  MetadataRepository,
  UsageException,
  WorkspaceLayout
}
import dev.kamu.core.manifests.{DatasetID, Volume, VolumeID}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class PushCommand(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository,
  volumeOperatorFactory: VolumeOperatorFactory,
  volumeID: String,
  ids: Seq[String],
  all: Boolean,
  recursive: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val volume = try {
      metadataRepository.getVolume(VolumeID(volumeID))
    } catch {
      case e: DoesNotExistException =>
        throw new UsageException(e.getMessage)
    }

    val datasetIDs = {
      if (all) metadataRepository.getAllDatasets()
      else ids.map(DatasetID)
    }

    val plan = try {
      metadataRepository
        .getDatasetsInDependencyOrder(
          datasetIDs,
          recursive || all // All implies recursive, which is more efficient
        )
    } catch {
      case e: DoesNotExistException =>
        throw new UsageException(e.getMessage)
    }

    logger.debug(s"Pushing following dataset(s): ${plan.mkString(", ")}")

    val numPushed = pushToVolume(plan, volume)
    logger.info(s"Pushed $numPushed dataset(s)")
  }

  def pushToVolume(datasets: Seq[DatasetID], volume: Volume): Int = {
    val volumeOperator = volumeOperatorFactory.buildFor(volume)
    volumeOperator.push(datasets)
    datasets.length
  }
}
