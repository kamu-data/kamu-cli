/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.external.RemoteOperatorFactory
import dev.kamu.cli.{
  DoesNotExistException,
  MetadataRepository,
  UsageException,
  WorkspaceLayout
}
import dev.kamu.core.manifests.{DatasetID, Remote, RemoteID}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class PushCommand(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository,
  remoteOperatorFactory: RemoteOperatorFactory,
  remoteID: String,
  ids: Seq[String],
  all: Boolean,
  recursive: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val remote = try {
      metadataRepository.getRemote(RemoteID(remoteID))
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

    val numPushed = pushToRemote(plan, remote)
    logger.info(s"Pushed $numPushed dataset(s)")
  }

  def pushToRemote(datasets: Seq[DatasetID], remote: Remote): Int = {
    val volumeOperator = remoteOperatorFactory.buildFor(remote)
    volumeOperator.push(datasets)
    datasets.length
  }
}
