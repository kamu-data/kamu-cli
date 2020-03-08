/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.{
  AlreadyExistsException,
  MetadataRepository,
  MissingReferenceException
}
import dev.kamu.core.manifests.{DatasetID, VolumeID}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class AddRemoteCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  volumeID: String,
  datasetID: String
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val added = try {
      metadataRepository.addDatasetReference(
        DatasetID(datasetID),
        VolumeID(volumeID)
      )
      true
    } catch {
      case e: AlreadyExistsException =>
        logger.warn(e.getMessage + " - skipping")
        false
      case e: MissingReferenceException =>
        logger.warn(e.getMessage + " - skipping")
        false
    }

    val numAdded = if (added) 1 else 0

    logger.info(s"Added $numAdded dataset")
  }
}
