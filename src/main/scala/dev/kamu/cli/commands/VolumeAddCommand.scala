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
  AlreadyExistsException,
  MetadataRepository,
  MissingReferenceException,
  SchemaNotSupportedException
}
import dev.kamu.core.manifests.Volume
import org.apache.log4j.LogManager

class VolumeAddCommand(
  metadataRepository: MetadataRepository,
  volumeOperatorFactory: VolumeOperatorFactory,
  manifests: Seq[java.net.URI],
  replace: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val volumes = {
      try {
        manifests.map(manifestURI => {
          logger.debug(s"Loading volume from: $manifestURI")
          metadataRepository.loadVolumeFromURI(manifestURI)
        })
      } catch {
        case e: java.io.FileNotFoundException =>
          logger.error(s"File not found: ${e.getMessage} - aborted")
          Seq.empty
        case e: SchemaNotSupportedException =>
          logger.error(s"URI schema not supported: ${e.getMessage} - aborted")
          Seq.empty
      }
    }

    @scala.annotation.tailrec
    def addVolume(volume: Volume): Boolean = {
      try {
        volumeOperatorFactory.ensureSupported(volume)
        metadataRepository.addVolume(volume)
        true
      } catch {
        case e: NotImplementedError =>
          logger.warn(e.getMessage + " - skipping")
          false
        case e: AlreadyExistsException =>
          if (replace) {
            logger.warn(e.getMessage + " - replacing")
            metadataRepository.deleteVolume(volume.id)
            addVolume(volume)
          } else {
            logger.warn(e.getMessage + " - skipping")
            false
          }
        case e: MissingReferenceException =>
          logger.warn(e.getMessage + " - skipping")
          false
      }
    }

    val numAdded = volumes
      .map(addVolume)
      .count(added => added)

    logger.info(s"Added $numAdded volume(s)")
  }
}
