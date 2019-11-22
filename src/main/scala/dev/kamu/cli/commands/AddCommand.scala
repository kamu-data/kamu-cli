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
  MissingReferenceException,
  SchemaNotSupportedException
}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class AddCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  manifests: Seq[java.net.URI]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val sources = {
      try {
        manifests.map(manifestURI => {
          logger.debug(s"Loading dataset from: $manifestURI")
          metadataRepository.loadDatasetFromURI(manifestURI)
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

    val numAdded = sources
      .map(ds => {
        try {
          metadataRepository.addDataset(ds)
          true
        } catch {
          case e: AlreadyExistsException =>
            logger.warn(e.getMessage + " - skipping")
            false
          case e: MissingReferenceException =>
            logger.warn(e.getMessage + " - skipping")
            false
        }
      })
      .count(added => added)
    logger.info(s"Added $numAdded datasets")
  }
}
