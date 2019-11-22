/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.{
  DanglingReferenceException,
  DoesNotExistsException,
  MetadataRepository
}
import dev.kamu.core.manifests.DatasetID
import org.apache.log4j.LogManager

class DeleteCommand(
  metadataRepository: MetadataRepository,
  ids: Seq[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {
    val numDeleted = ids
      .map(DatasetID)
      .map(id => {
        try {
          metadataRepository.deleteDataset(id)
          1
        } catch {
          case e: DoesNotExistsException =>
            logger.error(e.getMessage)
            0
          case e: DanglingReferenceException =>
            logger.error(e.getMessage)
            0
        }
      })
      .sum

    logger.info(s"Deleted $numDeleted datasets")
  }
}
