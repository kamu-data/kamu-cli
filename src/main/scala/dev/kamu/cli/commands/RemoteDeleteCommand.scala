/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.{DoesNotExistException, MetadataRepository}
import dev.kamu.core.manifests.RemoteID
import org.apache.log4j.LogManager

class RemoteDeleteCommand(
  metadataRepository: MetadataRepository,
  ids: Seq[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val numDeleted = ids
      .map(RemoteID)
      .map(id => {
        try {
          metadataRepository.deleteRemote(id)
          1
        } catch {
          case e: DoesNotExistException =>
            logger.error(e.getMessage)
            0
        }
      })

    logger.info(s"Deleted $numDeleted volume(s)")
  }
}
