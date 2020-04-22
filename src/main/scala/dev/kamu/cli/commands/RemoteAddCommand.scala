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
  AlreadyExistsException,
  MetadataRepository,
  MissingReferenceException,
  SchemaNotSupportedException
}
import dev.kamu.core.manifests.Remote
import org.apache.log4j.LogManager

class RemoteAddCommand(
  metadataRepository: MetadataRepository,
  remoteOperatorFactory: RemoteOperatorFactory,
  manifests: Seq[java.net.URI],
  replace: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val remotes = {
      try {
        manifests.map(manifestURI => {
          logger.debug(s"Loading remote from: $manifestURI")
          metadataRepository.loadRemoteFromURI(manifestURI)
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
    def addRemote(remote: Remote): Boolean = {
      try {
        remoteOperatorFactory.ensureSupported(remote)
        metadataRepository.addRemote(remote)
        true
      } catch {
        case e: NotImplementedError =>
          logger.warn(e.getMessage + " - skipping")
          false
        case e: AlreadyExistsException =>
          if (replace) {
            logger.warn(e.getMessage + " - replacing")
            metadataRepository.deleteRemote(remote.id)
            addRemote(remote)
          } else {
            logger.warn(e.getMessage + " - skipping")
            false
          }
        case e: MissingReferenceException =>
          logger.warn(e.getMessage + " - skipping")
          false
      }
    }

    val numAdded = remotes
      .map(addRemote)
      .count(added => added)

    logger.info(s"Added $numAdded volume(s)")
  }
}
