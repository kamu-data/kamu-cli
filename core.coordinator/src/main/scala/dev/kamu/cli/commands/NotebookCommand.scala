/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.external.NotebookRunnerDocker
import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.core.utils.DockerClient
import org.apache.logging.log4j.LogManager

class NotebookCommand(
  metadataRepository: MetadataRepository,
  dockerClient: DockerClient,
  environmentVars: Map[String, String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    new NotebookRunnerDocker(
      dockerClient,
      metadataRepository,
      environmentVars
    ).start()
  }
}
