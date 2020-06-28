/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.external.DockerImages
import dev.kamu.core.utils.DockerClient
import org.apache.logging.log4j.LogManager

class PullImagesCommand(
  dockerClient: DockerClient
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresWorkspace: Boolean = false

  def run(): Unit = {
    DockerImages.ALL.foreach(image => {
      logger.info(s"Pulling image: $image")
      dockerClient.pull(image)
    })
  }
}
