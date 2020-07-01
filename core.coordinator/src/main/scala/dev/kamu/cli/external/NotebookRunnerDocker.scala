/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.external

import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.core.utils.{DockerClient, DockerProcess, IOHandlerPresets}
import org.apache.logging.log4j.LogManager
import sun.misc.{Signal, SignalHandler}

class NotebookRunnerDocker(
  dockerClient: DockerClient,
  metadataRepository: MetadataRepository,
  environmentVars: Map[String, String]
) {
  protected val logger = LogManager.getLogger(getClass.getName)

  def start(): Unit = {
    val network = "kamu"
    dockerClient.withNetwork(network, Some(IOHandlerPresets.logged(logger))) {
      val livyBuilder =
        new LivyDockerProcessBuilder(
          metadataRepository.getLocalVolume(),
          dockerClient,
          Some(network)
        )

      val jupyterBuilder =
        new JupyterDockerProcessBuilder(
          dockerClient,
          network,
          environmentVars
        )

      var livyProcess: DockerProcess = null
      var jupyterProcess: JupyterDockerProcess = null

      def stopAll(): Unit = {
        if (livyProcess != null) {
          livyProcess.stop()
          livyProcess.join()
        }
        if (jupyterProcess != null) {
          jupyterProcess.stop()
          jupyterProcess.join()
        }
      }

      Signal.handle(new Signal("INT"), new SignalHandler {
        override def handle(signal: Signal): Unit = {
          stopAll()
        }
      })

      try {
        livyProcess = livyBuilder.run()
        jupyterProcess = jupyterBuilder.run()
        jupyterProcess.openBrowserWhenReady()
        jupyterProcess.join()
        livyProcess.join()
      } finally {
        stopAll()
        jupyterBuilder.chown()
      }
    }
  }
}
