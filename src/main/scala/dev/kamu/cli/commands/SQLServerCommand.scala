/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.MetadataRepository
import dev.kamu.cli.external.{DockerClient, LivyDockerProcessBuilder}
import org.apache.log4j.LogManager

import scala.concurrent.duration._

class SQLServerCommand(
  metadataRepository: MetadataRepository,
  dockerClient: DockerClient,
  port: Option[Int]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {
    val containerPort = 10090

    val livyProcess = new LivyDockerProcessBuilder(
      volumeLayout = metadataRepository.getLocalVolume(),
      dockerClient = dockerClient,
      exposePorts = if (port.isEmpty) List(containerPort) else List.empty,
      exposePortMap =
        if (port.isDefined) Map(containerPort -> port.get) else Map.empty
    ).run()

    // TODO: Avoid thrift ecxeption during testing of the port
    val hostPort = livyProcess.waitForHostPort(containerPort, 15 seconds)

    logger.info(s"Server is running at: jdbc:hive2://localhost:$hostPort")

    try {
      livyProcess.join()
    } finally {
      livyProcess.kill()
    }
  }
}
