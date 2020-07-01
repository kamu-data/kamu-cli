/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import java.io.PrintStream
import java.net.URI
import java.nio.file.Path

import dev.kamu.cli.external.LivyDockerProcessBuilder
import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.cli.output.OutputFormat
import dev.kamu.core.utils.{DockerClient, IOHandlerPresets}
import org.apache.logging.log4j.LogManager
import sqlline.SqlLine

import scala.concurrent.duration._

class SQLShellCommand(
  metadataRepository: MetadataRepository,
  dockerClient: DockerClient,
  url: Option[URI],
  command: Option[String],
  script: Option[Path],
  outputFormat: OutputFormat,
  outputStream: PrintStream
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresWorkspace: Boolean = url.isEmpty

  override def run(): Unit = {
    var args = Array(
      "--autoCommit=false",
      s"--color=${outputFormat.color}",
      "-nn",
      "kamu"
    )

    if (command.isDefined)
      args ++= Seq("-e", command.get)
    else if (script.isDefined)
      args ++= Seq("-f", script.get.toUri.getPath)

    if (outputFormat.incremental)
      args ++= Seq(s"--incremental=true${outputFormat.incremental}")

    if (!outputFormat.withHeader)
      args ++= Seq(s"--showHeader=${outputFormat.withHeader}")

    if (outputFormat.headerInterval.isDefined)
      args ++= Seq(s"--headerInterval=${outputFormat.headerInterval.get}")

    if (outputFormat.outputFormat.isDefined)
      args ++= Seq(s"--outputformat=${outputFormat.outputFormat.get}")

    if (outputFormat.delimiter.isDefined)
      args ++= Seq(s"--csvDelimiter=${outputFormat.delimiter.get}")

    if (outputFormat.quoteCharacter.isDefined)
      args ++= Seq(
        s"--csvQuoteCharacter=${outputFormat.quoteCharacter.get}"
      )

    if (outputFormat.nullValue.isDefined)
      args ++= Seq(s"--nullValue=${outputFormat.nullValue.get}")

    if (outputFormat.numberFormat.isDefined)
      args ++= Seq(s"--numberFormat=${outputFormat.numberFormat.get}")

    if (outputFormat.dateFormat.isDefined)
      args ++= Seq(s"--dateFormat=${outputFormat.dateFormat.get}")

    if (outputFormat.timeFormat.isDefined)
      args ++= Seq(s"--timeFormat=${outputFormat.timeFormat.get}")

    if (outputFormat.timestampFormat.isDefined)
      args ++= Seq(s"--timestampFormat=${outputFormat.timestampFormat.get}")

    maybeRunServer { url =>
      args ++= Seq("-u", url.toString)

      logger.debug("Starting sqlline: " + args.mkString(" "))

      val sqlline = new SqlLine()
      sqlline.setOutputStream(outputStream)
      sqlline.begin(args, null, true)
    }
  }

  def maybeRunServer[T](body: URI => T): T = {
    if (url.isDefined) {
      // Server is already running
      body(url.get)
    } else {
      // Start Livy container
      val containerPort = 10090

      val livyProcess = new LivyDockerProcessBuilder(
        volumeLayout = metadataRepository.getLocalVolume(),
        dockerClient = dockerClient,
        exposePorts = List(containerPort)
      ).run(
        Some(
          IOHandlerPresets.redirectToLogger(logger, tag = "[livy] ")
        )
      )

      try {
        val hostPort = livyProcess.waitForHostPort(containerPort, 60 seconds)
        val livyUrl =
          URI.create(s"jdbc:hive2://${dockerClient.getDockerHost}:${hostPort}")
        logger.debug(s"Resolved Livy URL: $livyUrl")

        body(livyUrl)
      } finally {
        livyProcess.stop()
        livyProcess.join()
      }
    }
  }
}
