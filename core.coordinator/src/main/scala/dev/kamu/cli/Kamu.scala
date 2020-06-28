/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.io.PrintStream

import better.files.File
import dev.kamu.cli.commands._
import dev.kamu.cli.transform.{EngineFactory, TransformService}
import dev.kamu.cli.external._
import dev.kamu.cli.ingest.IngestService
import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.cli.output._
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{Clock, DockerClient}
import org.apache.logging.log4j.Level

class Kamu(
  config: KamuConfig,
  systemClock: Clock
) {
  val workspaceLayout = WorkspaceLayout(
    kamuRootDir = config.kamuRoot,
    metadataDir = config.kamuRoot / "datasets",
    remotesDir = config.kamuRoot / "remotes",
    localVolumeDir = config.localVolume
  ).toAbsolute

  val metadataRepository =
    new MetadataRepository(workspaceLayout, systemClock)

  val remoteOperatorFactory =
    new RemoteOperatorFactory(workspaceLayout, metadataRepository)

  def run(cliArgs: CliArgs): Unit = {
    val command = getCommand(cliArgs)

    if (command.requiresWorkspace)
      ensureWorkspace()

    command.run()
  }

  def run(args: String*): Unit = {
    val cliArgs = new CliArgs(args)
    run(cliArgs)
  }

  def getCommand(c: CliArgs): Command = {
    c.subcommands match {
      case List(c.version) =>
        new VersionCommand()
      case List(c.init) =>
        if (c.init.pullImages())
          new PullImagesCommand(getDockerClient())
        else
          new InitCommand(workspaceLayout)
      case List(c.list) =>
        new ListCommand(
          metadataRepository,
          getOutputFormatter(c.list.getOutputFormat)
        )
      case List(c.add) =>
        if (c.add.interactive())
          new AddInteractiveCommand(
            metadataRepository
          )
        else
          new AddCommand(
            metadataRepository,
            c.add.manifests(),
            c.add.replace()
          )
      case List(c.purge) =>
        new PurgeCommand(
          metadataRepository,
          c.purge.ids(),
          c.purge.all(),
          c.purge.recursive()
        )
      case List(c.delete) =>
        new DeleteCommand(
          metadataRepository,
          c.delete.ids()
        )
      case List(c.pull) =>
        if (c.pull.setWatermark.isEmpty) {
          val engineFactory = getEngineFactory(
            if (c.debug()) Level.INFO else c.sparkLogLevel()
          )
          new PullCommand(
            new IngestService(
              workspaceLayout,
              metadataRepository,
              engineFactory,
              systemClock
            ),
            new TransformService(
              metadataRepository,
              systemClock,
              engineFactory
            ),
            metadataRepository,
            remoteOperatorFactory,
            c.pull.ids(),
            c.pull.all(),
            c.pull.recursive()
          )
        } else {
          new AssignWatermarkCommand(
            systemClock,
            metadataRepository,
            c.pull.ids(),
            c.pull.setWatermark()
          )
        }
      case List(c.log) =>
        new LogCommand(
          metadataRepository,
          c.log.id()
        )
      case List(c.push) =>
        new PushCommand(
          workspaceLayout,
          metadataRepository,
          remoteOperatorFactory,
          c.push.remote(),
          c.push.ids(),
          c.push.all(),
          c.push.recursive()
        )
      case List(c.remote, c.remote.list) =>
        new RemoteListCommand(
          metadataRepository,
          getOutputFormatter(c.remote.list.getOutputFormat)
        )
      case List(c.remote, c.remote.add) =>
        new RemoteAddCommand(
          metadataRepository,
          remoteOperatorFactory,
          c.remote.add.manifests(),
          c.remote.add.replace()
        )
      case List(c.remote, c.remote.delete) =>
        new RemoteDeleteCommand(
          metadataRepository,
          c.remote.delete.ids()
        )
      case List(c.sql) =>
        new SQLShellCommand(
          metadataRepository,
          getDockerClient(),
          c.sql.url.toOption,
          c.sql.command.toOption,
          c.sql.script.toOption,
          c.sql.getOutputFormat,
          getOutputStream()
        )
      case List(c.sql, c.sql.server) =>
        new SQLServerCommand(
          metadataRepository,
          getDockerClient(),
          c.sql.server.port.toOption
        )
      case List(c.notebook) =>
        new NotebookCommand(
          metadataRepository,
          getDockerClient(),
          c.notebook.env()
        )
      case List(c.completion) =>
        new CompletionCommand(
          c,
          c.completion.shell.toOption.get
        )
      case List(c.depgraph) =>
        new DependencyGraphCommand(
          metadataRepository
        )
      case _ =>
        new HelpCommand(
          c
        )
    }
  }

  protected def ensureWorkspace(): Unit = {
    if (!File(workspaceLayout.kamuRootDir).isDirectory)
      throw new UsageException("Not a kamu workspace")
  }

  protected def getDockerClient(): DockerClient = {
    new DockerClient()
  }

  protected def getEngineFactory(logLevel: Level): EngineFactory = {
    new EngineFactory(workspaceLayout, logLevel)
  }

  protected def getOutputStream(): PrintStream = {
    System.out
  }

  protected def getOutputFormatter(
    outputFormat: OutputFormat
  ): OutputFormatter = {
    val readableFormatter = new MissingValueFormatter(
      "",
      new ReadablePowerOfTwoValueFormatter(
        new ReadableRelativeTimeValueFormatter(
          systemClock.instant(),
          new SimpleValueFormatter()
        )
      )
    )

    val strictFormatter = new MissingValueFormatter(
      "",
      new SimpleValueFormatter()
    )

    outputFormat.outputFormat.map(_.toLowerCase).getOrElse("table") match {
      case "table" =>
        new TableOutputFormatter(System.out, outputFormat, readableFormatter)
      case "csv" =>
        val f = outputFormat.copy(
          delimiter = outputFormat.delimiter.orElse(Some(","))
        )
        new DelimitedFormatter(System.out, f, strictFormatter)
      case "tsv" =>
        val f = outputFormat.copy(
          delimiter = outputFormat.delimiter.orElse(Some("\t"))
        )
        new DelimitedFormatter(System.out, f, strictFormatter)
      case fmt =>
        throw new UsageException(s"Unsupported format: $fmt")
    }
  }
}
