/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.io.PrintStream

import dev.kamu.cli.commands._
import dev.kamu.cli.transform.TransformService
import dev.kamu.cli.external._
import dev.kamu.cli.ingest.IngestService
import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.cli.output._
import dev.kamu.core.utils.Clock
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Level

class Kamu(
  config: KamuConfig,
  fileSystem: FileSystem,
  systemClock: Clock
) {
  val workspaceLayout = WorkspaceLayout(
    kamuRootDir = config.kamuRoot,
    metadataDir = config.kamuRoot.resolve("datasets"),
    remotesDir = config.kamuRoot.resolve("remotes"),
    localVolumeDir = config.localVolume
  ).toAbsolute(fileSystem)

  val metadataRepository =
    new MetadataRepository(fileSystem, workspaceLayout, systemClock)

  val remoteOperatorFactory =
    new RemoteOperatorFactory(fileSystem, workspaceLayout, metadataRepository)

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
          new InitCommand(
            fileSystem,
            workspaceLayout
          )
      case List(c.list) =>
        new ListCommand(
          metadataRepository,
          getOutputFormatter(c.list.getOutputFormat)
        )
      case List(c.add) =>
        if (c.add.interactive())
          new AddInteractiveCommand(
            fileSystem,
            metadataRepository
          )
        else
          new AddCommand(
            fileSystem,
            metadataRepository,
            c.add.manifests(),
            c.add.replace()
          )
      case List(c.purge) =>
        new PurgeCommand(
          metadataRepository,
          c.purge.ids(),
          c.purge.all()
        )
      case List(c.delete) =>
        new DeleteCommand(
          metadataRepository,
          c.delete.ids()
        )
      case List(c.pull) =>
        val sparkRunner = getSparkRunner(
          c.localSpark(),
          if (c.debug()) Level.INFO else c.sparkLogLevel()
        )
        new PullCommand(
          fileSystem,
          new IngestService(
            fileSystem,
            workspaceLayout,
            metadataRepository,
            sparkRunner,
            systemClock
          ),
          new TransformService(
            fileSystem,
            workspaceLayout,
            metadataRepository,
            systemClock,
            sparkRunner
          ),
          workspaceLayout,
          metadataRepository,
          remoteOperatorFactory,
          sparkRunner,
          c.pull.ids(),
          c.pull.all(),
          c.pull.recursive()
        )
      case List(c.log) =>
        new LogCommand(
          metadataRepository,
          c.log.id()
        )
      case List(c.push) =>
        new PushCommand(
          fileSystem,
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
          fileSystem,
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

  def ensureWorkspace(): Unit = {
    if (!fileSystem.exists(workspaceLayout.kamuRootDir))
      throw new UsageException("Not a kamu workspace")
  }

  def getDockerClient(): DockerClient = {
    new DockerClient(fileSystem)
  }

  def getSparkRunner(useLocalSpark: Boolean, logLevel: Level): SparkRunner = {
    if (useLocalSpark)
      new SparkRunnerLocal(
        assemblyPath,
        fileSystem,
        logLevel,
        config.spark
      )
    else
      new SparkRunnerDocker(
        assemblyPath,
        fileSystem,
        logLevel,
        config.spark,
        getDockerClient()
      )
  }

  def assemblyPath: Path = {
    new Path(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
  }

  def getOutputStream(): PrintStream = {
    System.out
  }

  def getOutputFormatter(outputFormat: OutputFormat): OutputFormatter = {
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
