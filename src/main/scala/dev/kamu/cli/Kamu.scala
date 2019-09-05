package dev.kamu.cli

import dev.kamu.cli.commands._
import dev.kamu.cli.external._
import dev.kamu.cli.output._
import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Level

class Kamu(
  config: KamuConfig,
  fileSystem: FileSystem
) {
  val repositoryVolumeMap = RepositoryVolumeMap(
    sourcesDir = config.kamuRoot.resolve("sources"),
    downloadDir = config.kamuRoot.resolve("downloads"),
    checkpointDir = config.kamuRoot.resolve("checkpoints"),
    dataDir = config.kamuRoot.resolve("data")
  ).toAbsolute(fileSystem)

  val metadataRepository =
    new MetadataRepository(fileSystem, repositoryVolumeMap)

  def run(cliArgs: CliArgs): Unit = {
    val command = getCommand(cliArgs)

    if (command.requiresRepository)
      ensureRepository()

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
          new PullImagesCommand()
        else
          new InitCommand(
            fileSystem,
            repositoryVolumeMap,
            config.repositoryRoot
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
            c.add.manifests()
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
        new PullCommand(
          fileSystem,
          repositoryVolumeMap,
          metadataRepository,
          getSparkRunner(
            c.localSpark(),
            if (c.debug()) Level.INFO else c.sparkLogLevel()
          ),
          c.pull.ids(),
          c.pull.all()
        )
      case List(c.depgraph) =>
        new DependencyGraphCommand(
          metadataRepository
        )
      case List(c.sql) =>
        new SQLShellCommand(
          repositoryVolumeMap,
          c.sql.url.toOption,
          c.sql.command.toOption,
          c.sql.script.toOption,
          c.sql.getOutputFormat
        )
      case List(c.sql, c.sql.server) =>
        new SQLServerCommand(
          repositoryVolumeMap,
          c.sql.server.port.toOption
        )
      case List(c.notebook) =>
        new NotebookCommand(
          fileSystem,
          repositoryVolumeMap,
          c.notebook.env()
        )
      case _ =>
        new HelpCommand(
          c
        )
    }
  }

  def ensureRepository(): Unit = {
    if (!fileSystem.exists(config.kamuRoot))
      throw new UsageException("Not a kamu repository")

    repositoryVolumeMap.allPaths
      .filter(!fileSystem.exists(_))
      .foreach(fileSystem.mkdirs)
  }

  def getSparkRunner(useLocalSpark: Boolean, logLevel: Level): SparkRunner = {
    if (useLocalSpark)
      new SparkRunnerLocal(fileSystem, logLevel, config.spark)
    else
      new SparkRunnerDocker(fileSystem, logLevel, config.spark)
  }

  def getOutputFormatter(outputFormat: OutputFormat): OutputFormatter = {
    outputFormat.outputFormat.map(_.toLowerCase).getOrElse("table") match {
      case "table" =>
        new TableOutputFormatter(System.out, outputFormat)
      case "csv" =>
        val f = outputFormat.copy(
          csvDelimiter = outputFormat.csvDelimiter.orElse(Some(","))
        )
        new DelimitedFormatter(System.out, f)
      case "tsv" =>
        val f = outputFormat.copy(
          csvDelimiter = outputFormat.csvDelimiter.orElse(Some("\t"))
        )
        new DelimitedFormatter(System.out, f)
      case fmt =>
        throw new UsageException(s"Unsupported format: $fmt")
    }
  }
}
