package dev.kamu.cli

import dev.kamu.cli.commands._
import dev.kamu.cli.external._
import dev.kamu.cli.output._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, LogManager}

class UsageException(message: String = "", cause: Throwable = None.orNull)
    extends RuntimeException(message, cause)

object KamuApp extends App {
  val logger = LogManager.getLogger(getClass.getName)

  val fileSystem = FileSystem.get(new Configuration())
  fileSystem.setWriteChecksum(false)
  fileSystem.setVerifyChecksum(false)

  val repositoryVolumeMap = RepositoryVolumeMap(
    sourcesDir = new Path(".kamu/sources"),
    downloadDir = new Path(".kamu/downloads"),
    checkpointDir = new Path(".kamu/checkpoints"),
    dataDir = new Path(".kamu/data")
  ).toAbsolute(fileSystem)

  val metadataRepository =
    new MetadataRepository(fileSystem, repositoryVolumeMap)

  try {
    val c = new CliArgs(args)

    LogManager
      .getLogger(getClass.getPackage.getName)
      .setLevel(c.logLevel())

    val command = c.subcommands match {
      case List(c.version) =>
        new VersionCommand()
      case List(c.init) =>
        if (c.init.pullImages())
          new PullImagesCommand()
        else
          new InitCommand(
            fileSystem,
            repositoryVolumeMap
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
          getSparkRunner(c.localSpark(), c.sparkLogLevel()),
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

    if (command.requiresRepository)
      ensureRepository()

    command.run()
  } catch {
    case e: UsageException =>
      logger.error(e.getMessage)
      sys.exit(1)
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  def ensureRepository(): Unit = {
    if (!fileSystem.exists(new Path("./.kamu")))
      throw new UsageException("Not a kamu repository")

    repositoryVolumeMap.allPaths
      .filter(!fileSystem.exists(_))
      .foreach(fileSystem.mkdirs)
  }

  def getSparkRunner(useLocalSpark: Boolean, logLevel: Level): SparkRunner = {
    if (useLocalSpark)
      new SparkRunnerLocal(fileSystem, logLevel)
    else
      new SparkRunnerDocker(fileSystem, logLevel)
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
