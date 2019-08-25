package dev.kamu.cli

import dev.kamu.cli.commands._
import dev.kamu.cli.external.{SparkRunner, SparkRunnerDocker, SparkRunnerLocal}
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

  val cliParser = new CliParser()
  val cliOptions = cliParser.parse(args)

  val repositoryVolumeMap = RepositoryVolumeMap(
    sourcesDir = new Path(".kamu/sources"),
    downloadDir = new Path(".kamu/downloads"),
    checkpointDir = new Path(".kamu/checkpoints"),
    dataDir = new Path(".kamu/data")
  ).toAbsolute(fileSystem)

  val metadataRepository =
    new MetadataRepository(fileSystem, repositoryVolumeMap)

  try {
    cliOptions match {
      case Some(c) =>
        LogManager
          .getLogger(getClass.getPackage.getName)
          .setLevel(c.logLevel)

        val command = if (c.version.isDefined) {
          new VersionCommand()
        } else if (c.init.isDefined) {
          if (c.init.get.pullImages)
            new PullImagesCommand()
          else
            new InitCommand(
              fileSystem,
              repositoryVolumeMap
            )
        } else if (c.list.isDefined) {
          new ListCommand(
            metadataRepository
          )
        } else if (c.add.isDefined) {
          if (c.add.get.interactive)
            new AddInteractiveCommand(
              fileSystem,
              metadataRepository
            )
          else
            new AddCommand(
              fileSystem,
              metadataRepository,
              c.add.get.manifests
            )
        } else if (c.purge.isDefined) {
          new PurgeCommand(
            metadataRepository,
            c.purge.get.ids,
            c.purge.get.all
          )
        } else if (c.delete.isDefined) {
          new DeleteCommand(
            metadataRepository,
            c.delete.get.ids
          )
        } else if (c.pull.isDefined) {
          new PullCommand(
            fileSystem,
            repositoryVolumeMap,
            metadataRepository,
            getSparkRunner(c.sparkLogLevel),
            c.pull.get.ids,
            c.pull.get.all
          )
        } else if (c.depgraph.isDefined) {
          new DependencyGraphCommand(
            metadataRepository
          )
        } else if (c.sql.isDefined) {
          if (c.sql.get.server) {
            new SQLServerCommand(
              repositoryVolumeMap,
              c.sql.get.port
            )
          } else {
            new SQLShellCommand(
              repositoryVolumeMap,
              c.sql.get.url,
              c.sql.get.command,
              c.sql.get.script,
              c.sql.get.sqlLineOptions
            )
          }
        } else if (c.notebook.isDefined) {
          new NotebookCommand(
            fileSystem,
            repositoryVolumeMap,
            c.notebook.get.environmentVars
          )
        } else {
          new HelpCommand(
            cliParser
          )
        }

        if (command.requiresRepository)
          ensureRepository()

        command.run()
      case _ =>
        sys.exit(2)
    }

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

  def getSparkRunner(logLevel: Level): SparkRunner = {
    if (cliOptions.get.useLocalSpark)
      new SparkRunnerLocal(fileSystem, logLevel)
    else
      new SparkRunnerDocker(fileSystem, logLevel)
  }
}
