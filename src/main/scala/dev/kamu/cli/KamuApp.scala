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

        if (c.init.isDefined) {
          new InitCommand(
            fileSystem,
            repositoryVolumeMap
          ).run()
        } else {
          // Other commands need to have repository initialized
          if (repositoryVolumeMap.allPaths.exists(!fileSystem.exists(_)))
            throw new UsageException("Not a kamu repository")

          if (c.list.isDefined) {
            new ListCommand(
              metadataRepository
            ).run()
          } else if (c.add.isDefined) {
            if (c.add.get.interactive)
              new AddInteractiveCommand(
                fileSystem,
                metadataRepository
              ).run()
            else
              new AddCommand(
                fileSystem,
                metadataRepository,
                c.add.get.manifests
              ).run()
          } else if (c.purge.isDefined) {
            new PurgeCommand(
              metadataRepository,
              c.purge.get.ids,
              c.purge.get.all
            ).run()
          } else if (c.delete.isDefined) {
            new DeleteCommand(metadataRepository, c.delete.get.ids).run()
          } else if (c.pull.isDefined) {
            new PullCommand(
              fileSystem,
              repositoryVolumeMap,
              metadataRepository,
              getSparkRunner(c.sparkLogLevel),
              c.pull.get.ids,
              c.pull.get.all
            ).run()
          } else if (c.depgraph.isDefined) {
            new DependencyGraphCommand(
              metadataRepository
            ).run()
          } else if (c.sql.isDefined) {
            if (c.sql.get.server) {
              new SQLServerCommand(
                repositoryVolumeMap,
                c.sql.get.port
              ).run()
            } else if (c.sql.get.command.isDefined) {
              new SQLCommand(
                fileSystem,
                repositoryVolumeMap,
                getSparkRunner(c.sparkLogLevel),
                c.sql.get.command
              ).run()
            } else {
              new SQLShellCommand(
                repositoryVolumeMap
              ).run()
            }
          } else if (c.notebook.isDefined) {
            new NotebookCommand(
              fileSystem,
              repositoryVolumeMap
            ).run()
          } else {
            println(cliParser.usage())
          }
        }
      case _ =>
    }

  } catch {
    case e: UsageException =>
      logger.error(e.getMessage)
      sys.exit(1)
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  def getSparkRunner(logLevel: Level): SparkRunner = {
    if (cliOptions.get.useLocalSpark)
      new SparkRunnerLocal(fileSystem, logLevel)
    else
      new SparkRunnerDocker(fileSystem, logLevel)
  }
}
