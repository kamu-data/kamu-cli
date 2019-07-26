package dev.kamu.cli

import dev.kamu.cli.commands.{
  AddManifestCommand,
  DeleteCommand,
  DependencyGraphCommand,
  InitCommand,
  ListCommand,
  NotebookCommand,
  PullCommand
}
import dev.kamu.core.manifests.RepositoryVolumeMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

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
    dataDirRoot = new Path(".kamu/data"),
    dataDirDeriv = new Path(".kamu/data")
  ).toAbsolute(fileSystem)

  val metadataRepository =
    new MetadataRepository(fileSystem, repositoryVolumeMap)

  try {
    cliOptions match {
      case Some(c) =>
        if (c.init.isDefined) {
          new InitCommand(fileSystem, repositoryVolumeMap).run()
        } else {
          // Other commands need to have repository initialized
          if (repositoryVolumeMap.allPaths.exists(!fileSystem.exists(_)))
            throw new UsageException("Not a kamu repository")

          if (c.list.isDefined) {
            new ListCommand(metadataRepository).run()
          } else if (c.add.isDefined && c.add.get.manifests.nonEmpty) {
            new AddManifestCommand(
              fileSystem,
              metadataRepository,
              c.add.get.manifests
            ).run()
          } else if (c.delete.isDefined) {
            new DeleteCommand(metadataRepository, c.delete.get.ids).run()
          } else if (c.pull.isDefined) {
            new PullCommand(
              fileSystem,
              repositoryVolumeMap,
              metadataRepository,
              getSparkRunner,
              c.pull.get.ids
            ).run()
          } else if (c.depgraph.isDefined) {
            new DependencyGraphCommand(metadataRepository).run()
          } else if (c.notebook.isDefined) {
            new NotebookCommand(fileSystem, repositoryVolumeMap).run()
          } else {
            println(cliParser.usage())
          }
        }
      case _ =>
    }

  } catch {
    case usg: UsageException =>
      Console.err.println(s"[ERROR] ${usg.getMessage}")
      sys.exit(1)
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  def getSparkRunner: SparkRunner = {
    if (cliOptions.get.useLocalSpark)
      new SparkRunnerLocal(fileSystem)
    else
      new SparkRunnerDocker(fileSystem)
  }
}
