package dev.kamu.cli.commands

import dev.kamu.cli.MetadataRepository
import dev.kamu.cli.external.{DockerClient, NotebookRunnerDocker}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class NotebookCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  dockerClient: DockerClient,
  environmentVars: Map[String, String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    new NotebookRunnerDocker(
      fileSystem,
      dockerClient,
      metadataRepository,
      environmentVars
    ).start()
  }
}
