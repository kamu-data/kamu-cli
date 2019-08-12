package dev.kamu.cli.commands

import dev.kamu.cli.RepositoryVolumeMap
import dev.kamu.cli.external.NotebookRunnerDocker
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class NotebookCommand(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap,
  environmentVars: Map[String, String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    new NotebookRunnerDocker(fileSystem, repositoryVolumeMap, environmentVars)
      .start()
  }
}
