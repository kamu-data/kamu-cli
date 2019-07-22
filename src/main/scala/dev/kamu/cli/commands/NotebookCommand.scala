package dev.kamu.cli.commands

import dev.kamu.cli.NotebookRunnerDocker
import dev.kamu.core.manifests.RepositoryVolumeMap
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class NotebookCommand(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val runner = new NotebookRunnerDocker(fileSystem, repositoryVolumeMap)
    runner.start()
  }
}
