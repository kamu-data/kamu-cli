package dev.kamu.cli.commands

import dev.kamu.cli.RepositoryVolumeMap
import dev.kamu.cli.external.SQLShellRunnerDocker
import org.apache.log4j.LogManager

class SQLShellCommand(
  repositoryVolumeMap: RepositoryVolumeMap
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {
    new SQLShellRunnerDocker(repositoryVolumeMap).start()
  }
}
