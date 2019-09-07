package dev.kamu.cli.commands

import dev.kamu.cli.external.{DockerClient, DockerImages}
import org.apache.log4j.LogManager

class PullImagesCommand(
  dockerClient: DockerClient
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresRepository: Boolean = false

  def run(): Unit = {
    DockerImages.ALL.foreach(image => {
      logger.info(s"Pulling image: $image")
      dockerClient.pull(image)
    })
  }
}
