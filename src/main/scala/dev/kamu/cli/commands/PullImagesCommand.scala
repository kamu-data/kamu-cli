package dev.kamu.cli.commands

import dev.kamu.cli.external.{DockerClient, DockerImages}
import org.apache.log4j.LogManager

class PullImagesCommand(
  ) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresRepository: Boolean = false

  def run(): Unit = {
    val docker = new DockerClient()
    DockerImages.ALL.foreach(image => {
      logger.info(s"Pulling image: $image")
      docker.pull(image)
    })
  }
}
