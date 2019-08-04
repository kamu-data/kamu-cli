package dev.kamu.cli.commands

import dev.kamu.cli.RepositoryVolumeMap
import dev.kamu.cli.external.LivyDockerProcessBuilder
import org.apache.log4j.LogManager

class SQLServerCommand(
  repositoryVolumeMap: RepositoryVolumeMap,
  port: Option[Int]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {
    val containerPort = 10090

    val livyProcess = new LivyDockerProcessBuilder(
      repositoryVolumeMap = repositoryVolumeMap,
      exposePorts = if (port.isEmpty) List(containerPort) else List.empty,
      exposePortMap =
        if (port.isDefined) Map(containerPort -> port.get) else Map.empty
    ).run()

    // TODO: Avoid sleeping somehow
    Thread.sleep(1000)
    logger.info(
      s"URL: jdbc:hive2://localhost:${livyProcess.getHostPort(10090).get}"
    )

    try {
      livyProcess.join()
    } finally {
      livyProcess.kill()
    }
  }
}
