package dev.kamu.cli.commands

import java.net.URI

import dev.kamu.cli.RepositoryVolumeMap
import dev.kamu.cli.external.{IOHandlerPresets, LivyDockerProcessBuilder}
import org.apache.log4j.LogManager
import sqlline.SqlLine
import scala.concurrent.duration._

class SQLShellCommand(
  repositoryVolumeMap: RepositoryVolumeMap,
  url: Option[URI],
  command: Option[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresRepository: Boolean = url.isEmpty

  override def run(): Unit = {
    var args = Array(
      "--autoCommit=false",
      "--color=true",
      "-nn",
      "kamu"
    )

    if (command.isDefined)
      args ++= Seq("-e", command.get)

    maybeRunServer { url =>
      args ++= Seq("-u", url.toString)

      logger.debug("Starting sqlline: " + args.mkString(" "))
      new SqlLine().begin(args, null, true)
    }
  }

  def maybeRunServer[T](body: URI => T): T = {
    if (url.isDefined) {
      // Server is already running
      body(url.get)
    } else {
      // Start Livy container
      val containerPort = 10090

      val livyProcess = new LivyDockerProcessBuilder(
        repositoryVolumeMap = repositoryVolumeMap,
        exposePorts = List(containerPort)
      ).run(Some(IOHandlerPresets.blackHoled()))

      try {
        val hostPort = livyProcess.waitForHostPort(containerPort, 15 seconds)
        val livyUrl = URI.create(s"jdbc:hive2://localhost:$hostPort")
        logger.debug(s"Resolved Livy URL: $livyUrl")

        body(livyUrl)
      } finally {
        livyProcess.kill()
      }
    }
  }
}
