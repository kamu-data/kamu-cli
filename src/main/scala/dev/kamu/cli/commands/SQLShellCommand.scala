package dev.kamu.cli.commands

import java.net.URI

import dev.kamu.cli.{RepositoryVolumeMap, SqlLineOptions}
import dev.kamu.cli.external.{IOHandlerPresets, LivyDockerProcessBuilder}
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import sqlline.SqlLine

import scala.concurrent.duration._

class SQLShellCommand(
  repositoryVolumeMap: RepositoryVolumeMap,
  url: Option[URI],
  command: Option[String],
  script: Option[Path],
  sqlLineOptions: SqlLineOptions
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresRepository: Boolean = url.isEmpty

  override def run(): Unit = {
    var args = Array(
      "--autoCommit=false",
      s"--color=${sqlLineOptions.color}",
      "-nn",
      "kamu"
    )

    if (command.isDefined)
      args ++= Seq("-e", command.get)
    else if (script.isDefined)
      args ++= Seq("-f", script.get.toUri.getPath)

    if (sqlLineOptions.incremental.isDefined)
      args ++= Seq(s"--incremental=${sqlLineOptions.incremental.get}")

    if (sqlLineOptions.showHeader.isDefined)
      args ++= Seq(s"--showHeader=${sqlLineOptions.showHeader.get}")

    if (sqlLineOptions.headerInterval.isDefined)
      args ++= Seq(s"--headerInterval=${sqlLineOptions.headerInterval.get}")

    if (sqlLineOptions.outputFormat.isDefined)
      args ++= Seq(s"--outputformat=${sqlLineOptions.outputFormat.get}")

    if (sqlLineOptions.csvDelimiter.isDefined)
      args ++= Seq(s"--csvDelimiter=${sqlLineOptions.csvDelimiter.get}")

    if (sqlLineOptions.csvQuoteCharacter.isDefined)
      args ++= Seq(
        s"--csvQuoteCharacter=${sqlLineOptions.csvQuoteCharacter.get}"
      )

    if (sqlLineOptions.nullValue.isDefined)
      args ++= Seq(s"--nullValue=${sqlLineOptions.nullValue.get}")

    if (sqlLineOptions.numberFormat.isDefined)
      args ++= Seq(s"--numberFormat=${sqlLineOptions.numberFormat.get}")

    if (sqlLineOptions.dateFormat.isDefined)
      args ++= Seq(s"--dateFormat=${sqlLineOptions.dateFormat.get}")

    if (sqlLineOptions.timeFormat.isDefined)
      args ++= Seq(s"--timeFormat=${sqlLineOptions.timeFormat.get}")

    if (sqlLineOptions.timestampFormat.isDefined)
      args ++= Seq(s"--timestampFormat=${sqlLineOptions.timestampFormat.get}")

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
      ).run(
        Some(
          IOHandlerPresets.redirectToLogger(logger, tag = "[livy] ")
        )
      )

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
