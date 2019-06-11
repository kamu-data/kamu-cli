package dev.kamu.cli

import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.RepositoryVolumeMap
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process.Process

class SparkRunnerDocker(
  fileSystem: FileSystem,
  image: String = "kamu/spark:2.4.0_0.0.1"
) extends SparkRunner {
  protected val logger = LogManager.getLogger(getClass.getName)

  override def submit(
    repo: RepositoryVolumeMap,
    appClass: String,
    jars: Seq[Path]
  ): Unit = {
    val assemblyPathInContainer = "/opt/kamu/kamu"

    val jarsInContainer =
      jars
        .map(p => Tuple2(p.toUri.getPath, "/opt/kamu/jars/" + p.getName))
        .toList

    val jarVolumes = jarsInContainer.flatMap(t => Seq("-v", t._1 + ":" + t._2))

    val repoVolumes = repo.allPaths
      .map(p => p.toUri.getPath)
      .flatMap(p => Seq("-v", s"$p:$p"))

    val dockerArgs = Seq(
      "docker",
      "run",
      "--rm",
      "-t",
      "--entrypoint",
      "bash",
      "-v",
      s"${assemblyPath.toUri.getPath}:$assemblyPathInContainer"
    ) ++ jarVolumes ++ repoVolumes ++ Seq(
      image
    )

    val submitArgs = List(
      "/opt/spark/bin/spark-submit",
      "--master=local[4]",
      "--conf",
      "spark.sql.warehouse.dir=/opt/spark-warehouse",
      s"--class=$appClass",
      "--jars=" + jarsInContainer.map(_._2).mkString(","),
      assemblyPathInContainer
    )

    logger.info("Starting Spark job")
    runDocker(dockerArgs, submitArgs)

    // TODO: avoid this by setting up correct user inside the container
    logger.debug("Fixing file ownership")
    val unix = new com.sun.security.auth.module.UnixSystem()
    runDocker(
      dockerArgs,
      Seq(
        "chown",
        "-R",
        s"${unix.getUid}:${unix.getGid}"
      ) ++ repo.allPaths.map(
        p => p.toUri.getPath
      )
    )
  }

  def runDocker(dockerArgs: Seq[String], shellCommand: Seq[String]): Unit = {
    val cmd = dockerArgs ++ Seq("-c", shellCommand.mkString(" "))

    logger.debug("Docker cmd: " + cmd.mkString(" "))

    val process = Process(cmd)

    val exitCode = process.!

    if (exitCode != 0)
      throw new RuntimeException(
        s"Command failed with exit code $exitCode: ${cmd.mkString(" ")}"
      )
  }
}
