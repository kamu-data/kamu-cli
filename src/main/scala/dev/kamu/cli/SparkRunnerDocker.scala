package dev.kamu.cli

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Level

class SparkRunnerDocker(
  fileSystem: FileSystem,
  logLevel: Level,
  image: String = "kamu/spark:2.4.0_0.0.1"
) extends SparkRunner(fileSystem, logLevel) {

  protected override def submit(
    repo: RepositoryVolumeMap,
    appClass: String,
    jars: Seq[Path],
    loggingConfig: Path
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
      "-v",
      s"${assemblyPath.toUri.getPath}:$assemblyPathInContainer",
      "-v",
      s"${loggingConfig.toUri.getPath}:/opt/spark/conf/log4j.properties"
    ) ++ jarVolumes ++ repoVolumes

    val submitArgs = List(
      "/opt/spark/bin/spark-submit",
      "--master=local[4]",
      "--conf",
      "spark.sql.warehouse.dir=/opt/spark-warehouse",
      s"--class=$appClass"
    ) ++ (
      if (jars.nonEmpty)
        Seq("--jars=" + jarsInContainer.map(_._2).mkString(","))
      else
        Seq()
    ) ++ Seq(
      assemblyPathInContainer
    )

    logger.info("Starting Spark job")

    val dockerClient = new DockerClient()
    dockerClient.runShell(
      image = image,
      shellCommand = submitArgs,
      extraArgs = dockerArgs
    )

    // TODO: avoid this by setting up correct user inside the container
    logger.debug("Fixing file ownership")

    val unix = new com.sun.security.auth.module.UnixSystem()
    val chownArgs = Seq(
      "chown",
      "-R",
      s"${unix.getUid}:${unix.getGid}"
    ) ++ repo.allPaths.map(
      p => p.toUri.getPath
    )

    dockerClient.runShell(
      image = image,
      shellCommand = chownArgs,
      extraArgs = dockerArgs
    )
  }
}
