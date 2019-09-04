package dev.kamu.cli.external

import dev.kamu.cli.{RepositoryVolumeMap, SparkConfig}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Level

class SparkRunnerDocker(
  fileSystem: FileSystem,
  logLevel: Level,
  sparkConfig: SparkConfig,
  image: String = DockerImages.SPARK
) extends SparkRunner(fileSystem, logLevel) {

  protected override def submit(
    repo: RepositoryVolumeMap,
    appClass: String,
    jars: Seq[Path],
    extraMounts: Seq[Path],
    loggingConfig: Path
  ): Unit = {
    val assemblyPathInContainer = new Path("/opt/kamu/kamu")

    val appVolumes = Map(
      assemblyPath -> assemblyPathInContainer,
      loggingConfig -> new Path("/opt/spark/conf/log4j.properties")
    )

    val extraVolumes = extraMounts.map(p => (p, p)).toMap

    val jarVolumes = jars
      .map(p => (p, new Path("/opt/kamu/jars/" + p.getName)))
      .toMap

    val repoVolumes = repo.allPaths
      .map(p => (p, p))
      .toMap

    val submitArgs = List(
      "/opt/spark/bin/spark-submit",
      "--master=local[4]",
      s"--driver-memory=${sparkConfig.driverMemory}",
      "--conf",
      "spark.sql.warehouse.dir=/opt/spark-warehouse",
      s"--class=$appClass"
    ) ++ (
      if (jars.nonEmpty)
        Seq("--jars=" + jarVolumes.values.map(_.toUri.getPath).mkString(","))
      else
        Seq()
    ) ++ Seq(
      assemblyPathInContainer.toUri.getPath
    )

    logger.info("Starting Spark job")

    val dockerClient = new DockerClient()
    try {
      dockerClient.runShell(
        DockerRunArgs(
          image = image,
          volumeMap = appVolumes ++ repoVolumes ++ jarVolumes ++ extraVolumes
        ),
        submitArgs
      )
    } finally {
      // TODO: avoid this by setting up correct user inside the container
      logger.debug("Fixing file ownership")

      val unix = new com.sun.security.auth.module.UnixSystem()
      val chownArgs = Seq(
        "chown",
        "-R",
        s"${unix.getUid}:${unix.getGid}"
      ) ++ repoVolumes.values.map(_.toUri.getPath)

      dockerClient.runShell(
        DockerRunArgs(
          image = image,
          volumeMap = repoVolumes
        ),
        chownArgs
      )
    }
  }
}
