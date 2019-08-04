package dev.kamu.cli.external

import dev.kamu.cli.RepositoryVolumeMap
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
    val assemblyPathInContainer = new Path("/opt/kamu/kamu")

    val appVolumes = Map(
      assemblyPath -> assemblyPathInContainer,
      loggingConfig -> new Path("/opt/spark/conf/log4j.properties")
    )

    val jarVolumes = jars
      .map(p => (p, new Path("/opt/kamu/jars/" + p.getName)))
      .toMap

    val repoVolumes = repo.allPaths
      .map(p => (p, p))
      .toMap

    val submitArgs = List(
      "/opt/spark/bin/spark-submit",
      "--master=local[4]",
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
    dockerClient.runShell(
      DockerRunArgs(
        image = image,
        volumeMap = appVolumes ++ repoVolumes ++ jarVolumes
      ),
      submitArgs
    )

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
