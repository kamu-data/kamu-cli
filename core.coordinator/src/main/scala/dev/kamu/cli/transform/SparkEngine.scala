/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import java.io.OutputStream
import java.util.zip.{ZipEntry, ZipOutputStream}

import dev.kamu.cli.WorkspaceLayout
import dev.kamu.cli.external.DockerImages
import dev.kamu.core.utils.{DockerClient, DockerRunArgs}
import dev.kamu.core.utils.fs._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, LogManager, Logger}

abstract class SparkEngineBase(
  fileSystem: FileSystem,
  logLevel: Level
) extends Engine {
  protected var logger: Logger = LogManager.getLogger(getClass.getName)

  def submit(
    workspaceLayout: WorkspaceLayout,
    extraFiles: Map[String, OutputStream => Unit] = Map.empty,
    extraMounts: Seq[Path] = Seq.empty,
    jars: Seq[Path] = Seq.empty
  ): Unit = {
    val tmpJar =
      if (extraFiles.nonEmpty)
        prepareJar(extraFiles)
      else
        null

    val loggingConfig = prepareLog4jConfig()

    try {
      submit(
        workspaceLayout,
        Seq(tmpJar) ++ jars,
        extraMounts,
        loggingConfig
      )
    } finally {
      if (tmpJar != null)
        fileSystem.delete(tmpJar, false)

      fileSystem.delete(loggingConfig, false)
    }
  }

  protected def submit(
    workspaceLayout: WorkspaceLayout,
    jars: Seq[Path],
    extraMounts: Seq[Path],
    loggingConfig: Path
  )

  protected def prepareJar(files: Map[String, OutputStream => Unit]): Path = {
    val jarPath = tempDir.resolve("kamu-configs.jar")

    logger.debug(s"Writing temporary JAR to: $jarPath")

    val fileStream = fileSystem.create(jarPath, true)
    val zipStream = new ZipOutputStream(fileStream)

    files.foreach {
      case (name, writeFun) =>
        zipStream.putNextEntry(new ZipEntry(name))
        writeFun(zipStream)
        zipStream.closeEntry()
    }

    zipStream.close()
    jarPath
  }

  protected def prepareLog4jConfig(): Path = {
    val path = tempDir.resolve("kamu-spark-log4j.properties")

    val resourceName = logLevel match {
      case Level.ALL | Level.TRACE | Level.DEBUG | Level.INFO =>
        "spark.info.log4j.properties"
      case Level.WARN | Level.ERROR =>
        "spark.warn.log4j.properties"
      case _ =>
        "spark.info.log4j.properties"
    }

    val configStream = getClass.getClassLoader.getResourceAsStream(resourceName)
    val outputStream = fileSystem.create(path, true)

    IOUtils.copy(configStream, outputStream)

    outputStream.close()
    configStream.close()

    path
  }

  protected def tempDir: Path = {
    new Path(sys.props("java.io.tmpdir"))
  }
}

class SparkEngineDocker(
  fileSystem: FileSystem,
  logLevel: Level,
  dockerClient: DockerClient,
  appClass: String = "dev.kamu.engine.spark.transform.TransformApp",
  image: String = DockerImages.SPARK
) extends SparkEngineBase(fileSystem, logLevel) {

  protected override def submit(
    workspaceLayout: WorkspaceLayout,
    jars: Seq[Path],
    extraMounts: Seq[Path],
    loggingConfig: Path
  ): Unit = {
    val appVolumes = Map(
      loggingConfig -> new Path("/opt/spark/conf/log4j.properties")
    )

    val extraVolumes = extraMounts.map(p => (p, p)).toMap

    val jarVolumes = jars
      .map(p => (p, new Path("/opt/kamu/jars/" + p.getName)))
      .toMap

    val workspaceVolumes =
      Seq(workspaceLayout.kamuRootDir, workspaceLayout.localVolumeDir)
        .filter(fileSystem.exists)
        .map(p => (p, p))
        .toMap

    val submitArgs = List(
      "/opt/spark/bin/spark-submit",
      "--master=local[4]",
      s"--driver-memory=2g",
      "--conf",
      "spark.sql.warehouse.dir=/opt/spark-warehouse",
      s"--class=$appClass"
    ) ++ (
      if (jars.nonEmpty)
        Seq("--jars=" + jarVolumes.values.map(_.toUri.getPath).mkString(","))
      else
        Seq()
    ) ++ Seq(
      "/opt/kamu/engine.spark.jar"
    )

    logger.info("Starting Spark job")

    try {
      dockerClient.runShell(
        DockerRunArgs(
          image = image,
          volumeMap = appVolumes ++ workspaceVolumes ++ jarVolumes ++ extraVolumes
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
      ) ++ workspaceVolumes.values.map(_.toUri.getPath)

      dockerClient.runShell(
        DockerRunArgs(
          image = image,
          volumeMap = workspaceVolumes
        ),
        chownArgs
      )
    }
  }
}
