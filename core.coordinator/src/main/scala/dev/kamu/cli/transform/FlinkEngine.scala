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
import dev.kamu.cli.external.{DockerClient, DockerImages, DockerRunArgs}
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}

class FlinkEngine(
  fileSystem: FileSystem,
  dockerClient: DockerClient,
  image: String = DockerImages.FLINK
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

    try {
      submit(
        workspaceLayout,
        Seq(tmpJar) ++ jars,
        extraMounts
      )
    } finally {
      if (tmpJar != null)
        fileSystem.delete(tmpJar, false)
    }
  }

  protected def submit(
    workspaceLayout: WorkspaceLayout,
    jars: Seq[Path],
    extraMounts: Seq[Path]
  ): Unit = {
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
      "java",
      "-cp",
      "/opt/flink/lib/*:/opt/kamu/jars/*:/opt/kamu/engine.flink.jar",
      "dev.kamu.engine.flink.EngineApp"
    )

    logger.info("Starting Flink job")

    try {
      dockerClient.runShell(
        DockerRunArgs(
          image = image,
          volumeMap = workspaceVolumes ++ jarVolumes ++ extraVolumes
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

  protected def tempDir: Path = {
    new Path(sys.props("java.io.tmpdir"))
  }
}
