/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.external

import java.nio.file.{Files, Path, Paths}

import better.files.File
import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.core.manifests.{DatasetID, Remote, VolumeLayout}
import dev.kamu.core.utils.Temp
import org.apache.logging.log4j.LogManager

import scala.sys.process.Process

class RemoteOperatorS3Cli(
  metadataRepository: MetadataRepository,
  remote: Remote
) extends RemoteOperator {
  private val logger = LogManager.getLogger(getClass.getName)

  override def push(datasets: Seq[DatasetID]): Unit = {
    withVolumeLayout(datasets) { localDir =>
      s3Sync(localDir, Paths.get(remote.url))
    }
  }

  override def pull(datasets: Seq[DatasetID]): Unit = {
    val remoteVolumeLayout = VolumeLayout(
      metadataDir = Paths.get("datasets"),
      checkpointsDir = Paths.get("checkpoints"),
      dataDir = Paths.get("data"),
      cacheDir = Paths.get("cache")
    )

    for (id <- datasets) {
      // TODO: Do one sync instead since volume layouts should match
      val sourceLayout = remoteVolumeLayout.relativeTo(Paths.get(remote.url))
      val destLayout = metadataRepository.getDatasetLayout(id)

      s3Sync(
        sourceLayout.metadataDir.resolve(id.toString),
        destLayout.metadataDir
      )

      s3Sync(
        sourceLayout.checkpointsDir.resolve(id.toString),
        destLayout.checkpointsDir
      )

      s3Sync(
        sourceLayout.dataDir.resolve(id.toString),
        destLayout.dataDir
      )
    }
  }

  protected def withVolumeLayout[T](
    datasets: Seq[DatasetID]
  )(func: Path => T): T = {
    Temp.withRandomTempDir("kamu-volume-") { tempDir =>
      val targetLayout = VolumeLayout(
        metadataDir = Paths.get("datasets"),
        checkpointsDir = Paths.get("checkpoints"),
        dataDir = Paths.get("data"),
        cacheDir = Paths.get("cache")
      ).relativeTo(tempDir)

      targetLayout.allDirs.foreach(File(_).createDirectories())

      for (id <- datasets) {
        val datasetLayout = metadataRepository.getDatasetLayout(id)

        Files.createSymbolicLink(
          targetLayout.dataDir.resolve(id.toString),
          datasetLayout.dataDir
        )

        if (File(datasetLayout.checkpointsDir).exists)
          Files.createSymbolicLink(
            targetLayout.checkpointsDir.resolve(id.toString),
            datasetLayout.checkpointsDir
          )

        Files.createSymbolicLink(
          targetLayout.metadataDir.resolve(id.toString),
          datasetLayout.metadataDir
        )
      }

      func(tempDir)
    }
  }

  protected def s3Sync(from: Path, to: Path): Unit = {
    command(
      Array(
        "aws",
        "s3",
        "sync",
        from.toString.replace("file:", ""),
        to.toString.replace("file:", "")
      )
    )
  }

  protected def s3Copy(from: Path, to: Path): Unit = {
    command(
      Array(
        "aws",
        "s3",
        "cp",
        from.toString.replace("file:", ""),
        to.toString.replace("file:", "")
      )
    )
  }

  protected def command(cmd: Seq[String]): Unit = {
    logger.debug(s"Executing command: ${cmd.mkString(" ")}")
    if (Process(cmd).! != 0)
      throw new Exception(s"Command failed: ${cmd.mkString(" ")}")
  }
}
