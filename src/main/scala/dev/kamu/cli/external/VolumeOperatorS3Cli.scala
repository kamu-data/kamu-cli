/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.external

import dev.kamu.cli.{MetadataRepository, WorkspaceLayout}
import dev.kamu.core.manifests.{Dataset, Volume, VolumeLayout}
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process.Process

class VolumeOperatorS3Cli(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository,
  volume: Volume
) extends VolumeOperator {
  private val logger = LogManager.getLogger(getClass.getName)

  override def push(datasets: Seq[Dataset]): Unit = {
    withVolumeLayout(datasets) { localDir =>
      s3Sync(localDir, new Path(volume.url))
    }
  }

  override def pull(datasets: Seq[Dataset]): Unit = {
    val localVolumeLayout = metadataRepository.getLocalVolume()

    val remoteVolumeLayout = VolumeLayout(
      datasetsDir = new Path("datasets"),
      checkpointsDir = new Path("checkpoints"),
      dataDir = new Path("data"),
      cacheDir = new Path("cache")
    )

    for (ds <- datasets) {
      val sourceLayout = remoteVolumeLayout.relativeTo(new Path(volume.url))

      s3Copy(
        sourceLayout.datasetsDir.resolve(ds.id.toString + ".yaml"),
        localVolumeLayout.datasetsDir.resolve(ds.id.toString + ".yaml")
      )

      s3Sync(
        sourceLayout.checkpointsDir.resolve(ds.id.toString),
        localVolumeLayout.checkpointsDir.resolve(ds.id.toString)
      )

      s3Sync(
        sourceLayout.dataDir.resolve(ds.id.toString),
        localVolumeLayout.dataDir.resolve(ds.id.toString)
      )
    }
  }

  protected def withVolumeLayout[T](
    datasets: Seq[Dataset]
  )(func: Path => T): T = {
    Temp.withRandomTempDir(fileSystem, "kamu-volume-") { tempDir =>
      val localVolumeLayout =
        metadataRepository.getLocalVolume().toAbsolute(fileSystem)

      val targetLayout = VolumeLayout(
        datasetsDir = new Path("datasets"),
        checkpointsDir = new Path("checkpoints"),
        dataDir = new Path("data"),
        cacheDir = new Path("cache")
      ).relativeTo(tempDir)

      targetLayout.allDirs.foreach(fileSystem.mkdirs)

      for (ds <- datasets) {
        fileSystem.createSymlink(
          localVolumeLayout.dataDir.resolve(ds.id.toString),
          targetLayout.dataDir.resolve(ds.id.toString),
          false
        )

        fileSystem.createSymlink(
          localVolumeLayout.checkpointsDir.resolve(ds.id.toString),
          targetLayout.checkpointsDir.resolve(ds.id.toString),
          false
        )

        val dsManifestPath =
          if (fileSystem.exists(
                localVolumeLayout.datasetsDir.resolve(ds.id.toString + ".yaml")
              ))
            localVolumeLayout.datasetsDir.resolve(ds.id.toString + ".yaml")
          else
            workspaceLayout.datasetsDir.resolve(ds.id.toString + ".yaml")

        fileSystem.createSymlink(
          dsManifestPath,
          targetLayout.datasetsDir.resolve(ds.id.toString + ".yaml"),
          false
        )
      }

      func(tempDir)
    }
  }

  def s3Sync(from: Path, to: Path) = {
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

  def s3Copy(from: Path, to: Path) = {
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

  def command(cmd: Seq[String]) = {
    logger.debug(s"Executing command: ${cmd.mkString(" ")}")
    if (Process(cmd).! != 0)
      throw new Exception(s"Command failed: ${cmd.mkString(" ")}")
  }
}
