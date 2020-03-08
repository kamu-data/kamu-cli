/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.external

import dev.kamu.cli.{MetadataRepository, WorkspaceLayout}
import dev.kamu.core.manifests.{DatasetID, Volume}
import org.apache.hadoop.fs.FileSystem

trait VolumeOperator {
  def push(datasets: Seq[DatasetID])

  def pull(datasets: Seq[DatasetID])
}

class VolumeOperatorFactory(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository
) {
  def ensureSupported(volume: Volume): Unit = {
    volume.url.getScheme match {
      case "s3" =>
      case _ =>
        throw new NotImplementedError(
          s"No volume operator found that support scheme: ${volume.url.getScheme}"
        )
    }
  }

  def buildFor(volume: Volume): VolumeOperator = {
    volume.url.getScheme match {
      case "s3" =>
        new VolumeOperatorS3Cli(
          fileSystem,
          metadataRepository,
          volume
        )
      case _ =>
        throw new NotImplementedError(
          s"No volume operator found that support scheme: ${volume.url.getScheme}"
        )
    }
  }
}
