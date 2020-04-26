/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.external

import dev.kamu.cli.WorkspaceLayout
import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.core.manifests.{DatasetID, Remote}
import org.apache.hadoop.fs.FileSystem

trait RemoteOperator {
  def push(datasets: Seq[DatasetID])

  def pull(datasets: Seq[DatasetID])
}

class RemoteOperatorFactory(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository
) {
  def ensureSupported(remote: Remote): Unit = {
    remote.url.getScheme match {
      case "s3" =>
      case _ =>
        throw new NotImplementedError(
          s"No remote operator found that supports scheme: ${remote.url.getScheme}"
        )
    }
  }

  def buildFor(remote: Remote): RemoteOperator = {
    remote.url.getScheme match {
      case "s3" =>
        new RemoteOperatorS3Cli(
          fileSystem,
          metadataRepository,
          remote
        )
      case _ =>
        throw new NotImplementedError(
          s"No remote operator found that supports scheme: ${remote.url.getScheme}"
        )
    }
  }
}
