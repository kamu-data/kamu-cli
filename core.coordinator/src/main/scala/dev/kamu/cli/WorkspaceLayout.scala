/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import org.apache.hadoop.fs.{FileSystem, Path}
import dev.kamu.core.utils.fs._

/** Describes the layout of the workspace on disk */
case class WorkspaceLayout(
  /** Root directory of the workspace */
  kamuRootDir: Path,
  /** Contains dataset metadata */
  metadataDir: Path,
  /** Contains remote definitions */
  remotesDir: Path,
  /** Root directory of a local storage volume */
  localVolumeDir: Path
) {

  def toAbsolute(fs: FileSystem): WorkspaceLayout = {
    copy(
      kamuRootDir = fs.toAbsolute(kamuRootDir),
      metadataDir = fs.toAbsolute(metadataDir),
      remotesDir = fs.toAbsolute(remotesDir),
      localVolumeDir = fs.toAbsolute(localVolumeDir)
    )
  }

}

object WorkspaceLayout {
  val GITIGNORE_CONTENT: String =
    """
      |""".stripMargin

  val LOCAL_VOLUME_GITIGNORE_CONTENT: String =
    """
      |*
      |""".stripMargin
}
