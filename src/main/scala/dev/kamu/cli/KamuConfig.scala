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

case class KamuConfig(
  workspaceRoot: Path,
  spark: SparkConfig = SparkConfig()
) {
  def kamuRoot: Path = {
    workspaceRoot.resolve(KamuConfig.ROOT_DIR_NAME)
  }

  def localVolume: Path = {
    workspaceRoot.resolve(".kamu.local")
  }
}

object KamuConfig {
  val ROOT_DIR_NAME = ".kamu"

  def findWorkspaceRoot(fileSystem: FileSystem, dir: Path): Option[Path] = {
    findWorkspaceRootRec(fileSystem, fileSystem.toAbsolute(dir))
  }

  @scala.annotation.tailrec
  private def findWorkspaceRootRec(
    fileSystem: FileSystem,
    dir: Path
  ): Option[Path] = {
    if (fileSystem.exists(dir.resolve(ROOT_DIR_NAME))) {
      Some(dir)
    } else if (dir.isRoot) {
      None
    } else {
      findWorkspaceRootRec(fileSystem, dir.getParent)
    }
  }
}

case class SparkConfig(
  driverMemory: String = "2g"
)
