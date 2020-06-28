/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.nio.file.Path

import better.files.File

case class KamuConfig(
  workspaceRoot: Path
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

  def findWorkspaceRoot(dir: Path): Option[Path] = {
    findWorkspaceRootRec(dir.toAbsolutePath)
  }

  @scala.annotation.tailrec
  private def findWorkspaceRootRec(dir: Path): Option[Path] = {
    if (dir == null) {
      None
    } else if (File(dir.resolve(ROOT_DIR_NAME)).exists) {
      Some(dir)
    } else {
      findWorkspaceRootRec(dir.getParent)
    }
  }
}
