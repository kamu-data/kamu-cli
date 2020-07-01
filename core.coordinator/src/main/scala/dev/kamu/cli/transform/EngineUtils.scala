/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import java.nio.file.{Path, Paths}

import dev.kamu.core.utils.OS

trait EngineUtils {
  /////////////////////////////////////////////////////////////////////////////
  // Path mappings between host and container
  /////////////////////////////////////////////////////////////////////////////

  protected val volumeDirInContainer: String = "/opt/engine/volume"
  protected val inOutDirInContainer: String = "/opt/engine/in-out"

  protected def isSubPathOf(p: Path, parent: Path): Boolean = {
    var pp = p.getParent
    while (pp != null) {
      if (pp == parent)
        return true
      pp = pp.getParent
    }
    false
  }

  protected def toContainerPath(ps: String, volumePath: Path): String = {
    val p = Paths.get(ps).normalize().toAbsolutePath
    val rel = volumePath.relativize(p)
    val x = if (!OS.isWindows) {
      Paths.get(volumeDirInContainer).resolve(rel).toString
    } else {
      volumeDirInContainer + "/" + rel.toString.replace("\\", "/")
    }
    println(s"Mapped path $ps to $x")
    x
  }
}
