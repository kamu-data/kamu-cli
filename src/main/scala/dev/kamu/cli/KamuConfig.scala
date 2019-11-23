/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import org.apache.hadoop.fs.Path
import dev.kamu.core.utils.fs._

case class KamuConfig(
  workspaceRoot: Path = new Path("."),
  spark: SparkConfig = SparkConfig()
) {
  def kamuRoot: Path = {
    workspaceRoot.resolve(".kamu")
  }

  def localVolume: Path = {
    workspaceRoot.resolve(".kamu.local")
  }
}

case class SparkConfig(
  driverMemory: String = "2g"
)
