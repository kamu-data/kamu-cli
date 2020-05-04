/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import dev.kamu.cli.external.DockerClient
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Level

class EngineFactory(fileSystem: FileSystem, logLevel: Level) {
  def getEngine(): Engine = {
    new EngineDocker(
      fileSystem,
      logLevel,
      new DockerClient(fileSystem)
    )
  }
}
