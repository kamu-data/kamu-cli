/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import java.io.OutputStream

import dev.kamu.cli.WorkspaceLayout
import dev.kamu.cli.external.DockerClient
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Level

trait Engine {
  def submit(
    workspaceLayout: WorkspaceLayout,
    extraFiles: Map[String, OutputStream => Unit] = Map.empty,
    extraMounts: Seq[Path] = Seq.empty,
    jars: Seq[Path] = Seq.empty
  ): Unit
}

class EngineFactory(fileSystem: FileSystem, logLevel: Level) {
  def getEngine(engineID: String): Engine = {
    engineID match {
      case "sparkIngest" =>
        new SparkEngineDocker(
          fileSystem,
          logLevel,
          new DockerClient(fileSystem),
          "dev.kamu.engine.spark.ingest.IngestApp"
        )
      case "sparkSQL" =>
        new SparkEngineDocker(
          fileSystem,
          logLevel,
          new DockerClient(fileSystem)
        )
      case "flink" =>
        new FlinkEngine(
          fileSystem,
          new DockerClient(fileSystem)
        )
      case _ => throw new NotImplementedError(s"Unsupported engine: $engineID")
    }
  }
}
