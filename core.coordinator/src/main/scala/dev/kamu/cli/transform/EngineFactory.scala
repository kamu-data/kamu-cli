/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import dev.kamu.cli.WorkspaceLayout
import dev.kamu.core.manifests.infra.{
  ExecuteQueryRequest,
  ExecuteQueryResult,
  IngestRequest,
  IngestResult
}
import dev.kamu.core.utils.DockerClient
import org.apache.logging.log4j.Level

trait Engine {
  def ingest(request: IngestRequest): IngestResult
  def executeQuery(request: ExecuteQueryRequest): ExecuteQueryResult
}

class EngineFactory(
  workspaceLayout: WorkspaceLayout,
  logLevel: Level
) {
  def getEngine(engineID: String): Engine = {
    engineID match {
      case "sparkSQL" =>
        new SparkEngine(
          workspaceLayout,
          logLevel,
          new DockerClient()
        )
      case "flink" =>
        new FlinkEngine(
          workspaceLayout,
          new DockerClient()
        )
      case _ => throw new NotImplementedError(s"Unsupported engine: $engineID")
    }
  }
}
