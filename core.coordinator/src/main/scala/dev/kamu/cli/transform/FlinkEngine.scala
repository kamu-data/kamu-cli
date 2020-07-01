/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import java.nio.file.{Path, Paths}

import better.files.File
import dev.kamu.cli.WorkspaceLayout

import scala.concurrent.duration._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.cli.external.DockerImages
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.infra.{
  ExecuteQueryRequest,
  ExecuteQueryResult,
  IngestRequest,
  IngestResult
}
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{
  DockerClient,
  DockerProcessBuilder,
  DockerRunArgs,
  ExecArgs,
  IOHandlerPresets,
  OS,
  Temp
}
import org.slf4j.LoggerFactory

class FlinkEngine(
  workspaceLayout: WorkspaceLayout,
  dockerClient: DockerClient,
  image: String = DockerImages.FLINK,
  networkName: String = "kamu-flink"
) extends Engine
    with EngineUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  private val engineJarInContainer =
    Paths.get("/opt/engine/bin/engine.flink.jar")

  override def ingest(request: IngestRequest): IngestResult = {
    throw new NotImplementedError()
  }

  override def executeQuery(
    request: ExecuteQueryRequest
  ): ExecuteQueryResult = {
    val workspaceVolumes = Map(
      workspaceLayout.localVolumeDir -> Paths.get(volumeDirInContainer)
    )

    Temp.withRandomTempDir(
      "kamu-inout-"
    ) { inOutDir =>
      val newRequest =
        request.copy(
          checkpointsDir = toContainerPath(
            request.checkpointsDir,
            workspaceLayout.localVolumeDir
          ),
          dataDirs = request.dataDirs.map {
            case (k, v) =>
              (k, toContainerPath(v, workspaceLayout.localVolumeDir))
          }
        )

      yaml.save(Manifest(newRequest), inOutDir / "request.yaml")

      dockerClient.withNetwork(networkName) {

        logger.info("Starting Flink job")

        val jobManager = new DockerProcessBuilder(
          "jobmanager",
          dockerClient,
          DockerRunArgs(
            image = image,
            containerName = Some("jobmanager"),
            hostname = Some("jobmanager"),
            args = List("jobmanager"),
            environmentVars = Map("JOB_MANAGER_RPC_ADDRESS" -> "jobmanager"),
            exposePorts = List(6123, 8081),
            network = Some(networkName),
            volumeMap = Map(
              inOutDir -> Paths.get(inOutDirInContainer)
            ) ++ workspaceVolumes
          )
        ).run(Some(IOHandlerPresets.redirectOutputTagged("jobmanager: ")))

        val taskManager = new DockerProcessBuilder(
          "taskmanager",
          dockerClient,
          DockerRunArgs(
            image = image,
            containerName = Some("taskmanager"),
            hostname = Some("taskmanager"),
            args = List("taskmanager"),
            environmentVars = Map("JOB_MANAGER_RPC_ADDRESS" -> "jobmanager"),
            exposePorts = List(6121, 6122),
            network = Some(networkName),
            volumeMap = workspaceVolumes
          )
        ).run(Some(IOHandlerPresets.redirectOutputTagged("taskmanager: ")))

        jobManager.waitForHostPort(8081, 15 seconds)

        val prevSavepoint = getPrevSavepoint(request)
        val savepointArgs = prevSavepoint
          .map(p => toContainerPath(p.toString, workspaceLayout.localVolumeDir))
          .map(p => s"-s $p")
          .getOrElse("")

        try {
          val exitCode = dockerClient
            .exec(
              ExecArgs(),
              jobManager.containerName,
              Seq(
                "bash",
                "-c",
                s"flink run $savepointArgs $engineJarInContainer"
              )
            )
            .!

          if (exitCode != 0)
            throw new RuntimeException(
              s"Engine run failed with exit code: $exitCode"
            )

          commitSavepoint(prevSavepoint)

        } finally {
          if (!OS.isWindows) {
            logger.debug("Fixing file ownership")

            dockerClient
              .exec(
                ExecArgs(),
                jobManager.containerName,
                Seq(
                  "bash",
                  "-c",
                  s"chown -R ${OS.uid}:${OS.gid} $volumeDirInContainer"
                )
              )
              .!
          }

          taskManager.stop()
          jobManager.stop()

          taskManager.join()
          jobManager.join()
        }
      }

      yaml.load[Manifest[ExecuteQueryResult]](inOutDir / "result.yaml").content
    }
  }

  protected def getPrevSavepoint(request: ExecuteQueryRequest): Option[Path] = {
    val checkpointsDir = File(request.checkpointsDir)

    if (!checkpointsDir.exists)
      return None

    val allSavepoints = checkpointsDir.list
      .filter(_.isDirectory)
      .toList

    // TODO: Atomicity
    if (allSavepoints.length > 1)
      throw new RuntimeException(
        "Multiple checkpoints found: " + allSavepoints.mkString(", ")
      )

    logger.debug("Using savepoint: {}", allSavepoints.headOption)

    allSavepoints.map(_.path).headOption
  }

  // TODO: Atomicity
  protected def commitSavepoint(oldSavepoint: Option[Path]): Unit = {
    if (oldSavepoint.isEmpty)
      return

    logger.debug("Deleting savepoint: {}", oldSavepoint)

    oldSavepoint.foreach(p => File(p).delete())
  }
}
