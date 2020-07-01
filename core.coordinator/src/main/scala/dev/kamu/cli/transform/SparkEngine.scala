/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import java.io.OutputStream
import java.nio.file.{Path, Paths}

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.cli.WorkspaceLayout
import dev.kamu.cli.external.DockerImages
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.infra.{
  ExecuteQueryRequest,
  ExecuteQueryResult,
  IngestRequest,
  IngestResult
}
import dev.kamu.core.utils.{DockerClient, DockerRunArgs, OS, Temp}
import dev.kamu.core.utils.fs._
import org.apache.commons.io.IOUtils
import org.apache.logging.log4j.{Level, LogManager, Logger}

class SparkEngine(
  workspaceLayout: WorkspaceLayout,
  logLevel: Level,
  dockerClient: DockerClient,
  image: String = DockerImages.SPARK
) extends Engine
    with EngineUtils {
  protected val logger: Logger = LogManager.getLogger(getClass.getName)

  override def ingest(request: IngestRequest): IngestResult = {
    Temp.withRandomTempDir(
      "kamu-inout-"
    ) { inOutDir =>
      val newRequest = request.copy(
        ingestPath = toContainerPath(
          request.ingestPath,
          workspaceLayout.localVolumeDir
        ),
        dataDir = toContainerPath(
          request.dataDir,
          workspaceLayout.localVolumeDir
        )
      )

      yaml.save(Manifest(newRequest), inOutDir / "request.yaml")

      submit("dev.kamu.engine.spark.ingest.IngestApp", inOutDir)

      yaml.load[Manifest[IngestResult]](inOutDir / "result.yaml").content
    }
  }

  override def executeQuery(
    request: ExecuteQueryRequest
  ): ExecuteQueryResult = {
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

      submit("dev.kamu.engine.spark.transform.TransformApp", inOutDir)

      yaml.load[Manifest[ExecuteQueryResult]](inOutDir / "result.yaml").content
    }
  }

  protected def submit(appClass: String, inOutDir: Path): Unit = {
    Temp.withTempFile(
      "kamu-logging-cfg-",
      writeLog4jConfig
    ) { loggingConfigPath =>
      val workspaceVolumes =
        Map(workspaceLayout.localVolumeDir -> Paths.get(volumeDirInContainer))

      val appVolumes = Map(
        loggingConfigPath -> Paths.get("/opt/spark/conf/log4j.properties"),
        inOutDir -> Paths.get(inOutDirInContainer)
      )

      val submitArgs = List(
        "/opt/spark/bin/spark-submit",
        "--master=local[4]",
        s"--driver-memory=2g",
        "--conf",
        "spark.sql.warehouse.dir=/opt/spark-warehouse",
        s"--class=$appClass",
        "/opt/engine/bin/engine.spark.jar"
      )

      logger.info("Starting Spark job")

      try {
        dockerClient.runShell(
          DockerRunArgs(
            image = image,
            volumeMap = workspaceVolumes ++ appVolumes
          ),
          submitArgs
        )
      } finally {
        if (!OS.isWindows) {
          // TODO: avoid this by setting up correct user inside the container
          logger.debug("Fixing file ownership")

          val chownArgs = Seq(
            "chown",
            "-R",
            s"${OS.uid}:${OS.gid}",
            volumeDirInContainer
          )

          dockerClient.runShell(
            DockerRunArgs(
              image = image,
              volumeMap = workspaceVolumes
            ),
            chownArgs
          )
        }
      }
    }
  }

  protected def writeLog4jConfig(outputStream: OutputStream): Unit = {
    val resourceName = logLevel match {
      case Level.ALL | Level.TRACE | Level.DEBUG | Level.INFO =>
        "spark.info.log4j.properties"
      case Level.WARN | Level.ERROR =>
        "spark.warn.log4j.properties"
      case _ =>
        "spark.info.log4j.properties"
    }

    val configStream = getClass.getClassLoader.getResourceAsStream(resourceName)
    IOUtils.copy(configStream, outputStream)

    outputStream.close()
    configStream.close()
  }

}
