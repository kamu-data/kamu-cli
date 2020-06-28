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

import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.cli.WorkspaceLayout
import dev.kamu.cli.external.DockerImages
import dev.kamu.core.manifests.{FetchSourceKind, Manifest}
import dev.kamu.core.manifests.infra.{
  ExecuteQueryRequest,
  ExecuteQueryResult,
  IngestRequest,
  IngestResult
}
import dev.kamu.core.utils.{DockerClient, DockerRunArgs}
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.Temp
import org.apache.commons.io.IOUtils
import org.apache.logging.log4j.{Level, LogManager, Logger}

class SparkEngine(
  workspaceLayout: WorkspaceLayout,
  logLevel: Level,
  dockerClient: DockerClient,
  image: String = DockerImages.SPARK
) extends Engine {
  protected var logger: Logger = LogManager.getLogger(getClass.getName)

  override def ingest(request: IngestRequest): IngestResult = {
    Temp.withRandomTempDir(
      "kamu-inout-"
    ) { inOutDir =>
      yaml.save(Manifest(request), inOutDir / "request.yaml")

      // TODO: Account for missing files
      val extraMounts = request.source.fetch match {
        case furl: FetchSourceKind.Url =>
          furl.url.getScheme match {
            case "file" | null => List(Paths.get(furl.url))
            case _             => List.empty
          }
        case glob: FetchSourceKind.FilesGlob =>
          List(glob.path.getParent)
        case _ =>
          throw new RuntimeException(
            s"Unsupported fetch kind: ${request.source.fetch}"
          )
      }

      submit("dev.kamu.engine.spark.ingest.IngestApp", inOutDir, extraMounts)

      yaml.load[Manifest[IngestResult]](inOutDir / "result.yaml").content
    }
  }

  override def executeQuery(
    request: ExecuteQueryRequest
  ): ExecuteQueryResult = {
    Temp.withRandomTempDir(
      "kamu-inout-"
    ) { inOutDir =>
      yaml.save(Manifest(request), inOutDir / "request.yaml")

      submit(
        "dev.kamu.engine.spark.transform.TransformApp",
        inOutDir,
        Seq.empty
      )

      yaml.load[Manifest[ExecuteQueryResult]](inOutDir / "result.yaml").content
    }
  }

  protected def submit(
    appClass: String,
    inOutDir: Path,
    extraMounts: Seq[Path]
  ): Unit = {
    val inOutDirInContainer = Paths.get("/opt/engine/in-out")
    val engineJarInContainer = Paths.get("/opt/engine/bin/engine.spark.jar")

    Temp.withTempFile(
      "kamu-logging-cfg-",
      writeLog4jConfig
    ) { loggingConfigPath =>
      val workspaceVolumes =
        Seq(workspaceLayout.kamuRootDir, workspaceLayout.localVolumeDir)
          .filter(p => File(p).exists)
          .map(p => (p, p))
          .toMap

      val appVolumes = Map(
        loggingConfigPath -> Paths.get("/opt/spark/conf/log4j.properties"),
        inOutDir -> inOutDirInContainer
      )

      val extraVolumes = extraMounts.map(p => (p, p)).toMap

      val submitArgs = List(
        "/opt/spark/bin/spark-submit",
        "--master=local[4]",
        s"--driver-memory=2g",
        "--conf",
        "spark.sql.warehouse.dir=/opt/spark-warehouse",
        s"--class=$appClass",
        engineJarInContainer.toUri.getPath
      )

      logger.info("Starting Spark job")

      try {
        dockerClient.runShell(
          DockerRunArgs(
            image = image,
            volumeMap = workspaceVolumes ++ appVolumes ++ extraVolumes
          ),
          submitArgs
        )
      } finally {
        // TODO: avoid this by setting up correct user inside the container
        logger.debug("Fixing file ownership")

        val unix = new com.sun.security.auth.module.UnixSystem()
        val chownArgs = Seq(
          "chown",
          "-R",
          s"${unix.getUid}:${unix.getGid}"
        ) ++ workspaceVolumes.values.map(_.toUri.getPath)

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
