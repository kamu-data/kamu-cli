/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.external.SparkRunner
import dev.kamu.cli.{MetadataRepository, WorkspaceLayout}
import dev.kamu.core.manifests.{Dataset, DatasetID, ExternalSourceKind}
import dev.kamu.core.ingest.polling
import dev.kamu.core.transform.streaming
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.utils.fs._
import yaml.defaults._
import pureconfig.generic.auto._

class PullCommand(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository,
  sparkRunner: SparkRunner,
  ids: Seq[String],
  all: Boolean,
  recursive: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val datasetIDs = {
      if (all) metadataRepository.getAllDatasetIDs()
      else ids.map(DatasetID)
    }

    val plan = metadataRepository
      .getDatasetsInDependencyOrder(
        datasetIDs,
        recursive || all // All implies recursive, which is more efficient
      )

    logger.debug(s"Pulling datasets in following order:")
    plan.foreach(d => logger.debug(s"  ${d.id.toString}"))

    val numUpdated = pullBatched(plan)
    logger.info(s"Updated $numUpdated datasets")
  }

  def pullBatched(plan: Seq[Dataset]): Int = {
    if (plan.nonEmpty) {
      val kind = plan.head.kind
      val (batch, rest) = plan.span(_.kind == kind)

      kind match {
        case Dataset.Kind.Root       => pullRoot(batch)
        case Dataset.Kind.Derivative => pullDerivative(batch)
        case Dataset.Kind.Remote =>
          throw new NotImplementedError("Cannot pull remote datasets")
      }

      batch.length + pullBatched(rest)
    } else {
      0
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Root
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullRoot(batch: Seq[Dataset]): Boolean = {
    val datasets = batch.toVector

    logger.debug(
      "Pulling root datasets: " + datasets.map(_.id.toString).mkString(", ")
    )

    val extraMounts = datasets
      .map(_.rootPollingSource.get.fetch)
      .flatMap({
        case furl: ExternalSourceKind.FetchUrl =>
          furl.url.getScheme match {
            case "file" | null => List(new Path(furl.url))
            case _             => List.empty
          }
        case glob: ExternalSourceKind.FetchFilesGlob =>
          List(glob.path.getParent)
      })

    val pollConfig = polling.AppConf(
      tasks = datasets
        .map(ds => {
          val volume = metadataRepository.getVolumeFor(ds.id)
          polling.IngestTask(
            datasetToIngest = ds,
            checkpointsPath = volume.checkpointsDir.resolve(ds.id.toString),
            pollCachePath = volume.checkpointsDir.resolve(ds.id.toString),
            dataPath = volume.dataDir.resolve(ds.id.toString)
          )
        })
    )

    sparkRunner.submit(
      workspaceLayout = workspaceLayout,
      appClass = "dev.kamu.core.ingest.polling.IngestApp",
      extraFiles = Map(
        "pollConfig.yaml" -> (os => yaml.save(pollConfig.asManifest, os))
      ),
      extraMounts = extraMounts
    )

    logger.debug(
      s"Successfully pulled root datasets: " + datasets
        .map(_.id.toString)
        .mkString(", ")
    )

    true
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Derivative
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullDerivative(batch: Seq[Dataset]): Boolean = {
    val datasets = batch.toVector

    logger.debug(
      s"Running transformations for derivative datasets: " + datasets
        .map(_.id.toString)
        .mkString(", ")
    )

    val transformConfig = streaming.AppConfig(
      tasks = datasets.map(ds => {
        val volume = metadataRepository.getVolumeFor(ds.id)

        streaming.TransformTaskConfig(
          datasetToTransform = ds,
          inputDataPaths = ds.derivativeSource.get.inputs
            .map(i => i.id.toString -> volume.dataDir.resolve(i.id.toString))
            .toMap,
          checkpointsPath = volume.checkpointsDir.resolve(ds.id.toString),
          outputDataPath = volume.dataDir.resolve(ds.id.toString)
        )
      })
    )

    sparkRunner.submit(
      workspaceLayout = workspaceLayout,
      appClass = "dev.kamu.core.transform.streaming.TransformApp",
      extraFiles = Map(
        "transformConfig.yaml" -> (
          os => yaml.save(transformConfig.asManifest, os)
        )
      )
    )

    logger.debug(
      s"Successfully applied transformations for derivative datasets: " + datasets
        .map(_.id.toString)
        .mkString(", ")
    )

    true
  }
}
