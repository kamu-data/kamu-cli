/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.external.{SparkRunner, VolumeOperatorFactory}
import dev.kamu.cli.{
  DoesNotExistException,
  MetadataRepository,
  UsageException,
  WorkspaceLayout
}
import dev.kamu.core.ingest.polling
import dev.kamu.core.manifests.{
  DatasetID,
  DatasetKind,
  FetchSourceKind,
  Manifest,
  SourceKind
}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.transform.streaming
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import yaml.defaults._
import pureconfig.generic.auto._

class PullCommand(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository,
  volumeOperatorFactory: VolumeOperatorFactory,
  sparkRunner: SparkRunner,
  ids: Seq[String],
  all: Boolean,
  recursive: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val datasetIDs = {
      if (all) metadataRepository.getAllDatasets()
      else ids.map(DatasetID)
    }

    val plan = try {
      metadataRepository
        .getDatasetsInDependencyOrder(
          datasetIDs,
          recursive || all // All implies recursive, which is more efficient
        )
    } catch {
      case e: DoesNotExistException =>
        throw new UsageException(e.getMessage)
    }

    logger.debug("Pulling datasets in following order: " + plan.mkString(", "))

    val numUpdated = pullBatched(plan)
    logger.info(s"Updated $numUpdated datasets")
  }

  def pullBatched(plan: Seq[DatasetID]): Int = {
    if (plan.nonEmpty) {
      val withKinds =
        plan.map(id => (id, metadataRepository.getDatasetKind(id)))

      val kind = withKinds.head._2
      val (first, rest) = withKinds.span(_._2 == kind)
      val batch = first.map(_._1)

      kind match {
        case DatasetKind.Root =>
          pullRoot(batch)
        case DatasetKind.Derivative =>
          // TODO: Streaming transform currently doesn't handle dependencies
          batch.foreach(ds => pullDerivative(Seq(ds)))
        case DatasetKind.Remote =>
          pullRemote(batch)
      }

      batch.length + pullBatched(rest.map(_._1))
    } else {
      0
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Root
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullRoot(batch: Seq[DatasetID]): Boolean = {
    val datasets = batch.toVector

    logger.debug(
      "Pulling root datasets: " + datasets.map(_.toString).mkString(", ")
    )

    // TODO: Costly chain traversal
    // TODO: Account for missing files
    val extraMounts = datasets
      .flatMap(id => {
        val metaChain = metadataRepository.getMetadataChain(id)

        metaChain
          .getBlocks()
          .flatMap(_.source)
          .flatMap {
            case r: SourceKind.Root => Some(r.fetch)
            case _                  => None
          }
      })
      .flatMap({
        case furl: FetchSourceKind.Url =>
          furl.url.getScheme match {
            case "file" | null => List(new Path(furl.url))
            case _             => List.empty
          }
        case glob: FetchSourceKind.FilesGlob =>
          List(glob.path.getParent)
      })

    val pollConfig = polling.AppConf(
      tasks = datasets
        .map(id => {
          polling.IngestTask(
            datasetToIngest = id,
            datasetLayout = metadataRepository.getDatasetLayout(id)
          )
        })
    )

    sparkRunner.submit(
      workspaceLayout = workspaceLayout,
      appClass = "dev.kamu.core.ingest.polling.IngestApp",
      extraFiles = Map(
        "pollConfig.yaml" -> (os => yaml.save(Manifest(pollConfig), os))
      ),
      extraMounts = extraMounts
    )

    logger.debug(
      s"Successfully pulled root datasets: " + datasets
        .map(_.toString)
        .mkString(", ")
    )

    true
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Derivative
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullDerivative(batch: Seq[DatasetID]): Boolean = {
    val datasets = batch.toVector

    logger.debug(
      s"Running transformations for derivative datasets: " + datasets
        .map(_.toString)
        .mkString(", ")
    )

    val transformConfig = streaming.AppConfig(
      tasks = datasets.map(id => {
        val metaChain = metadataRepository.getMetadataChain(id)

        // TODO: Costly chain traversal
        val allInputs = metaChain
          .getBlocks()
          .flatMap(_.source)
          .flatMap {
            case d: SourceKind.Derivative => d.inputs
            case _                        => Seq.empty
          }
          .map(_.id)

        val allDatasets = allInputs :+ id

        streaming.TransformTaskConfig(
          datasetToTransform = id,
          datasetLayouts = allDatasets
            .map(i => (i.toString, metadataRepository.getDatasetLayout(i)))
            .toMap
        )
      })
    )

    sparkRunner.submit(
      workspaceLayout = workspaceLayout,
      appClass = "dev.kamu.core.transform.streaming.TransformApp",
      extraFiles = Map(
        "transformConfig.yaml" -> (
          os => yaml.save(Manifest(transformConfig), os)
        )
      )
    )

    logger.debug(
      s"Successfully applied transformations for derivative datasets: " + datasets
        .map(_.toString)
        .mkString(", ")
    )

    true
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Remote
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullRemote(batch: Seq[DatasetID]): Boolean = {
    val datasets = batch.toVector

    logger.debug(
      "Pulling remote datasets: " + datasets.map(_.toString).mkString(", ")
    )

    for (id <- datasets) {
      val sourceVolume = metadataRepository.getVolume(
        metadataRepository
          .getDatasetVolumeID(id)
      )

      val volumeOperator = volumeOperatorFactory.buildFor(sourceVolume)

      volumeOperator.pull(Seq(id))
    }
    true
  }
}
