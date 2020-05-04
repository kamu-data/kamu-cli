/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.transform.TransformService
import dev.kamu.cli.external.RemoteOperatorFactory
import dev.kamu.cli.ingest.IngestService
import dev.kamu.cli.metadata.{DoesNotExistException, MetadataRepository}
import dev.kamu.cli.UsageException
import dev.kamu.core.manifests.{DatasetID, DatasetKind}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.utils.fs._
import org.apache.log4j.LogManager
import yaml.defaults._
import pureconfig.generic.auto._

class PullCommand(
  ingestService: IngestService,
  transformService: TransformService,
  metadataRepository: MetadataRepository,
  remoteOperatorFactory: RemoteOperatorFactory,
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

    ingestService.pollAndIngest(batch)

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

    transformService.executeTransform(batch)

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
      val sourceRemote = metadataRepository.getRemote(
        metadataRepository
          .getDatasetRef(id)
          .remoteID
      )

      val volumeOperator = remoteOperatorFactory.buildFor(sourceRemote)
      volumeOperator.pull(Seq(id))
    }
    true
  }
}
