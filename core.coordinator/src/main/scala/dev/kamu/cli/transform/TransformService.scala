/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import dev.kamu.cli.WorkspaceLayout
import dev.kamu.cli.metadata.{MetadataChain, MetadataRepository}
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.infra.{TransformConfig, TransformTaskConfig}
import dev.kamu.core.utils.Clock
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import spire.math.Interval
import spire.math.interval.{Unbound, ValueBound}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.fs.Temp
import yaml.defaults._
import pureconfig.generic.auto._

case class TransformBatch(
  source: SourceKind.Derivative,
  inputSlices: Map[DatasetID, DataSlice]
) {
  def isEmpty: Boolean = inputSlices.values.forall(_.interval.isEmpty)
}

class TransformService(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository,
  systemClock: Clock,
  engineFactory: EngineFactory
) {
  val logger = LogManager.getLogger(getClass.getName)

  def executeTransform(datasetIDs: Seq[DatasetID]): Unit = {
    for (datasetID <- datasetIDs) {
      val batch = getNextBatch(datasetID)

      val missingInputs = batch.inputSlices.keys.filter(
        inputID =>
          !fileSystem
            .exists(metadataRepository.getDatasetLayout(inputID).dataDir)
      )

      if (missingInputs.nonEmpty) {
        val mia = missingInputs.map(_.toString).mkString(", ")
        logger.warn(
          s"Dataset $datasetID depends on $mia which has not been pulled before - skipping"
        )
      } else if (batch.isEmpty) {
        logger.info(s"Dataset is up-to-date: $datasetID")
      } else {
        // TODO: Atomicity
        val nextBlock = engineExecuteQuery(datasetID, batch)
        commitNewBlock(datasetID, nextBlock)
      }
    }
  }

  private def engineExecuteQuery(
    datasetID: DatasetID,
    batch: TransformBatch
  ): MetadataBlock = {
    Temp.withRandomTempDir(fileSystem, "kamu-transform-") { tempDir =>
      val allDatasets = batch.source.inputs.map(_.id) :+ datasetID

      val transformConfig = TransformConfig(
        tasks = Vector(
          TransformTaskConfig(
            datasetID = datasetID,
            source = batch.source,
            inputSlices = batch.inputSlices.map {
              case (id, slice) => (id.toString, slice)
            },
            datasetVocabs = allDatasets
              .map(
                id => (id.toString, metadataRepository.getDatasetVocabulary(id))
              )
              .toMap,
            datasetLayouts = allDatasets
              .map(i => (i.toString, metadataRepository.getDatasetLayout(i)))
              .toMap,
            metadataOutputDir = tempDir
          )
        )
      )

      val engine = engineFactory.getEngine()

      engine.submit(
        workspaceLayout = workspaceLayout,
        appClass = "dev.kamu.engine.spark.transform.TransformApp",
        extraFiles = Map(
          TransformConfig.configFileName -> (
            os => yaml.save(Manifest(transformConfig), os)
          )
        ),
        extraMounts = Seq(tempDir)
      )

      val inputStream = fileSystem.open(tempDir.resolve("block.yaml"))
      val block = yaml.load[Manifest[MetadataBlock]](inputStream).content
      inputStream.close()
      block
    }
  }

  private def commitNewBlock(
    datasetID: DatasetID,
    block: MetadataBlock
  ): Unit = {
    val outputMetaChain = metadataRepository.getMetadataChain(datasetID)
    val newBlock = outputMetaChain.append(
      block.copy(
        prevBlockHash = outputMetaChain.getBlocks().last.blockHash
      )
    )

    outputMetaChain.updateSummary(
      s =>
        s.copy(
          lastPulled = Some(systemClock.instant()),
          numRecords = s.numRecords + newBlock.outputSlice.get.numRecords,
          dataSize = fileSystem
            .getContentSummary(
              metadataRepository.getDatasetLayout(datasetID).dataDir
            )
            .getSpaceConsumed
        )
    )

    logger.info(
      s"Committed new block: $datasetID (${newBlock.blockHash})"
    )
  }

  private def getNextBatch(
    datasetID: DatasetID
  ): TransformBatch = {
    val outputMetaChain = metadataRepository.getMetadataChain(datasetID)

    val sources = outputMetaChain
      .getBlocks()
      .reverse
      .flatMap(_.source)

    // TODO: source could've changed several times
    if (sources.length > 1)
      throw new RuntimeException("Transform evolution is not yet supported")

    val source = sources.head.asInstanceOf[SourceKind.Derivative]

    val inputSlices = source.inputs.zipWithIndex.map {
      case (input, index) =>
        val inputMetaChain = metadataRepository.getMetadataChain(input.id)
        (
          input.id,
          getInputSlice(
            input.id,
            index,
            inputMetaChain,
            outputMetaChain
          )
        )
    }.toMap

    TransformBatch(source, inputSlices)
  }

  private def getInputSlice(
    inputID: DatasetID,
    inputIndex: Int,
    inputMetaChain: MetadataChain,
    outputMetaChain: MetadataChain
  ): DataSlice = {

    // Determine available data range
    // Result is either: () or (-inf, upper]
    val ivAvailable = inputMetaChain
      .getBlocks()
      .reverse
      .flatMap(_.outputSlice)
      .find(_.interval.nonEmpty)
      .map(_.interval)
      .map(i => Interval.fromBounds(Unbound(), i.upperBound))
      .getOrElse(Interval.empty)

    // Determine processed data range
    // Result is either: () or (inf, upper] or (lower, upper]
    val ivProcessed = outputMetaChain
      .getBlocks()
      .reverse
      .filter(_.inputSlices.nonEmpty)
      .map(_.inputSlices(inputIndex))
      .find(_.interval.nonEmpty)
      .map(_.interval)
      .getOrElse(Interval.empty)

    // Determine unprocessed data range
    // Result is either: (-inf, inf) or (lower, inf)
    val ivUnprocessed = ivProcessed.upperBound match {
      case ValueBound(upper) =>
        Interval.above(upper)
      case _ =>
        Interval.all
    }

    // Result is either: () or (lower, upper]
    val ivToProcess = ivAvailable & ivUnprocessed

    logger.debug(
      s"Input range for $inputID is: $ivToProcess (available: $ivAvailable, processed: $ivProcessed)"
    )
    DataSlice(
      interval = ivToProcess,
      hash = "",
      numRecords = -1
    )
  }
}
