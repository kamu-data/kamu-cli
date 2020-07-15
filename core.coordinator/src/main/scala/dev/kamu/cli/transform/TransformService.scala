/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.transform

import better.files.File
import dev.kamu.cli.metadata.{MetadataChain, MetadataRepository}
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.infra._
import dev.kamu.core.utils.Clock
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.LogManager
import spire.math.Interval
import spire.math.interval.{Closed, Unbound, ValueBound}

case class TransformBatch(
  source: DatasetSource.Derivative,
  inputSlices: Map[DatasetID, InputDataSlice]
) {
  def isEmpty: Boolean = {
    inputSlices.values.forall(
      s => s.interval.isEmpty && s.explicitWatermarks.isEmpty
    )
  }
}

class TransformService(
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
          !File(metadataRepository.getDatasetLayout(inputID).dataDir).exists
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
        val nextBlock = engineExecuteQuery(datasetID, batch).block
        commitNewBlock(datasetID, nextBlock)
      }
    }
  }

  private def engineExecuteQuery(
    datasetID: DatasetID,
    batch: TransformBatch
  ): ExecuteQueryResult = {
    val allDatasets = batch.source.inputs :+ datasetID

    val request = ExecuteQueryRequest(
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
      dataDirs = allDatasets
        .map(
          i =>
            (
              i.toString,
              metadataRepository.getDatasetLayout(i).dataDir.toString
            )
        )
        .toMap,
      checkpointsDir =
        metadataRepository.getDatasetLayout(datasetID).checkpointsDir.toString
    )

    val engine = engineFactory.getEngine(batch.source.transformPartial.engine)
    engine.executeQuery(request)
  }

  private def commitNewBlock(
    datasetID: DatasetID,
    block: MetadataBlock
  ): Unit = {
    val outputMetaChain = metadataRepository.getMetadataChain(datasetID)
    val newBlock = outputMetaChain.append(
      block.copy(
        prevBlockHash = outputMetaChain.getBlocks().head.blockHash
      )
    )

    val dataSize = Some(metadataRepository.getDatasetLayout(datasetID).dataDir)
      .filter(p => File(p).exists)
      .map(p => FileUtils.sizeOfDirectory(p.toFile))
      .getOrElse(0L)

    outputMetaChain.updateSummary(
      s =>
        s.copy(
          lastPulled = Some(systemClock.instant()),
          numRecords = s.numRecords + newBlock.outputSlice.get.numRecords,
          dataSize = dataSize
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
      .flatMap(_.source)

    // TODO: source could've changed several times
    if (sources.length > 1)
      throw new RuntimeException("Transform evolution is not yet supported")

    val source = sources.head.asInstanceOf[DatasetSource.Derivative]

    val inputSlices = source.inputs.zipWithIndex.map {
      case (inputID, index) =>
        val inputMetaChain = metadataRepository.getMetadataChain(inputID)
        (
          inputID,
          getInputSlice(
            inputID,
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
  ): InputDataSlice = {
    // Determine processed data range
    // Result is either: () or (inf, upper] or (lower, upper]
    val ivProcessed = outputMetaChain
      .getBlocks()
      .filter(_.inputSlices.nonEmpty)
      .map(_.inputSlices.get(inputIndex))
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

    // Filter unprocessed input blocks
    val blocksUnprocessed = inputMetaChain
      .getBlocks()
      .takeWhile(b => ivUnprocessed.contains(b.systemTime))
      .toVector

    // Determine available data/watermark range
    // Result is either: () or (-inf, upper]
    val ivAvailable = blocksUnprocessed.headOption
      .map(b => Interval.fromBounds(Unbound(), Closed(b.systemTime)))
      .getOrElse(Interval.empty)

    // Result is either: () or (lower, upper]
    val ivToProcess = ivAvailable & ivUnprocessed

    val explicitWatermarks = blocksUnprocessed.reverse
      .filter(_.outputWatermark.isDefined)
      .map(b => Watermark(b.systemTime, b.outputWatermark.get))

    logger.debug(
      s"Input range for $inputID is: $ivToProcess (available: $ivAvailable, processed: $ivProcessed)"
    )

    InputDataSlice(
      interval = ivToProcess,
      explicitWatermarks = explicitWatermarks
    )
  }
}
