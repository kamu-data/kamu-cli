/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest

import java.time.Instant

import dev.kamu.cli.WorkspaceLayout
import dev.kamu.cli.external.SparkRunner
import dev.kamu.cli.ingest.convert.{ConversionStepFactory, IngestCheckpoint}
import dev.kamu.cli.ingest.fetch.{
  CacheableSource,
  CachingBehavior,
  DownloadCheckpoint,
  SourceFactory
}
import dev.kamu.cli.ingest.prep.{PrepCheckpoint, PrepStepFactory}
import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.core.manifests.infra.{IngestConfig, IngestTask, MetadataChainFS}
import dev.kamu.core.manifests.{
  DatasetID,
  DatasetVocabulary,
  DatasetVocabularyOverrides,
  FetchSourceKind,
  Manifest,
  MetadataBlock,
  SourceKind
}
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.Clock
import org.apache.commons.compress.compressors.bzip2.{
  BZip2CompressorInputStream,
  BZip2CompressorOutputStream
}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._

class IngestService(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  metadataRepository: MetadataRepository,
  sparkRunner: SparkRunner,
  systemClock: Clock
) {
  val downloadCheckpointFileName = "download.checkpoint.yaml"
  val downloadDataFileName = "download.bz2"
  val prepCheckpointFileName = "prepare.checkpoint.yaml"
  val prepDataFileName = "prepare.bz2"
  val ingestCheckpointFileName = "ingest.checkpoint.yaml"

  private val logger = LogManager.getLogger(getClass.getName)

  private val sourceFactory = new SourceFactory(fileSystem, systemClock)
  private val conversionStepFactory = new ConversionStepFactory()
  private val prepStepFactory = new PrepStepFactory(fileSystem)
  private val downloadExecutor =
    new CheckpointingExecutor[DownloadCheckpoint](fileSystem)
  private val prepExecutor =
    new CheckpointingExecutor[PrepCheckpoint](fileSystem)
  private val ingestExecutor =
    new CheckpointingExecutor[IngestCheckpoint](fileSystem)

  def pollAndIngest(datasetIDs: Seq[DatasetID]): Unit = {
    for (datasetID <- datasetIDs) {
      val metaChain = metadataRepository.getMetadataChain(datasetID)
      val datasetLayout = metadataRepository.getDatasetLayout(datasetID)

      val summary = metaChain.getSummary()
      val source = metaChain
        .getBlocks()
        .reverse
        .flatMap(_.source)
        .head
        .asInstanceOf[SourceKind.Root]

      val cachingBehavior = sourceFactory.getCachingBehavior(source.fetch)

      for (externalSource <- sourceFactory.getSource(source.fetch)) {
        logger.debug(
          s"Processing data source: $datasetID:${externalSource.sourceID}"
        )

        val downloadCheckpointPath = datasetLayout.checkpointsDir
          .resolve(externalSource.sourceID)
          .resolve(downloadCheckpointFileName)
        val downloadDataPath = datasetLayout.cacheDir
          .resolve(externalSource.sourceID)
          .resolve(downloadDataFileName)
        val prepCheckpointPath = datasetLayout.checkpointsDir
          .resolve(externalSource.sourceID)
          .resolve(prepCheckpointFileName)
        val prepDataPath = datasetLayout.cacheDir
          .resolve(externalSource.sourceID)
          .resolve(prepDataFileName)
        val ingestCheckpointPath = datasetLayout.checkpointsDir
          .resolve(externalSource.sourceID)
          .resolve(ingestCheckpointFileName)

        Seq(
          downloadCheckpointPath,
          downloadDataPath,
          prepCheckpointPath,
          prepDataPath
        ).map(_.getParent)
          .filter(!fileSystem.exists(_))
          .foreach(fileSystem.mkdirs)

        logger.debug(s"Stage: polling")
        val downloadResult = maybeDownload(
          source,
          externalSource,
          cachingBehavior,
          downloadCheckpointPath,
          downloadDataPath
        )

        logger.debug(s"Stage: prep")
        val prepResult = maybePrepare(
          source,
          downloadDataPath,
          downloadResult.checkpoint,
          prepCheckpointPath,
          prepDataPath
        )

        logger.debug(s"Stage: ingest")
        val ingestResult = maybeIngest(
          datasetID,
          source,
          prepResult.checkpoint,
          prepDataPath,
          ingestCheckpointPath,
          summary.vocabulary
            .getOrElse(DatasetVocabularyOverrides())
            .asDatasetVocabulary()
        )

        if (ingestResult.wasUpToDate) {
          // TODO: Should we commit anyway to advance dataset clock?
          logger.debug(
            s"Data is up to date: $datasetID:${externalSource.sourceID}"
          )
        } else {
          // TODO: Atomicity
          commitMetadata(
            datasetID,
            metaChain,
            ingestResult
          )

          logger.debug(
            s"Data was updated: $datasetID:${externalSource.sourceID}"
          )

          // Clean up the source cache dir
          fileSystem.delete(
            datasetLayout.cacheDir.resolve(externalSource.sourceID),
            true
          )
        }
      }
    }
  }

  def maybeDownload(
    source: SourceKind.Root,
    externalSource: CacheableSource,
    cachingBehavior: CachingBehavior,
    downloadCheckpointPath: Path,
    downloadDataPath: Path
  ): ExecutionResult[DownloadCheckpoint] = {
    downloadExecutor.execute(
      checkpointPath = downloadCheckpointPath,
      execute = storedCheckpoint => {
        val downloadResult = externalSource.maybeDownload(
          storedCheckpoint,
          cachingBehavior,
          body => {
            val outputStream = fileSystem.create(downloadDataPath, true)
            val compressedStream =
              new BZip2CompressorOutputStream(outputStream)
            try {
              IOUtils.copy(body, compressedStream)
            } finally {
              compressedStream.close()
            }
          }
        )

        if (!downloadResult.checkpoint.isCacheable)
          logger.warn(
            "Data source is uncacheable - data will not be updated in future."
          )

        downloadResult
      }
    )
  }

  // TODO: Avoid copying data if prepare step is a no-op
  def maybePrepare(
    source: SourceKind.Root,
    downloadDataPath: Path,
    downloadCheckpoint: DownloadCheckpoint,
    prepCheckpointPath: Path,
    prepDataPath: Path
  ): ExecutionResult[PrepCheckpoint] = {
    prepExecutor.execute(
      checkpointPath = prepCheckpointPath,
      execute = storedCheckpoint => {
        if (storedCheckpoint.isDefined
            && storedCheckpoint.get.downloadTimestamp == downloadCheckpoint.lastDownloaded) {
          ExecutionResult(
            wasUpToDate = true,
            checkpoint = storedCheckpoint.get
          )
        } else {
          val prepStep = prepStepFactory.getComposedSteps(source.prepare)
          val convertStep = conversionStepFactory.getComposedSteps(source.read)

          val inputStream = fileSystem.open(downloadDataPath)
          val decompressedInStream = new BZip2CompressorInputStream(inputStream)

          val outputStream = fileSystem.create(prepDataPath, true)
          val compressedOutStream =
            new BZip2CompressorOutputStream(outputStream)

          try {
            val preparedInStream = prepStep.prepare(decompressedInStream)
            val convertedInStream = convertStep.convert(preparedInStream)

            IOUtils.copy(convertedInStream, compressedOutStream)

            prepStep.join()
          } finally {
            decompressedInStream.close()
            compressedOutStream.close()
          }

          ExecutionResult(
            wasUpToDate = false,
            checkpoint = PrepCheckpoint(
              downloadTimestamp = downloadCheckpoint.lastDownloaded,
              eventTime = downloadCheckpoint.eventTime,
              lastPrepared = systemClock.instant()
            )
          )
        }
      }
    )
  }

  def maybeIngest(
    datasetID: DatasetID,
    source: SourceKind.Root,
    prepCheckpoint: PrepCheckpoint,
    prepDataPath: Path,
    ingestCheckpointPath: Path,
    vocab: DatasetVocabulary
  ): ExecutionResult[IngestCheckpoint] = {
    ingestExecutor.execute(
      checkpointPath = ingestCheckpointPath,
      execute = storedCheckpoint => {
        if (storedCheckpoint.isDefined
            && storedCheckpoint.get.prepTimestamp == prepCheckpoint.lastPrepared) {
          ExecutionResult(
            wasUpToDate = true,
            checkpoint = storedCheckpoint.get
          )
        } else {
          val block = ingest(
            datasetID,
            source,
            prepCheckpoint.eventTime,
            prepDataPath,
            vocab
          )

          ExecutionResult(
            wasUpToDate = false,
            checkpoint = IngestCheckpoint(
              prepTimestamp = prepCheckpoint.lastPrepared,
              lastIngested = systemClock.instant(),
              resultingBlock = block
            )
          )
        }
      }
    )
  }

  def ingest(
    datasetID: DatasetID,
    source: SourceKind.Root,
    eventTime: Option[Instant],
    prepDataPath: Path,
    vocabulary: DatasetVocabulary
  ): MetadataBlock = {
    Temp.withRandomTempDir(fileSystem, "kamu-ingest-") { tempDir =>
      // TODO: Account for missing files
      val extraMounts = source.fetch match {
        case furl: FetchSourceKind.Url =>
          furl.url.getScheme match {
            case "file" | null => List(new Path(furl.url))
            case _             => List.empty
          }
        case glob: FetchSourceKind.FilesGlob =>
          List(glob.path.getParent)
        case _ =>
          throw new RuntimeException(s"Unsupported fetch kind: ${source.fetch}")
      }

      val pollConfig = IngestConfig(
        tasks = Vector(
          IngestTask(
            datasetID = datasetID,
            source = source,
            datasetLayout = metadataRepository.getDatasetLayout(datasetID),
            dataToIngest = prepDataPath,
            eventTime = eventTime,
            datasetVocab = vocabulary,
            metadataOutputDir = tempDir
          )
        )
      )

      sparkRunner.submit(
        workspaceLayout = workspaceLayout,
        appClass = "dev.kamu.engine.spark.ingest.IngestApp",
        extraFiles = Map(
          IngestConfig.configFileName -> (
            os => yaml.save(Manifest(pollConfig), os)
          )
        ),
        extraMounts = tempDir :: extraMounts
      )

      val inputStream = fileSystem.open(tempDir.resolve("block.yaml"))
      val block = yaml.load[Manifest[MetadataBlock]](inputStream).content
      inputStream.close()
      block
    }
  }

  def commitMetadata(
    datasetID: DatasetID,
    metaChain: MetadataChainFS,
    ingestResult: ExecutionResult[IngestCheckpoint]
  ): Unit = {
    // TODO: Avoid loading blocks again
    val block = metaChain.append(
      ingestResult.checkpoint.resultingBlock.copy(
        prevBlockHash = metaChain.getBlocks().last.blockHash
      )
    )

    val dataSize = fileSystem
      .getContentSummary(metadataRepository.getDatasetLayout(datasetID).dataDir)
      .getSpaceConsumed

    // TODO: Atomicity
    metaChain.updateSummary(
      s =>
        s.copy(
          lastPulled = Some(systemClock.instant()),
          numRecords = s.numRecords + block.outputSlice.get.numRecords,
          dataSize = dataSize
        )
    )

    logger.info(
      s"Committing new metadata block: $datasetID (${block.blockHash})"
    )
  }

}
