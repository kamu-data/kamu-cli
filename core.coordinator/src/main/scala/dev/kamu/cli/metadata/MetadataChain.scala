/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.metadata

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Instant

import better.files.File
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import pureconfig.{ConfigReader, ConfigWriter, Derivation}
import pureconfig.generic.auto._

import scala.reflect.ClassTag

class MetadataChain(datasetDir: Path) {

  def init(ds: DatasetSnapshot, systemTime: Instant): Unit = {
    val initialBlock = MetadataBlock(
      blockHash = "",
      prevBlockHash = "",
      systemTime = systemTime,
      source = Some(ds.source)
    ).hashed()

    val initialSummary = DatasetSummary(
      id = ds.id,
      kind = ds.kind,
      datasetDependencies = ds.dependsOn.toSet,
      vocab = ds.vocab,
      lastPulled = None,
      numRecords = 0,
      dataSize = 0
    )

    try {
      File(blocksDir).createDirectories()
      saveResource(initialSummary, summaryPath)
      saveResource(initialBlock, blocksDir.resolve(initialBlock.blockHash))
    } catch {
      case e: Exception =>
        File(datasetDir).delete()
        throw e
    }
  }

  // TODO: add invariant validation
  def append(_block: MetadataBlock): MetadataBlock = {
    val block = _block.hashed()
    saveResource(block, blocksDir.resolve(block.blockHash))
    block
  }

  def getSummary(): DatasetSummary = {
    loadResource[DatasetSummary](summaryPath)
  }

  def updateSummary(
    update: DatasetSummary => DatasetSummary
  ): DatasetSummary = {
    val newSummary = update(getSummary())
    saveResource(newSummary, summaryPath)
    newSummary
  }

  def getSnapshot(): DatasetSnapshot = {
    val summary = getSummary()

    val source = getBlocks().reverse
      .flatMap(_.source)
      .head

    DatasetSnapshot(
      id = summary.id,
      source = source,
      vocab = summary.vocab
    )
  }

  /** Returns metadata blocks in historical order */
  def getBlocks(): Vector[MetadataBlock] = {
    val blocks = File(blocksDir).list
      .map(f => loadResource[MetadataBlock](f.path))
      .map(b => (b.blockHash, b))
      .toMap

    val nextBlocks = blocks.values
      .map(b => (b.prevBlockHash, b.blockHash))
      .toMap

    val blocksOrdered =
      new scala.collection.immutable.VectorBuilder[MetadataBlock]()

    var parentBlockHash = ""
    while (nextBlocks.contains(parentBlockHash)) {
      parentBlockHash = nextBlocks(parentBlockHash)
      blocksOrdered += blocks(parentBlockHash)
    }

    blocksOrdered.result()
  }

  protected def summaryPath: Path = datasetDir.resolve("summary")

  protected def blocksDir: Path = datasetDir.resolve("blocks")

  /////////////////////////////////////////////////////////////////////////////
  // Helpers
  /////////////////////////////////////////////////////////////////////////////

  protected def saveResource[T: ClassTag](obj: T, path: Path)(
    implicit derivation: Derivation[ConfigWriter[Manifest[T]]]
  ): Unit = {
    yaml.save(Manifest(obj), path)
  }

  protected def loadResource[T: ClassTag](path: Path)(
    implicit derivation: Derivation[ConfigReader[Manifest[T]]]
  ): T = {
    yaml.load[Manifest[T]](path).content
  }

  protected implicit class MetadataBlockEx(b: MetadataBlock) {
    def hashed(): MetadataBlock = {
      val digest = MessageDigest.getInstance("sha-256")
      val repr = yaml.saveStr(b)

      val blockHash = digest
        .digest(repr.getBytes(StandardCharsets.UTF_8))
        .map("%02x".format(_))
        .mkString

      b.copy(blockHash = blockHash)
    }
  }

}
