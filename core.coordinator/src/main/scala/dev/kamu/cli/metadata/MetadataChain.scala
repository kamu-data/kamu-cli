/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.metadata

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Instant
import java.util.Scanner

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
      File(refsDir).createDirectories()
      saveResource(initialSummary, summaryPath)
      saveResource(initialBlock, blocksDir.resolve(initialBlock.blockHash))
      saveRef("head", initialBlock.blockHash)
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
    saveRef("head", block.blockHash)
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

    val source = getBlocks()
      .flatMap(_.source)
      .head

    DatasetSnapshot(
      id = summary.id,
      source = source,
      vocab = summary.vocab
    )
  }

  /** Returns metadata blocks starting from head and down until the origin */
  def getBlocks(): Stream[MetadataBlock] = {
    loadRef("head") match {
      case None => Stream.empty
      case Some(hash) => {
        val block = loadResource[MetadataBlock](blocksDir.resolve(hash))
        block #:: nextBlock(block)
      }
    }
  }

  protected def nextBlock(block: MetadataBlock): Stream[MetadataBlock] = {
    if (block.prevBlockHash.isEmpty) {
      Stream.empty
    } else {
      val newBlock = loadResource[MetadataBlock](
        blocksDir.resolve(block.prevBlockHash)
      )
      newBlock #:: nextBlock(newBlock)
    }
  }

  protected def summaryPath: Path = datasetDir.resolve("summary")

  protected def blocksDir: Path = datasetDir.resolve("blocks")

  protected def refsDir: Path = datasetDir.resolve("refs")

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

  protected def saveRef(name: String, value: String): Unit = {
    val path = refsDir.resolve(name)
    val outputStream = File(path).newOutputStream()
    try {
      val writer = new PrintWriter(outputStream)
      try {
        writer.write(value)
      } finally {
        writer.close()
      }
    } finally {
      outputStream.close()
    }
  }

  protected def loadRef(name: String): Option[String] = {
    val file = File(refsDir.resolve(name))
    if (!file.exists) {
      None
    } else {
      val scanner = new Scanner(file.toJava)
      try {
        Some(scanner.next)
      } finally {
        scanner.close()
      }
    }
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
