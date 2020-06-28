/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.metadata.{DoesNotExistException, MetadataRepository}
import dev.kamu.core.manifests._
import org.apache.logging.log4j.LogManager

class LogCommand(
  metadataRepository: MetadataRepository,
  id: String
) extends Command {
  implicit private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val datasetID = DatasetID(id)

    val metadataChain = try {
      metadataRepository.getMetadataChain(datasetID)
    } catch {
      case e: DoesNotExistException =>
        logger.error(e.getMessage)
        return
    }

    metadataChain.getBlocks().reverse.foreach(renderBlock)
  }

  private def renderBlock(block: MetadataBlock): Unit = {
    println(renderHeader(block))
    println(renderProperty("Date", block.systemTime))

    block.outputSlice.foreach { s =>
      println(renderProperty("Output.Records", s.numRecords))
      println(renderProperty("Output.Interval", s.interval.format()))
      if (s.hash.nonEmpty)
        println(renderProperty("Output.Hash", s.hash))
    }

    block.outputWatermark.foreach { w =>
      println(renderProperty("Output.Watermark", w))
    }

    block.inputSlices.zipWithIndex.foreach {
      case (s, i) =>
        println(renderProperty(s"Input[$i].Records", s.numRecords))
        println(renderProperty(s"Input[$i].Interval", s.interval.format()))
        if (s.hash.nonEmpty)
          println(renderProperty(s"Input[$i].Hash", s.hash))
    }

    block.source.foreach {
      case _: SourceKind.Root =>
        println(renderProperty("Source", "<Root source updated>"))
      case _: SourceKind.Derivative =>
        println(renderProperty("Source", "<Derivative source updated>"))
    }

    println()
  }

  private def renderHeader(block: MetadataBlock): String = {
    s"\033[33mBlock: ${block.blockHash}\033[0m"
  }

  private def renderProperty(name: String, value: Any): String = {
    s"\033[2m$name:\033[0m $value"
  }
}
