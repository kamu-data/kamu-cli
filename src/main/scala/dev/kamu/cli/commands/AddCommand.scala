/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.utility.DependencyGraph
import dev.kamu.cli.{
  AlreadyExistsException,
  MetadataRepository,
  MissingReferenceException,
  SchemaNotSupportedException
}
import dev.kamu.core.manifests.{DatasetID, DatasetSnapshot}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class AddCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  manifests: Seq[java.net.URI],
  replace: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val sources = {
      try {
        manifests
          .map(manifestURI => {
            logger.debug(s"Loading dataset from: $manifestURI")
            val snapshot =
              metadataRepository.loadDatasetSnapshotFromURI(manifestURI)
            (snapshot.id, snapshot)
          })
          .toMap
      } catch {
        case e: java.io.FileNotFoundException =>
          logger.error(s"File not found: ${e.getMessage} - aborted")
          Map.empty[DatasetID, DatasetSnapshot]
        case e: SchemaNotSupportedException =>
          logger.error(s"URI schema not supported: ${e.getMessage} - aborted")
          Map.empty[DatasetID, DatasetSnapshot]
      }
    }

    @scala.annotation.tailrec
    def addDataset(ds: DatasetSnapshot): Boolean = {
      try {
        metadataRepository.addDataset(ds)
        true
      } catch {
        case e: AlreadyExistsException =>
          if (replace) {
            logger.warn(e.getMessage + " - replacing")
            metadataRepository.deleteDataset(ds.id)
            addDataset(ds)
          } else {
            logger.warn(e.getMessage + " - skipping")
            false
          }
        case e: MissingReferenceException =>
          logger.warn(e.getMessage + " - skipping")
          false
      }
    }

    val depGraph = new DependencyGraph[DatasetID](
      sources.get(_).map(_.dependsOn.toList).getOrElse(List.empty)
    )

    val ordered = depGraph
      .resolve(sources.keys.toList)
      .filter(sources.contains)

    val numAdded = ordered
      .map(sources(_))
      .map(addDataset)
      .count(added => added)

    logger.info(s"Added $numAdded dataset(s)")
  }
}
