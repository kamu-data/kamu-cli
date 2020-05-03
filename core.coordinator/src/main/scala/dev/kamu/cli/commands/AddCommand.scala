/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import java.net.URI

import dev.kamu.cli.utility.DependencyGraph
import dev.kamu.cli.metadata.{
  AlreadyExistsException,
  DoesNotExistException,
  MetadataRepository,
  MissingReferenceException,
  SchemaNotSupportedException
}
import dev.kamu.core.manifests.{DatasetID, DatasetRef, DatasetSnapshot}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class AddCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  urls: Seq[URI],
  replace: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val (remoteRefs, rest) = partitionRemoteRefs(urls)
    val numAdded = addDatasetsFromRemotes(remoteRefs) + addDatasetsFromManifests(
      rest
    )
    logger.info(s"Added $numAdded dataset(s)")
  }

  private def partitionRemoteRefs(
    urls: Seq[URI]
  ): (Vector[DatasetRef], Vector[URI]) = {
    val parsed = urls
      .map(url => {
        DatasetRef.fromString(url.toString) match {
          case Some(ref) =>
            try {
              metadataRepository.getRemote(ref.remoteID)
              Left(ref)
            } catch {
              case _: DoesNotExistException =>
                Right(url)
            }
          case None =>
            Right(url)
        }
      })
      .toVector

    (
      parsed.flatMap(_.left.toOption),
      parsed.flatMap(_.right.toOption)
    )
  }

  private def addDatasetsFromManifests(manifestUrls: Seq[URI]): Int = {
    val sources = {
      try {
        manifestUrls
          .map(manifestURI => {
            logger.debug(s"Loading dataset from: $manifestURI")
            val snapshot = try {
              metadataRepository.loadDatasetSnapshotFromURI(manifestURI)
            } catch {
              case e: Exception =>
                logger.error(s"Error while loading dataset from: $manifestURI")
                throw e
            }
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

    val depGraph = new DependencyGraph[DatasetID](
      sources.get(_).map(_.dependsOn.toList).getOrElse(List.empty)
    )

    val ordered = depGraph
      .resolve(sources.keys.toList)
      .filter(sources.contains)

    ordered
      .map(sources(_))
      .map(addDatasetFromSnapshot)
      .count(added => added)
  }

  @scala.annotation.tailrec
  private def addDatasetFromSnapshot(ds: DatasetSnapshot): Boolean = {
    try {
      metadataRepository.addDataset(ds)
      true
    } catch {
      case e: AlreadyExistsException =>
        if (replace) {
          logger.warn(e.getMessage + " - replacing")
          metadataRepository.deleteDataset(ds.id)
          addDatasetFromSnapshot(ds)
        } else {
          logger.warn(e.getMessage + " - skipping")
          false
        }
      case e: MissingReferenceException =>
        logger.warn(e.getMessage + " - skipping")
        false
    }
  }

  private def addDatasetsFromRemotes(refs: Seq[DatasetRef]): Int = {
    refs
      .map(addDatasetFromRemote)
      .count(added => added)
  }

  @scala.annotation.tailrec
  private def addDatasetFromRemote(ref: DatasetRef): Boolean = {
    try {
      metadataRepository.addDatasetReference(ref)
      true
    } catch {
      case e: AlreadyExistsException =>
        if (replace) {
          logger.warn(e.getMessage + " - replacing")
          metadataRepository.deleteDataset(DatasetID(e.id))
          addDatasetFromRemote(ref)
        } else {
          logger.warn(e.getMessage + " - skipping")
          false
        }
    }
  }
}
