/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest

import java.nio.file.Path

import better.files.File
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.Manifest
import org.apache.logging.log4j.LogManager
import pureconfig.{ConfigReader, ConfigWriter, Derivation}

case class ExecutionResult[TCheckpoint](
  wasUpToDate: Boolean,
  checkpoint: TCheckpoint
)

class CheckpointingExecutor[TCheckpoint]()(
  implicit icr: Derivation[ConfigReader[TCheckpoint]],
  icmr: Derivation[ConfigReader[Manifest[TCheckpoint]]],
  icw: Derivation[ConfigWriter[TCheckpoint]],
  icmw: Derivation[ConfigWriter[Manifest[TCheckpoint]]]
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def execute(
    checkpointPath: Path,
    execute: Option[TCheckpoint] => ExecutionResult[TCheckpoint]
  ): ExecutionResult[TCheckpoint] = {
    val storedCheckpoint = readCheckpoint(checkpointPath)

    if (storedCheckpoint.isDefined) {
      logger.debug(s"Stored checkpoint: ${storedCheckpoint.get}")
    } else {
      logger.debug("Fist time pass")
    }

    val executionResult = execute(storedCheckpoint)

    if (executionResult.wasUpToDate) {
      logger.debug("Up to date")
    } else {
      logger.debug("Saving checkpoint")
      writeCheckpoint(checkpointPath, executionResult.checkpoint)
    }

    executionResult
  }

  def readCheckpoint(
    checkpointPath: Path
  ): Option[TCheckpoint] = {
    if (!File(checkpointPath).exists)
      return None

    val cacheInfo = yaml.load[Manifest[TCheckpoint]](checkpointPath).content

    // TODO: detect when cache should be invalidated
    Some(cacheInfo)
  }

  def writeCheckpoint(checkpointPath: Path, checkpoint: TCheckpoint): Unit = {
    if (!File(checkpointPath.getParent).exists)
      File(checkpointPath.getParent).createDirectories()

    yaml.save(Manifest(checkpoint), checkpointPath)
  }
}
