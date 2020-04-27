/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.fetch

import java.io.InputStream
import java.time.Instant

import dev.kamu.cli.ingest.ExecutionResult
import dev.kamu.core.utils.Clock
import org.apache.hadoop.fs.{FileSystem, Path}

class FileSystemSource(
  val sourceID: String,
  fileSystem: FileSystem,
  systemClock: Clock,
  val path: Path,
  eventTimeSource: EventTimeSource
) extends CacheableSource {

  override def maybeDownload(
    checkpoint: Option[DownloadCheckpoint],
    cachingBehavior: CachingBehavior,
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint] = {
    if (!cachingBehavior.shouldDownload(checkpoint))
      return ExecutionResult(
        wasUpToDate = true,
        checkpoint = checkpoint.get
      )

    logger.debug(s"FS stat $path")
    val fs = path.getFileSystem(fileSystem.getConf)

    val lastModified =
      Instant.ofEpochMilli(fs.getFileStatus(path).getModificationTime)

    val needsPull = checkpoint
      .flatMap(_.lastModified)
      .forall(lastModified.compareTo(_) > 0)

    if (needsPull) {
      logger.debug(s"FS reading $path")
      handler(fs.open(path))

      ExecutionResult(
        wasUpToDate = false,
        checkpoint = DownloadCheckpoint(
          lastDownloaded = systemClock.instant(),
          lastModified = Some(lastModified),
          eventTime = eventTimeSource.getEventTime(this)
        )
      )
    } else {
      ExecutionResult(
        wasUpToDate = true,
        checkpoint = checkpoint.get
      )
    }
  }
}
