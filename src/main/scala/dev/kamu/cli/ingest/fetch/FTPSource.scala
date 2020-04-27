/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.fetch

import java.io.InputStream
import java.net.URI

import dev.kamu.cli.ingest.ExecutionResult
import dev.kamu.core.utils.Clock
import org.apache.commons.net.ftp.{FTP, FTPClient}

class FTPSource(
  val sourceID: String,
  systemClock: Clock,
  url: URI,
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

    logger.debug(s"FTP request $url")

    val client = new FTPClient()
    client.connect(url.getHost)
    client.login("anonymous", "")
    client.enterLocalPassiveMode()
    client.setFileType(FTP.BINARY_FILE_TYPE)

    val inputStream = client.retrieveFileStream(url.getPath)
    handler(inputStream)

    val success = client.completePendingCommand()
    inputStream.close()

    if (!success)
      throw new RuntimeException(s"FTP download failed")

    ExecutionResult(
      wasUpToDate = false,
      checkpoint = DownloadCheckpoint(
        lastDownloaded = systemClock.instant(),
        eventTime = eventTimeSource.getEventTime(this)
      )
    )
  }
}
