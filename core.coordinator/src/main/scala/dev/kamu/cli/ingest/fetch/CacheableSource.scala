/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.fetch

import java.io.InputStream

import dev.kamu.cli.ingest.ExecutionResult
import org.apache.log4j.LogManager

trait CacheableSource {
  protected val logger = LogManager.getLogger(getClass.getName)

  def sourceID: String

  def maybeDownload(
    checkpoint: Option[DownloadCheckpoint],
    cachingBehavior: CachingBehavior,
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint]
}
