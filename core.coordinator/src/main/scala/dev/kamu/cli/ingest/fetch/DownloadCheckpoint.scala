/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.fetch

import java.time.Instant

case class DownloadCheckpoint(
  eventTime: Option[Instant],
  lastDownloaded: Instant,
  lastModified: Option[Instant] = None,
  eTag: Option[String] = None
) {
  def isCacheable: Boolean = lastModified.isDefined || eTag.isDefined
}
