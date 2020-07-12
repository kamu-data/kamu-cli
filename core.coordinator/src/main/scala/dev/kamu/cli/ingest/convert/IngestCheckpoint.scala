/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.convert

import java.time.Instant

import dev.kamu.core.manifests.MetadataBlock

case class IngestCheckpoint(
  prepTimestamp: Instant,
  lastIngested: Instant,
  resultingBlock: MetadataBlock
)
