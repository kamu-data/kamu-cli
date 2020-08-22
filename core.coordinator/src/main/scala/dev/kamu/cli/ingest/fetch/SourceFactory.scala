/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.fetch

import java.nio.file.Paths

import better.files.File
import dev.kamu.core.manifests
import dev.kamu.core.manifests._
import dev.kamu.core.utils.Clock
import org.apache.logging.log4j.LogManager

class SourceFactory(systemClock: Clock) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getSource(fetch: FetchStep): Seq[CacheableSource] = {
    fetch match {
      case url: FetchStep.Url =>
        getFetchUrlSource(url, getEventTimeSource(url.eventTime))
      case glob: FetchStep.FilesGlob =>
        getFetchFilesGlobSource(glob, getEventTimeSource(glob.eventTime))
    }
  }

  def getEventTimeSource(
    eventTimeSource: Option[manifests.EventTimeSource]
  ): EventTimeSource = {
    eventTimeSource match {
      case None =>
        new EventTimeSource.FromSystemTime(systemClock)
      case Some(e: manifests.EventTimeSource.FromPath) =>
        new EventTimeSource.FromPath(
          e.pattern,
          e.timestampFormat.getOrElse("yyyy-MM-dd")
        )
      case _ =>
        throw new NotImplementedError(
          s"Unsupported event time source: ${eventTimeSource}"
        )
    }
  }

  def getFetchUrlSource(
    kind: FetchStep.Url,
    eventTimeSource: EventTimeSource
  ): Seq[CacheableSource] = {
    kind.url.getScheme match {
      case "http" | "https" =>
        Seq(
          new HTTPSource(
            "primary",
            systemClock,
            kind.url,
            eventTimeSource
          )
        )
      case "ftp" =>
        Seq(
          new FTPSource(
            "primary",
            systemClock,
            kind.url,
            eventTimeSource
          )
        )
      case "gs" | "hdfs" | "file" | null =>
        // TODO: restrict allowed source paths for security
        Seq(
          new FileSystemSource(
            "primary",
            systemClock,
            Paths.get(kind.url),
            eventTimeSource
          )
        )
      case _ =>
        throw new NotImplementedError(s"Unsupported source: ${kind.url}")
    }
  }

  def getFetchFilesGlobSource(
    kind: FetchStep.FilesGlob,
    eventTimeSource: EventTimeSource
  ): Seq[CacheableSource] = {
    val path = Paths.get(kind.path)
    val globbed = File(path.getParent)
      .glob(path.getFileName.toString)
      .map(
        f =>
          new FileSystemSource(
            f.name,
            systemClock,
            f.path,
            eventTimeSource
          )
      )
      .toVector

    val orderBy = kind.order.getOrElse(
      if (kind.eventTime.isDefined) SourceOrdering.ByEventTime
      else SourceOrdering.ByName
    )

    val sorted = orderBy match {
      case SourceOrdering.ByName =>
        globbed.sortBy(_.path.getFileName.toString.toLowerCase())
      case SourceOrdering.ByEventTime =>
        globbed.sortBy(eventTimeSource.getEventTime)
    }

    logger.debug(
      s"Glob pattern resolved to: ${sorted.map(_.path.getFileName).mkString(", ")}"
    )

    sorted
  }

  def getCachingBehavior(kind: FetchStep): CachingBehavior = {
    val cacheSettings = kind match {
      case url: FetchStep.Url        => url.cache
      case glob: FetchStep.FilesGlob => glob.cache
    }
    cacheSettings match {
      case None                           => new CachingBehaviorDefault()
      case Some(_: SourceCaching.Forever) => new CachingBehaviorForever()
    }
  }
}
