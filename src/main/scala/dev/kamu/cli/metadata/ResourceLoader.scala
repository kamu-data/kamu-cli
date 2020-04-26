/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.metadata

/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import java.net.URI

import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import dev.kamu.core.manifests.{Manifest, Resource}
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import pureconfig.{ConfigReader, ConfigWriter, Derivation}

import scala.reflect.ClassTag

// TODO: Remove this class?
class ResourceLoader(
  fileSystem: FileSystem
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def loadResourceFromFile[T <: Resource: ClassTag](
    p: Path
  )(
    implicit reader: Derivation[ConfigReader[Manifest[T]]]
  ): T = {
    val inputStream = fileSystem.open(p)
    try {
      yaml.load[Manifest[T]](inputStream).content
    } finally {
      inputStream.close()
    }
  }

  def saveResourceToFile[T <: Resource: ClassTag](
    res: T,
    path: Path
  )(
    implicit writer: Derivation[ConfigWriter[Manifest[T]]]
  ): Unit = {
    val outputStream = fileSystem.create(path)

    try {
      yaml.save(Manifest(res), outputStream)
    } catch {
      case e: Exception =>
        outputStream.close()
        fileSystem.delete(path, false)
        throw e
    } finally {
      outputStream.close()
    }
  }

  def loadResourceFromURI[T <: Resource: ClassTag](
    uri: URI
  )(
    implicit reader: Derivation[ConfigReader[Manifest[T]]]
  ): T = {
    uri.getScheme match {
      case "https"       => loadResourceFromURL(uri.toURL)
      case "http"        => loadResourceFromURL(uri.toURL)
      case null | "file" => loadResourceFromFile(new Path(uri.getPath))
      case s             => throw new SchemaNotSupportedException(s)
    }
  }

  private def loadResourceFromURL[T <: Resource: ClassTag](
    url: java.net.URL
  )(
    implicit reader: Derivation[ConfigReader[Manifest[T]]]
  ): T = {
    val source = scala.io.Source.fromURL(url)
    try {
      yaml.load[Manifest[T]](source.mkString).content
    } finally {
      source.close()
    }
  }
}
