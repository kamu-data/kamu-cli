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
import java.nio.file.{Path, Paths}

import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import dev.kamu.core.manifests.Manifest
import org.apache.logging.log4j.LogManager
import pureconfig.{ConfigReader, ConfigWriter, Derivation}

import scala.reflect.ClassTag

// TODO: Remove this class?
class ResourceLoader() {
  private val logger = LogManager.getLogger(getClass.getName)

  def loadResourceFromFile[T: ClassTag](p: Path)(
    implicit reader: Derivation[ConfigReader[Manifest[T]]]
  ): T = {
    yaml.load[Manifest[T]](p).content
  }

  def saveResourceToFile[T: ClassTag](res: T, path: Path)(
    implicit writer: Derivation[ConfigWriter[Manifest[T]]]
  ): Unit = {
    yaml.save(Manifest(res), path)
  }

  def loadResourceFromURI[T: ClassTag](
    uri: URI
  )(
    implicit reader: Derivation[ConfigReader[Manifest[T]]]
  ): T = {
    uri.getScheme match {
      case "https" => loadResourceFromURL(uri.toURL)
      case "http"  => loadResourceFromURL(uri.toURL)
      case "file"  => loadResourceFromFile(Paths.get(uri))
      case null    => loadResourceFromFile(Paths.get(uri.toString))
      case s       => throw new SchemaNotSupportedException(s)
    }
  }

  private def loadResourceFromURL[T: ClassTag](
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
