/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.net.URI

import dev.kamu.core.manifests.{DatasetID, Manifest, Resource}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import pureconfig.{ConfigReader, ConfigWriter, Derivation}

import scala.reflect.ClassTag

class GenericResourceRepository[TRes <: Resource[TRes]: ClassTag, TID](
  fileSystem: FileSystem,
  storagePath: Path,
  resourceKind: String,
  idFromString: String => TID,
  idFromRes: TRes => TID
)(
  implicit rderivation: Derivation[ConfigReader[Manifest[TRes]]],
  wderivation: Derivation[ConfigWriter[Manifest[TRes]]]
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getResourcePath(id: TID): Path = {
    storagePath.resolve(id.toString + ".yaml")
  }

  def getAllResourceIDs(): Seq[TID] = {
    fileSystem
      .listStatus(storagePath)
      .map(_.getPath.getName)
      .map(filename => filename.substring(0, filename.length - ".yaml".length))
      .map(idFromString)
  }

  def getResource(id: TID): TRes = {
    getResourceOpt(id) match {
      case None     => throw new DoesNotExistException(id.toString, resourceKind)
      case Some(ds) => ds
    }
  }

  def getResourceOpt(id: TID): Option[TRes] = {
    val path = getResourcePath(id)

    if (!fileSystem.exists(path))
      None
    else
      Some(loadResourceFromFile(path))
  }

  def getAllResources(): Seq[TRes] = {
    val resourceFiles = fileSystem
      .listStatus(storagePath)
      .map(_.getPath)

    resourceFiles.map(loadResourceFromFile)
  }

  def addResource(res: TRes): Unit = {
    val id = idFromRes(res)
    val path = getResourcePath(id)

    if (fileSystem.exists(path))
      throw new AlreadyExistsException(
        id.toString,
        resourceKind
      )

    saveResource(res)
  }

  def deleteResource(id: TID): Unit = {
    val path = getResourcePath(id)

    if (!fileSystem.exists(path))
      throw new DoesNotExistException(id.toString, resourceKind)

    fileSystem.delete(path, false)
  }

  def loadResourceFromFile(p: Path): TRes = {
    val inputStream = fileSystem.open(p)
    try {
      yaml.load[Manifest[TRes]](inputStream).content
    } catch {
      case e: Exception =>
        logger.error(s"Error while loading $resourceKind from file: $p")
        throw e
    } finally {
      inputStream.close()
    }
  }

  def saveResource(res: TRes): Unit = {
    val path = getResourcePath(idFromRes(res))
    saveResourceToFile(res, path)
  }

  def saveResourceToFile(res: TRes, path: Path): Unit = {
    val outputStream = fileSystem.create(path)

    try {
      yaml.save(res.asManifest, outputStream)
    } catch {
      case e: Exception =>
        outputStream.close()
        fileSystem.delete(path, false)
        throw e
    } finally {
      outputStream.close()
    }
  }

  def loadResourceFromURI(uri: URI): TRes = {
    uri.getScheme match {
      case "https"       => loadResourceFromURL(uri.toURL)
      case "http"        => loadResourceFromURL(uri.toURL)
      case null | "file" => loadResourceFromFile(new Path(uri.getPath))
      case s             => throw new SchemaNotSupportedException(s)
    }
  }

  private def loadResourceFromURL(url: java.net.URL): TRes = {
    val source = scala.io.Source.fromURL(url)
    try {
      yaml.load[Manifest[TRes]](source.mkString).content
    } catch {
      case e: Exception =>
        logger.error(
          s"Error while loading ${resourceKind} manifest from URL: $url"
        )
        throw e
    } finally {
      source.close()
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Exceptions
/////////////////////////////////////////////////////////////////////////////////////////

class DoesNotExistException(
  val id: String,
  val kind: String
) extends Exception(s"${kind.capitalize} $id does not exist")

class AlreadyExistsException(
  val id: String,
  val kind: String
) extends Exception(s"${kind.capitalize} $id already exists")

class MissingReferenceException(
  val fromID: String,
  val fromKind: String,
  val toID: String,
  val toKind: String
) extends Exception(
      s"${fromKind.capitalize} $fromID refers to non existent $toKind $toID"
    )

class SchemaNotSupportedException(val schema: String)
    extends Exception(s"$schema")

class DanglingReferenceException(
  val fromIDs: Seq[DatasetID],
  val toID: DatasetID
) extends Exception(
      s"Dataset $toID is referenced by: " + fromIDs.mkString(", ")
    )
