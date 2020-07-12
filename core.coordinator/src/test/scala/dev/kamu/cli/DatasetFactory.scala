/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.net.URI

import com.typesafe.config.ConfigObject
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._

import scala.util.Random

object DatasetFactory {
  private val _schemes = Array(
    "http",
    "https",
    "ftp"
  )

  private val _topLevelDomains = Array(
    "com",
    "org",
    "net",
    "edu",
    "gov"
  )

  private val _organizations = (0 to 100)
    .map(_ => Random.nextInt(10) + 3)
    .map(len => Random.alphanumeric.take(len).mkString.toLowerCase)
    .toArray

  private val _subdomains = Array(
    "api",
    "catalog",
    "data",
    "portal"
  )

  private val _extensions = Array(
    "zip",
    "tar.gz",
    "gz",
    "csv",
    "tsv"
  )

  def newRootDataset(
    id: Option[DatasetID] = None,
    url: Option[URI] = None,
    format: Option[String] = None,
    header: Boolean = false,
    mergeStrategy: Option[MergeStrategy] = None,
    schema: Seq[String] = Seq.empty
  ): DatasetSnapshot = {
    val _id = id.getOrElse(newDatasetID())
    DatasetSnapshot(
      id = _id,
      source = DatasetSource.Root(
        fetch = FetchStep.Url(url.getOrElse(newURL(_id))),
        read = ReadStep.Csv(
          header = Some(header),
          schema = Some(schema.toVector)
        ),
        merge = mergeStrategy.getOrElse(MergeStrategy.Append())
      )
    )
  }

  def newDerivativeDataset(
    source: DatasetID,
    id: Option[DatasetID] = None,
    sql: Option[String] = None
  ): DatasetSnapshot = {
    val _id = id.getOrElse(newDatasetID())
    val _sql = sql.getOrElse(s"SELECT * FROM `$source`")
    DatasetSnapshot(
      id = _id,
      source = DatasetSource.Derivative(
        inputs = Vector(source),
        transform = yaml.load[ConfigObject](
          s"""
            |engine: sparkSQL
            |query: '${_sql}'
          """.stripMargin
        )
      )
    )
  }

  def newDatasetID(): DatasetID = {
    val top = _topLevelDomains(Random.nextInt(_topLevelDomains.length))
    val org = _organizations(Random.nextInt(_organizations.length))
    val sub = _subdomains(Random.nextInt(_subdomains.length))

    DatasetID(Seq(top, org, sub).mkString("."))
  }

  def newURL(): URI = {
    newURL(newDatasetID())
  }

  def newURL(datasetID: DatasetID): URI = {
    val scheme = _schemes(Random.nextInt(_schemes.length))
    val host = datasetID.toString.split('.').reverse.mkString(".")
    val path = Random.alphanumeric.take(Random.nextInt(5) + 3).mkString
    val ext = _extensions(Random.nextInt(_extensions.length))
    URI.create(scheme + "://" + host + "/" + path + "." + ext)
  }
}
