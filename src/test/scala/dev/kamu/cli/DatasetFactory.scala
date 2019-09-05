package dev.kamu.cli

import java.net.URI

import dev.kamu.core.manifests.{
  Dataset,
  DatasetID,
  DerivativeInput,
  DerivativeSource,
  ProcessingStepSQL,
  RootPollingSource
}

import scala.util.Random

object DatasetFactory {
  val _schemes = Array(
    "http",
    "https",
    "ftp"
  )

  val _topLevelDomains = Array(
    "com",
    "org",
    "net",
    "edu",
    "gov"
  )

  val _organizations = (0 to 100)
    .map(_ => Random.nextInt(10) + 3)
    .map(len => Random.alphanumeric.take(len).mkString.toLowerCase)
    .toArray

  val _subdomains = Array(
    "api",
    "catalog",
    "data",
    "portal"
  )

  val _extensions = Array(
    "zip",
    "tar.gz",
    "gz",
    "csv",
    "tsv"
  )

  def newRootDataset(): Dataset = {
    val id = newDatasetID()
    Dataset(
      id = id,
      rootPollingSource = Some(
        RootPollingSource(
          url = newURL(id),
          format = "csv"
        )
      )
    )
  }

  def newDerivativeDataset(sourceID: DatasetID): Dataset = {
    val id = newDatasetID()
    Dataset(
      id = id,
      derivativeSource = Some(
        DerivativeSource(
          inputs = Vector(
            DerivativeInput(
              id = sourceID
            )
          ),
          steps = Vector(
            ProcessingStepSQL(
              view = id.toString,
              query = s"SELECT * FROM `$sourceID`"
            )
          )
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
