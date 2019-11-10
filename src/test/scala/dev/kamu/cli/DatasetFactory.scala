package dev.kamu.cli

import java.net.URI

import dev.kamu.core.manifests._

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

  def newRootDataset(
    id: Option[DatasetID] = None,
    url: Option[URI] = None,
    format: Option[String] = None,
    header: Boolean = false,
    mergeStrategy: Option[MergeStrategyKind] = None,
    schema: Seq[String] = Seq.empty
  ): Dataset = {
    val _id = id.getOrElse(newDatasetID())
    Dataset(
      id = _id,
      rootPollingSource = Some(
        RootPollingSource(
          fetch = ExternalSourceFetchUrl(url.getOrElse(newURL(_id))),
          read = ReaderGeneric(
            name = format.getOrElse("csv"),
            options = if (!header) Map.empty else Map("header" -> "true"),
            schema = schema.toVector
          ),
          merge = mergeStrategy.getOrElse(MergeStrategyAppend())
        )
      )
    ).postLoad()
  }

  def newDerivativeDataset(
    source: DatasetID,
    id: Option[DatasetID] = None,
    sql: Option[String] = None
  ): Dataset = {
    val _id = id.getOrElse(newDatasetID())
    Dataset(
      id = _id,
      derivativeSource = Some(
        DerivativeSource(
          inputs = Vector(
            DerivativeInput(
              id = source
            )
          ),
          steps = Vector(
            ProcessingStepSQL(
              view = _id.toString,
              query = sql.getOrElse(s"SELECT * FROM `$source`")
            )
          )
        )
      )
    ).postLoad()
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
