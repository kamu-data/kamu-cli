package dev.kamu.cli

import dev.kamu.core.manifests.{Dataset, DatasetID, RootPollingSource}
import org.scalatest._
import java.net.{URI, URL}

class MetadataRepositorySpec extends FunSuite with Matchers with KamuTestBase {
  protected override val enableHiveSupport = false

  test("getDatasetsInDependencyOrder") {
    withEmptyRepo { kamu =>
      // A -> B -> C
      val dsC = DatasetFactory.newRootDataset(
        id = Some(DatasetID("C"))
      )
      val dsB = DatasetFactory.newDerivativeDataset(
        source = dsC.id,
        id = Some(DatasetID("B"))
      )
      val dsA = DatasetFactory.newDerivativeDataset(
        source = dsB.id,
        id = Some(DatasetID("A"))
      )

      kamu.addDataset(dsC)
      kamu.addDataset(dsB)
      kamu.addDataset(dsA)

      val actual1 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id, dsB.id, dsC.id),
          recursive = false
        )
        .map(_.id)

      actual1 shouldEqual Seq(dsC.id, dsB.id, dsA.id)

      val actual2 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id, dsB.id),
          recursive = false
        )
        .map(_.id)

      actual2 shouldEqual Seq(dsB.id, dsA.id)

      val actual3 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id),
          recursive = true
        )
        .map(_.id)

      actual3 shouldEqual Seq(dsC.id, dsB.id, dsA.id)
    }
  }

  test(raw"'kamu add' from HTTP") {
    withEmptyRepo { kamu =>
      val actual =
        kamu.metadataRepository.loadDatasetFromURI(
          raw"http://localhost:9000/example-dataset.yaml"
        )

      val expected = Dataset(
        DatasetID(raw"dev.kamu.example"),
        Some(
          RootPollingSource(
            new URI("ftp://kamu.dev/opendata/csv/example-data.zip"),
            Some("zip"),
            None,
            None,
            "csv",
            Map(
              "header" -> "true",
              "sep" -> ",",
              "quote" -> "\"",
              "escape" -> "\"",
              "multiline" -> "true",
              "nullValue" -> ""
            )
          )
        )
      ).postLoad()

      actual shouldEqual expected
    }
  }

  test(raw"'kamu add' from file") {
    withEmptyRepo { kamu =>
      val actual =
        kamu.metadataRepository.loadDatasetFromURI(
          raw"./testsuit-extras/test-http-server/example-dataset.yaml"
        )

      val expected = Dataset(
        DatasetID(raw"dev.kamu.example"),
        Some(
          RootPollingSource(
            new URI("ftp://kamu.dev/opendata/csv/example-data.zip"),
            Some("zip"),
            None,
            None,
            "csv",
            Map(
              "header" -> "true",
              "sep" -> ",",
              "quote" -> "\"",
              "escape" -> "\"",
              "multiline" -> "true",
              "nullValue" -> ""
            )
          )
        )
      ).postLoad()

      actual shouldEqual expected
    }
  }
}
