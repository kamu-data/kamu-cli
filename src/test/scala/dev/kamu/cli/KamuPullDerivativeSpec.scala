package dev.kamu.cli

import dev.kamu.core.manifests.ProcessingStepSQL
import org.scalatest._

class KamuPullDerivativeSpec extends FlatSpec with Matchers with KamuTestBase {

  import spark.implicits._
  protected override val enableHiveSupport = false

  "kamu pull" should "produce derivative datasets" in {
    withEmptyRepo { kamu =>
      val input = sc
        .parallelize(
          Seq(
            ("Vancouver", "123"),
            ("Seattle", "321")
          )
        )
        .toDF("City", "Population")

      val root = DatasetFactory.newRootDataset()
      kamu.addDataset(root, input)

      val deriv = DatasetFactory.newDerivativeDataset(
        root.id,
        Some(
          s"SELECT City, CAST(Population AS INT) + 1 as Population FROM `${root.id}`"
        )
      )

      kamu.addDataset(deriv)
      kamu.run("pull", deriv.id.toString)

      val result = kamu.readDataset(deriv.id)

      val expected = sc
        .parallelize(
          Seq(
            ("Vancouver", 124),
            ("Seattle", 322)
          )
        )
        .toDF("City", "Population")

      assertDataFrameEquals(expected, result, true)
    }
  }
}
