package dev.kamu.cli

import org.scalatest._

class KamuPullDerivativeSpec extends FlatSpec with Matchers with KamuTestBase {

  import spark.implicits._
  protected override val enableHiveSupport = false

  "kamu pull" should "produce derivative datasets" in {
    withEmptyRepo { kamu =>
      val input = sc
        .parallelize(
          Seq(
            (ts(1), "Vancouver", "123"),
            (ts(1), "Seattle", "321")
          )
        )
        .toDF("systemTime", "city", "population")

      val root = DatasetFactory.newRootDataset()
      kamu.addDataset(root, input)

      // TODO: systemTime should not be propagated but assigned during transform
      val deriv = DatasetFactory.newDerivativeDataset(
        root.id,
        Some(
          s"SELECT systemTime, city, CAST(population AS INT) + 1 as population FROM `${root.id}`"
        )
      )

      kamu.addDataset(deriv)
      kamu.run("pull", deriv.id.toString)

      val result = kamu.readDataset(deriv.id)

      val expected = sc
        .parallelize(
          Seq(
            (ts(1), "Vancouver", 124),
            (ts(1), "Seattle", 322)
          )
        )
        .toDF("systemTime", "city", "population")

      assertDataFrameEquals(expected, result, true)
    }
  }
}
