package dev.kamu.cli

import org.scalatest._

class KamuSQLShellSpec extends FlatSpec with Matchers with KamuTestBase {

  import spark.implicits._
  protected override val enableHiveSupport = false

  "kamu sql" should "be able to read a dataset" in {
    withEmptyWorkspace { kamu =>
      val input = sc
        .parallelize(
          Seq(
            ("Vancouver", "123"),
            ("Seattle", "321")
          )
        )
        .toDF("City", "Population")

      val ds = DatasetFactory.newRootDataset()
      kamu.addDataset(ds, input)

      val output = kamu
        .runEx(
          "sql",
          "-c",
          s"select * from parquet.`${ds.id}`",
          "-O",
          "csv"
        )
        .output
        .stripLineEnd

      println(output)

      val expected = """'City','Population'
                       |'Vancouver','123'
                       |'Seattle','321'""".stripMargin

      output shouldEqual expected
    }
  }
}
