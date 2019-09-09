package dev.kamu.cli

import dev.kamu.cli.output.{OutputFormat, SimpleResultSet}
import org.scalatest._

class KamuPullRootSpec extends FlatSpec with Matchers with KamuTestBase {

  import spark.implicits._
  protected override val enableHiveSupport = false

  "kamu pull" should "be able to import simple csv" in {
    withEmptyRepo { kamu =>
      val input = sc
        .parallelize(
          Seq(
            ("Vancouver", "123"),
            ("Seattle", "321")
          )
        )
        .toDF("City", "Population")

      val inputPath = kamu.writeData(input, OutputFormat.CSV)

      val ds = DatasetFactory.newRootDataset(
        url = Some(inputPath.toUri),
        format = Some("csv"),
        header = true
      )

      kamu.addDataset(ds)
      kamu.run("pull", ds.id.toString)

      val result = kamu.readDataset(ds.id)
      assertDataFrameEquals(input, result)
    }
  }
}
