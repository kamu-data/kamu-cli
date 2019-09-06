package dev.kamu.cli

import dev.kamu.cli.output.{OutputFormat, SimpleResultSet}
import org.scalatest._

class KamuPullRootSpec extends FlatSpec with Matchers with KamuMixin {
  "kamu pull" should "be able to import simple csv" in {
    withEmptyRepo { kamu =>
      val rs = new SimpleResultSet()
      rs.addColumns("City", "Population")
      rs.addRow("Vancouver", 123)
      rs.addRow("Seattle", 321)

      val dataPath = writeData(rs, OutputFormat.CSV)

      val ds = DatasetFactory.newRootDataset(
        url = Some(dataPath.toUri),
        format = Some("csv"),
        header = true
      )

      kamu.addDataset(ds)
      kamu.run("pull", ds.id.toString)

      val output = kamu
        .runEx(
          "sql",
          "-c",
          s"select * from parquet.`${ds.id}`"
        )
        .output
        .stripLineEnd
        .stripLineEnd

      val expected = """|+-----------+------------+
                        ||   City    | Population |
                        |+-----------+------------+
                        || Vancouver | 123        |
                        || Seattle   | 321        |
                        |+-----------+------------+
                        |""".stripMargin.stripLineEnd

      output shouldEqual expected
    }
  }
}
