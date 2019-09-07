package dev.kamu.cli

import dev.kamu.core.manifests.DatasetID
import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest._

class KamuPullDerivativeSpec
    extends FlatSpec
    with Matchers
    with KamuMixin
    with DataFrameSuiteBaseEx {

  import spark.implicits._
  protected override val enableHiveSupport = false

  def writeCSV(df: DataFrame, name: String): Path = {
    val dataDir = testDir.resolve(name)

    df.repartition(1)
      .write
      .option("header", "true")
      .csv(dataDir.toString)

    fileSystem
      .listStatus(dataDir)
      .filter(_.getPath.getName.startsWith("part"))
      .head
      .getPath
  }

  def readDataset(kamu: KamuTestAdapter, id: DatasetID): DataFrame = {
    spark.read.parquet(
      kamu.repositoryVolumeMap.dataDir
        .resolve(id.toString)
        .toUri
        .getPath
    )
  }

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

      val inputPath = writeCSV(input, "input.csv")

      val ds = DatasetFactory.newRootDataset(
        url = Some(inputPath.toUri),
        format = Some("csv"),
        header = true
      )

      kamu.addDataset(ds)
      kamu.run("pull", ds.id.toString)

      val result = readDataset(kamu, ds.id)
      assertDataFrameEquals(input, result)
    }
  }
}
