package dev.kamu.cli.external

import java.io.InputStream

import dev.kamu.cli.{MetadataRepository, RepositoryVolumeMap}
import dev.kamu.core.manifests.Resource
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

case class SparkSQLAppConfig(
  repositoryVolumeMap: RepositoryVolumeMap,
  command: Option[String]
) extends Resource[SparkSQLAppConfig]

// TODO: Remove in favor of JDBC client
class SparkSQLCommand(
  fileSystem: FileSystem,
  config: SparkSQLAppConfig,
  spark: SparkSession
) {
  val metadataRepository =
    new MetadataRepository(fileSystem, config.repositoryVolumeMap)

  def run(): Unit = {
    importDatasets(config.command.get)

    spark.sql(config.command.get).show()
  }

  def importDatasets(cmd: String): Unit = {
    val mentionedDatasets = metadataRepository
      .getAllDatasetIDs()
      .filter(id => cmd.contains(s"`$id`"))

    mentionedDatasets.foreach(id => {
      val df = spark.read.parquet(
        config.repositoryVolumeMap.dataDir
          .resolve(id.toString)
          .toString
      )

      df.createOrReplaceTempView(s"`$id`")
    })
  }
}

object SparkSQLApp {
  def main(args: Array[String]): Unit = {
    val fileSystem = FileSystem.get(hadoopConf)
    val config = loadConfig()
    val spark = sparkSession

    new SparkSQLCommand(fileSystem, config, spark).run()
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  def loadConfig(): SparkSQLAppConfig = {
    yaml.load[SparkSQLAppConfig](
      getConfigFromResources("sqlAppConfig.yaml")
    )
  }

  private def getConfigFromResources(configFileName: String): InputStream = {

    val configStream =
      getClass.getClassLoader.getResourceAsStream(configFileName)

    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath"
      )

    configStream
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  def sparkConf: SparkConf = {
    new SparkConf()
      .setAppName("transform.streaming")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  }

  def hadoopConf: org.apache.hadoop.conf.Configuration = {
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }

  def sparkSession: SparkSession = {
    val session = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(session)
    session
  }
}
