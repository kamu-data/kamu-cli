package dev.kamu.cli.external

import dev.kamu.cli.{RepositoryVolumeMap, SparkConfig, UsageException}
import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Level

import scala.sys.process.Process

class SparkRunnerLocal(
  assemblyPath: Path,
  fileSystem: FileSystem,
  logLevel: Level,
  sparkConfig: SparkConfig
) extends SparkRunner(fileSystem, logLevel) {

  protected override def submit(
    appClass: String,
    repo: RepositoryVolumeMap,
    jars: Seq[Path],
    extraMounts: Seq[Path],
    loggingConfig: Path
  ): Unit = {
    val sparkSubmit = findSparkSubmitBin()

    val submitArgs = Seq(
      sparkSubmit.toString,
      "--master=local[4]",
      s"--driver-memory=${sparkConfig.driverMemory}",
      "--conf",
      s"spark.sql.warehouse.dir=$getSparkWarehouseDir",
      s"--class=$appClass"
    ) ++ (if (jars.nonEmpty) Seq(s"--jars=${jars.mkString(",")}") else Seq()) ++ Seq(
      assemblyPath.toString
    )

    logger.debug("Spark cmd: " + submitArgs.mkString(" "))

    val sparkProcess = Process(submitArgs)

    logger.info("Starting Spark job")

    val exitCode = sparkProcess.!

    if (exitCode != 0)
      throw new RuntimeException(
        s"Command failed with exit code $exitCode: ${submitArgs.mkString(" ")}"
      )
  }

  protected def findSparkSubmitBin(): Path = {
    val sparkHome = sys.env.get("SPARK_HOME")
    if (sparkHome.isEmpty)
      throw new UsageException(
        "Can't find $SPARK_HOME environment variable. " +
          "Make sure Spark is installed."
      )

    val sparkSubmit = new Path(sparkHome.get)
      .resolve("bin")
      .resolve("spark-submit")

    if (!fileSystem.isFile(sparkSubmit))
      throw new UsageException(
        s"Can't find spark-submit binary in ${sparkSubmit.toString}"
      )

    sparkSubmit
  }

  protected def getSparkWarehouseDir: Path = {
    new Path(sys.props("java.io.tmpdir")).resolve("spark-warehouse")
  }
}
