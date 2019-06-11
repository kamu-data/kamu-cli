package dev.kamu.cli

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.RepositoryVolumeMap

import scala.sys.process.Process

class SparkRunnerLocal(
  fileSystem: FileSystem
) extends SparkRunner {
  protected val logger = LogManager.getLogger(getClass.getName)

  override def submit(
    repo: RepositoryVolumeMap,
    appClass: String,
    jars: Seq[Path]
  ): Unit = {
    val sparkSubmit = findSparkSubmitBin()

    val submitArgs = Seq(
      sparkSubmit.toString,
      "--master=local[4]",
      "--conf",
      s"spark.sql.warehouse.dir=$getSparkWarehouseDir",
      s"--class=$appClass",
      s"--jars=${jars.mkString(",")}",
      assemblyPath.toString
    )

    logger.debug("Spark cmd: " + submitArgs.mkString(" "))

    val sparkProcess = Process(submitArgs)

    /*val processIO = new ProcessIO(
      _ => (),
      stdout =>
        scala.io.Source.fromInputStream(stdout).getLines.foreach(println),
      _ => ()
    )*/

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
