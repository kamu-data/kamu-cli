package dev.kamu.cli

import java.io.{PrintStream, PrintWriter}
import java.util.UUID

import dev.kamu.cli.output.{DelimitedFormatter, OutputFormat, SimpleResultSet}
import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._

import scala.util.Random

trait KamuMixin extends TestSuiteMixin { this: TestSuite =>
  val fileSystem = FileSystem.get(new Configuration())

  def sysTempDir = new Path(System.getProperty("java.io.tmpdir"))

  val testDir = sysTempDir
    .resolve("kamu-test-" + UUID.randomUUID.toString)

  def withEmptyDir[T](func: KamuTestAdapter => T): T = {
    fileSystem.mkdirs(testDir)

    try {
      val config = KamuConfig(
        repositoryRoot = testDir
      )

      val kamu = new KamuTestAdapter(config, fileSystem)

      func(kamu)
    } finally {
      fileSystem.delete(testDir, true)
    }
  }

  def withEmptyRepo[T](func: KamuTestAdapter => T): T = {
    withEmptyDir { kamu =>
      kamu.run("init")
      func(kamu)
    }
  }

  def writeData(rs: SimpleResultSet, outputFormat: OutputFormat): Path = {
    val name = Random.alphanumeric.take(10).mkString + ".csv"
    val path = testDir.resolve(name)
    val stream = new PrintStream(fileSystem.create(path, false))
    val formatter = new DelimitedFormatter(stream, outputFormat)
    formatter.format(rs)
    stream.close()
    path
  }
}
