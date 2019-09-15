package dev.kamu.cli

import java.sql.Timestamp
import java.util.UUID

import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._

trait KamuTestBase extends DataFrameSuiteBaseEx { self: Suite =>

  val fileSystem = FileSystem.get(new Configuration())

  def getSystemTempDir(): Path =
    new Path(System.getProperty("java.io.tmpdir"))

  def getRandomDir(): Path =
    getSystemTempDir()
      .resolve("kamu-test-" + UUID.randomUUID.toString)

  def withEmptyDir[T](func: KamuTestAdapter => T): T = {
    val testDir = getRandomDir()
    fileSystem.mkdirs(testDir)

    try {
      val config = KamuConfig(
        repositoryRoot = testDir
      )

      val kamu = new KamuTestAdapter(config, fileSystem, spark)

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

  def ts(milis: Long) = new Timestamp(milis)

}
