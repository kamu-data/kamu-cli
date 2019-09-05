package dev.kamu.cli

import java.util.UUID

import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._

trait KamuMixin extends TestSuiteMixin { this: TestSuite =>
  val fileSystem = FileSystem.get(new Configuration())
  fileSystem.setWriteChecksum(false)
  fileSystem.setVerifyChecksum(false)

  def sysTempDir = new Path(System.getProperty("java.io.tmpdir"))

  def withEmptyDir[T](func: KamuTestAdapter => T): T = {
    val testDir = sysTempDir
      .resolve("kamu-test-" + UUID.randomUUID.toString)

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
}
