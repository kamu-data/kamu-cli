package dev.kamu.cli

import java.net.URI

import dev.kamu.core.manifests.DatasetID
import org.scalatest._
import dev.kamu.cli.external.{DockerClient, DockerProcessBuilder, DockerRunArgs}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration.Duration

class MetadataRepositorySpec extends FunSuite with Matchers with KamuTestBase {
  protected override val enableHiveSupport = false

  test("getDatasetsInDependencyOrder") {
    withEmptyRepo { kamu =>
      // A -> B -> C
      val dsC = DatasetFactory.newRootDataset(
        id = Some(DatasetID("C"))
      )
      val dsB = DatasetFactory.newDerivativeDataset(
        source = dsC.id,
        id = Some(DatasetID("B"))
      )
      val dsA = DatasetFactory.newDerivativeDataset(
        source = dsB.id,
        id = Some(DatasetID("A"))
      )

      kamu.addDataset(dsC)
      kamu.addDataset(dsB)
      kamu.addDataset(dsA)

      val actual1 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id, dsB.id, dsC.id),
          recursive = false
        )
        .map(_.id)

      actual1 shouldEqual Seq(dsC.id, dsB.id, dsA.id)

      val actual2 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id, dsB.id),
          recursive = false
        )
        .map(_.id)

      actual2 shouldEqual Seq(dsB.id, dsA.id)

      val actual3 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id),
          recursive = true
        )
        .map(_.id)

      actual3 shouldEqual Seq(dsC.id, dsB.id, dsA.id)
    }
  }

  test(raw"'kamu add' from HTTP") {
    withEmptyRepo { kamu =>
      val expected = DatasetFactory.newRootDataset()

      // create a temporary directory with the dataset to host
      val serverDir = getRandomDir()
      fileSystem.mkdirs(serverDir)
      val path: Path = new Path(serverDir, raw"test-dataset.yaml")
      kamu.metadataRepository.saveDataset(expected, path)

      // start up the server and host the directory
      val serverPort = 80 // httpd:2.4 default port
      val testServerName = s"kamu_test_http_server"
      val testHttpServerArgs = DockerRunArgs(
        image = "httpd:2.4",
        exposePorts = List(serverPort),
        volumeMap = Map(serverDir -> new Path("/usr/local/apache2/htdocs")), // httpd:2.4 default location
        containerName = Some(testServerName),
        detached = true
      )
      val testHttpServer = new DockerClient(fileSystem)
      val testHttpServerProc = new DockerProcessBuilder(
        "http",
        testHttpServer,
        testHttpServerArgs
      ).run()
      testHttpServerProc.join()
      val hostPort =
        testHttpServerProc.waitForHostPort(serverPort, Duration("500 ms"))

      // pull the dataset from the server
      val actual =
        kamu.metadataRepository.loadDatasetFromURI(
          new URI(s"http://localhost:${hostPort}/test-dataset.yaml")
        )

      // stop the server
      testHttpServer.kill(testServerName)
      fileSystem.delete(serverDir, true)

      actual shouldEqual expected
    }
  }

  test(raw"'kamu add' from file") {
    withEmptyRepo { kamu =>
      val expected = DatasetFactory.newRootDataset()
      val testDir = getRandomDir()
      val path: Path = new Path(testDir, raw"test-dataset.yaml")
      fileSystem.mkdirs(testDir)
      kamu.metadataRepository.saveDataset(expected, path)
      val actual =
        kamu.metadataRepository.loadDatasetFromFile(path)
      fileSystem.delete(testDir, true)

      actual shouldEqual expected
    }
  }
}
