package dev.kamu.cli

import java.net.URI

import dev.kamu.cli.external.{DockerClient, DockerProcessBuilder, DockerRunArgs}
import dev.kamu.core.manifests.DatasetID
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.Path.SEPARATOR
import org.scalatest._

import scala.concurrent.duration._

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

  test(raw"'kamu add' from HTTP URI") {
    withEmptyRepo { kamu =>
      val expected = DatasetFactory.newRootDataset()
      // create a temporary directory with the dataset to host
      val serverDir =
        new Path(kamu.config.repositoryRoot, "server")
      fileSystem.mkdirs(serverDir)
      val path: Path = new Path(serverDir, "test-dataset.yaml")
      kamu.metadataRepository.saveDataset(expected, path)

      // start up the server and host the directory
      val serverPort = 80 // httpd:2.4 default port
      val testServerName = "kamu_test_http_server"
      val testHttpServerArgs = DockerRunArgs(
        image = "httpd:2.4",
        exposePorts = List(serverPort),
        volumeMap = Map(serverDir -> new Path("/usr/local/apache2/htdocs")), // httpd:2.4 default location
        containerName = Some(testServerName),
        detached = true
      )
      val testHttpServer = new DockerClient(fileSystem)
      try {
        val testHttpServerProc = new DockerProcessBuilder(
          "http",
          testHttpServer,
          testHttpServerArgs
        ).run()
        testHttpServerProc.join()
        val hostPort =
          testHttpServerProc.waitForHostPort(serverPort, 1000 millis) // occasionally fails if set to 500 ms

        // pull the dataset from the server
        val actual =
          kamu.metadataRepository.loadDatasetFromURI(
            new URI(s"http://localhost:${hostPort}/test-dataset.yaml")
          )

        actual shouldEqual expected
      } finally {
        // stop the server
        testHttpServer.kill(testServerName)
      }
    }
  }

  test(raw"'kamu add' from file") {
    withEmptyRepo { kamu =>
      val expected = DatasetFactory.newRootDataset()
      val testDir =
        new Path(
          // Path(parent, child) throws an exception, while SEPARATOP
          // works for names with colons and spaces
          s"${kamu.config.repositoryRoot.toString}${SEPARATOR}test: add from file"
        )
      val path: Path = new Path(testDir, "test-dataset.yaml")
      fileSystem.mkdirs(testDir)
      kamu.metadataRepository.saveDataset(expected, path)
      val actual =
        kamu.metadataRepository.loadDatasetFromFile(path)

      actual shouldEqual expected
    }
  }
}
