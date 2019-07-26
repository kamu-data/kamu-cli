package dev.kamu.cli

import dev.kamu.core.manifests.{
  DataSource,
  DataSourcePolling,
  DatasetID,
  Manifest,
  RepositoryVolumeMap,
  TransformStreaming
}
import org.apache.hadoop.fs.{FileSystem, Path}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.utils.fs._
import org.apache.log4j.LogManager
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

class DoesNotExistsException(val datasetID: DatasetID)
    extends Exception(s"Dataset $datasetID does not exist")

class AlreadyExistsException(val datasetID: DatasetID)
    extends Exception(s"Dataset $datasetID already exists")

class MissingReferenceException(val fromID: DatasetID, val toID: DatasetID)
    extends Exception(s"Dataset $fromID refers to non existent dataset $toID")

class DanglingReferenceException(
  val fromIDs: Seq[DatasetID],
  val toID: DatasetID
) extends Exception(
      s"Dataset $toID is referenced by: " + fromIDs.mkString(", ")
    )

class MetadataRepository(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getDataSource(id: DatasetID): DataSource = {
    val path = repositoryVolumeMap.sourcesDir.resolve(id.toString + ".yaml")

    if (!fileSystem.exists(path))
      throw new DoesNotExistsException(id)

    loadDataSource(path)
  }

  def getDataSources(): Seq[DataSource] = {
    val manifestFiles = fileSystem
      .listStatus(repositoryVolumeMap.sourcesDir)
      .map(_.getPath)

    manifestFiles.map(loadDataSource)
  }

  def loadDataSource(p: Path): DataSource = {
    val inputStream = fileSystem.open(p)
    try {

      val rawYaml =
        new Yaml(new SafeConstructor())
          .load[java.util.Map[String, AnyRef]](inputStream)

      if (!rawYaml.containsKey("kind"))
        throw new RuntimeException("Malformed manifest")

      inputStream.seek(0)

      rawYaml.get("kind").asInstanceOf[String] match {
        case "DataSourcePolling" =>
          yaml.load[Manifest[DataSourcePolling]](inputStream).content
        case "TransformStreaming" =>
          yaml.load[Manifest[TransformStreaming]](inputStream).content
      }
    } catch {
      case e: Exception => {
        logger.error(s"Error while loading data source manifest: $p")
        throw e
      }
    } finally {
      inputStream.close()
    }
  }

  def addDataSource(ds: DataSource): Unit = {
    val path =
      repositoryVolumeMap.sourcesDir.resolve(ds.id + ".yaml")

    if (fileSystem.exists(path)) {
      throw new AlreadyExistsException(ds.id)
    } else {
      val outputStream = fileSystem.create(path)
      try {

        ds match {
          case ds: DataSourcePolling =>
            yaml.save(ds.asManifest, outputStream)
          case ds: TransformStreaming =>
            // Validate references
            try {
              ds.dependsOn.map(getDataSource)
            } catch {
              case e: DoesNotExistsException =>
                throw new MissingReferenceException(ds.id, e.datasetID)
            }
            yaml.save(ds.asManifest, outputStream)
        }

      } catch {
        case e: Exception =>
          outputStream.close()
          fileSystem.delete(path, false)
          throw e
      } finally {
        outputStream.close()
      }
    }
  }

  def deleteDataSource(id: DatasetID): Unit = {
    // Validate references
    val referencedBy = getDataSources().filter(_.dependsOn.contains(id))
    if (referencedBy.nonEmpty)
      throw new DanglingReferenceException(referencedBy.map(_.id), id)

    val path = repositoryVolumeMap.sourcesDir.resolve(id.toString + ".yaml")
    if (!fileSystem.exists(path))
      throw new DoesNotExistsException(id)

    getAllDataPaths(id).foreach(p => fileSystem.delete(p, true))
    fileSystem.delete(path, false)
  }

  def purgeDataSource(id: DatasetID): Unit = {
    getDataSource(id)
    getAllDataPaths(id).foreach(p => fileSystem.delete(p, true))
    // TODO: Purging a dataset that is used by non-empty derivatives should raise an error
  }

  def getAllDataPaths(id: DatasetID): Seq[Path] = {
    Seq(
      repositoryVolumeMap.dataDirRoot.resolve(id.toString),
      repositoryVolumeMap.dataDirDeriv.resolve(id.toString),
      repositoryVolumeMap.checkpointDir.resolve(id.toString),
      repositoryVolumeMap.downloadDir.resolve(id.toString)
    )
  }
}
