package dev.kamu.cli

import dev.kamu.core.manifests.{
  Dataset,
  DatasetID,
  Manifest,
  RepositoryVolumeMap
}
import org.apache.hadoop.fs.{FileSystem, Path}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.utils.fs._
import org.apache.log4j.LogManager

class MetadataRepository(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getDataset(id: DatasetID): Dataset = {
    val path = repositoryVolumeMap.sourcesDir.resolve(id.toString + ".yaml")

    if (!fileSystem.exists(path))
      throw new DoesNotExistsException(id)

    loadDatasetFromFile(path)
  }

  def getAllDatasetIDs(): Seq[DatasetID] = {
    fileSystem
      .listStatus(repositoryVolumeMap.sourcesDir)
      .map(_.getPath.getName)
      .map(filename => filename.substring(0, filename.length - ".yaml".length))
      .map(DatasetID)
  }

  def getAllDatasets(): Seq[Dataset] = {
    val manifestFiles = fileSystem
      .listStatus(repositoryVolumeMap.sourcesDir)
      .map(_.getPath)

    manifestFiles.map(loadDatasetFromFile)
  }

  def loadDatasetFromFile(p: Path): Dataset = {
    val inputStream = fileSystem.open(p)
    try {
      yaml.load[Manifest[Dataset]](inputStream).content
    } catch {
      case e: Exception =>
        logger.error(s"Error while loading data source manifest: $p")
        throw e
    } finally {
      inputStream.close()
    }
  }

  def addDataset(ds: Dataset): Unit = {
    val path =
      repositoryVolumeMap.sourcesDir.resolve(ds.id + ".yaml")

    if (fileSystem.exists(path))
      throw new AlreadyExistsException(ds.id)

    try {
      ds.dependsOn.map(getDataset)
    } catch {
      case e: DoesNotExistsException =>
        throw new MissingReferenceException(ds.id, e.datasetID)
    }

    saveDataset(ds, path)
  }

  def saveDataset(ds: Dataset, path: Path): Unit = {
    val outputStream = fileSystem.create(path)

    try {
      yaml.save(ds.asManifest, outputStream)
    } catch {
      case e: Exception =>
        outputStream.close()
        fileSystem.delete(path, false)
        throw e
    } finally {
      outputStream.close()
    }
  }

  def deleteDataset(id: DatasetID): Unit = {
    // Validate references
    val referencedBy = getAllDatasets().filter(_.dependsOn.contains(id))
    if (referencedBy.nonEmpty)
      throw new DanglingReferenceException(referencedBy.map(_.id), id)

    val path = repositoryVolumeMap.sourcesDir.resolve(id.toString + ".yaml")
    if (!fileSystem.exists(path))
      throw new DoesNotExistsException(id)

    getAllDataPaths(id).foreach(p => fileSystem.delete(p, true))
    fileSystem.delete(path, false)
  }

  def purgeDataset(id: DatasetID): Unit = {
    getDataset(id)
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

/////////////////////////////////////////////////////////////////////////////////////////
// Exceptions
/////////////////////////////////////////////////////////////////////////////////////////

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
