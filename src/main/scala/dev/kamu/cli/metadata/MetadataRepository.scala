/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.metadata

import java.io.PrintWriter
import java.net.URI

import dev.kamu.cli.utility.DependencyGraph
import dev.kamu.cli._
import dev.kamu.core.manifests.infra.MetadataChainFS
import dev.kamu.core.manifests._
import dev.kamu.core.utils.Clock
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

class MetadataRepository(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout,
  systemClock: Clock
) {
  private val logger = LogManager.getLogger(getClass.getName)

  protected val volumeRepo =
    new GenericResourceRepository[Remote, RemoteID](
      fileSystem,
      workspaceLayout.remotesDir,
      "remote",
      RemoteID,
      (remote: Remote) => remote.id
    )

  ////////////////////////////////////////////////////////////////////////////
  // Datasets
  ////////////////////////////////////////////////////////////////////////////

  protected def remoteRefFilePath(id: DatasetID): Path = {
    workspaceLayout.metadataDir.resolve(id.toString).resolve("ref.yaml")
  }

  protected def datasetMetadataDir(id: DatasetID): Path = {
    if (isRemote(id))
      getLocalVolume().metadataDir.resolve(id.toString)
    else
      workspaceLayout.metadataDir.resolve(id.toString)
  }

  protected def ensureDatasetExists(id: DatasetID): Unit = {
    if (!fileSystem.exists(workspaceLayout.metadataDir.resolve(id.toString)))
      throw new DoesNotExistException(id.toString, "dataset")
  }

  protected def ensureDatasetExistsAndPulled(id: DatasetID): Unit = {
    ensureDatasetExists(id)
    if (!fileSystem.exists(datasetMetadataDir(id)))
      throw new DoesNotExistException(id.toString, "dataset")
  }

  def isRemote(id: DatasetID): Boolean = {
    val refPath = remoteRefFilePath(id)
    fileSystem.exists(refPath)
  }

  def getDatasetLayout(id: DatasetID): DatasetLayout = {
    ensureDatasetExists(id)

    val localVolume = getLocalVolume()

    DatasetLayout(
      metadataDir = datasetMetadataDir(id),
      dataDir = localVolume.dataDir.resolve(id.toString),
      checkpointsDir = localVolume.checkpointsDir.resolve(id.toString),
      cacheDir = localVolume.cacheDir.resolve(id.toString)
    )
  }

  def getMetadataChain(id: DatasetID): MetadataChainFS = {
    new MetadataChainFS(fileSystem, getDatasetLayout(id).metadataDir)
  }

  def getDatasetKind(id: DatasetID): DatasetKind = {
    ensureDatasetExists(id)
    if (isRemote(id))
      DatasetKind.Remote
    else
      getDatasetSummary(id).kind
  }

  def getDatasetSummary(id: DatasetID): DatasetSummary = {
    ensureDatasetExistsAndPulled(id)

    val chain = new MetadataChainFS(fileSystem, datasetMetadataDir(id))
    chain.getSummary()
  }

  def getDatasetRef(id: DatasetID): DatasetRef = {
    if (!isRemote(id))
      throw new RuntimeException(s"Dataset $id is not remote")

    val refFile = remoteRefFilePath(id)

    new ResourceLoader(fileSystem)
      .loadResourceFromFile[DatasetRef](refFile)
  }

  protected def getDatasetDependencies(id: DatasetID): List[DatasetID] = {
    if (isRemote(id))
      List.empty
    else
      getDatasetSummary(id).datasetDependencies.toList
  }

  def getDatasetsInDependencyOrder(
    ids: Seq[DatasetID],
    recursive: Boolean
  ): Seq[DatasetID] = {
    // TODO: Check recursive implemented correctly
    val dependsOn =
      if (recursive)
        getDatasetDependencies _
      else
        getDatasetDependencies(_: DatasetID).filter(ids.contains)

    val depGraph = new DependencyGraph[DatasetID](dependsOn)
    depGraph.resolve(ids.toList)
  }

  def getAllDatasets(): Seq[DatasetID] = {
    fileSystem
      .listStatus(workspaceLayout.metadataDir)
      .map(_.getPath.getName)
      .map(DatasetID)
  }

  def loadDatasetSnapshotFromURI(uri: URI): DatasetSnapshot = {
    new ResourceLoader(fileSystem).loadResourceFromURI[DatasetSnapshot](uri)
  }

  def addDataset(ds: DatasetSnapshot): Unit = {
    val datasetDir = workspaceLayout.metadataDir.resolve(ds.id.toString)

    if (fileSystem.exists(datasetDir))
      throw new AlreadyExistsException(ds.id.toString, "dataset")

    try {
      ds.dependsOn.foreach(ensureDatasetExists)
    } catch {
      case e: DoesNotExistException =>
        throw new MissingReferenceException(
          ds.id.toString,
          "dataset",
          e.id,
          e.kind
        )
    }

    val chain = new MetadataChainFS(fileSystem, datasetDir)
    chain.init(ds, systemClock.instant())
  }

  def addDatasetReference(datasetRef: DatasetRef): Unit = {
    val localDatasetID = datasetRef.alias.getOrElse(datasetRef.datasetID)
    val datasetDir =
      workspaceLayout.metadataDir.resolve(localDatasetID.toString)

    if (fileSystem.exists(datasetDir))
      throw new AlreadyExistsException(localDatasetID.toString, "dataset")

    getRemote(datasetRef.remoteID)

    fileSystem.mkdirs(datasetDir)
    new ResourceLoader(fileSystem)
      .saveResourceToFile(datasetRef, remoteRefFilePath(localDatasetID))
  }

  def deleteDataset(id: DatasetID): Unit = {
    ensureDatasetExists(id)

    // Validate references
    val referencedBy = getAllDatasets()
      .map(getDatasetSummary)
      .filter(_.datasetDependencies.contains(id))

    if (referencedBy.nonEmpty)
      throw new DanglingReferenceException(referencedBy.map(_.id), id)

    val layout = getDatasetLayout(id)
    fileSystem.delete(layout.cacheDir, true)
    fileSystem.delete(layout.dataDir, true)
    fileSystem.delete(layout.checkpointsDir, true)
    fileSystem.delete(layout.metadataDir, true)
    fileSystem.delete(workspaceLayout.metadataDir.resolve(id.toString), true)
  }

  def purgeDataset(id: DatasetID): Unit = {
    // TODO: Purging a dataset that is used by non-empty derivatives should raise an error
    val snapshot = getMetadataChain(id).getSnapshot()
    deleteDataset(id)
    addDataset(snapshot)
  }

  ////////////////////////////////////////////////////////////////////////////
  // Volumes
  ////////////////////////////////////////////////////////////////////////////

  def getLocalVolume(): VolumeLayout = {
    if (!fileSystem.exists(workspaceLayout.localVolumeDir)) {
      fileSystem.mkdirs(workspaceLayout.localVolumeDir)
      val outputStream =
        fileSystem.create(workspaceLayout.localVolumeDir.resolve(".gitignore"))
      val writer = new PrintWriter(outputStream)
      writer.write(WorkspaceLayout.LOCAL_VOLUME_GITIGNORE_CONTENT)
      writer.close()
    }

    VolumeLayout(
      metadataDir = workspaceLayout.localVolumeDir.resolve("datasets"),
      checkpointsDir = workspaceLayout.localVolumeDir.resolve("checkpoints"),
      dataDir = workspaceLayout.localVolumeDir.resolve("data"),
      cacheDir = workspaceLayout.localVolumeDir.resolve("cache")
    )
  }

  def loadRemoteFromURI(uri: URI): Remote = {
    volumeRepo.loadResourceFromURI(uri)
  }

  def getAllRemoteIDs(): Seq[RemoteID] = {
    volumeRepo.getAllResourceIDs()
  }

  def getAllRemotes(): Seq[Remote] = {
    volumeRepo.getAllResources()
  }

  def getRemote(remoteID: RemoteID): Remote = {
    volumeRepo.getResource(remoteID)
  }

  def addRemote(remote: Remote): Unit = {
    volumeRepo.addResource(remote)
  }

  def deleteRemote(remoteID: RemoteID): Unit = {
    // TODO: validate references
    volumeRepo.deleteResource(remoteID)
  }

}
