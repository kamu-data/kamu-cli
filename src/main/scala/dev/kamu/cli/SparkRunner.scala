package dev.kamu.cli

import dev.kamu.core.manifests.RepositoryVolumeMap
import org.apache.hadoop.fs.Path

trait SparkRunner {
  def submit(
    repo: RepositoryVolumeMap,
    appClass: String,
    jars: Seq[Path]
  )

  protected def assemblyPath: Path = {
    new Path(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
  }
}
