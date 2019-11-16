package dev.kamu.cli

import org.apache.hadoop.fs.Path
import dev.kamu.core.manifests.utils.fs._

case class KamuConfig(
  workspaceRoot: Path = new Path("."),
  spark: SparkConfig = SparkConfig()
) {
  def kamuRoot: Path = {
    workspaceRoot.resolve(".kamu")
  }

  def localVolume: Path = {
    workspaceRoot.resolve(".kamu.local")
  }
}

case class SparkConfig(
  driverMemory: String = "2g"
)
