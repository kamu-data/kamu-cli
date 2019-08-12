package dev.kamu.cli.external

object DockerImages {
  val SPARK = "kamudata/spark-py-uber:2.4.0_0.0.1"
  val LIVY = SPARK
  val JUPYTER = "kamudata/jupyter-uber:0.0.1"

  val ALL = Array(
    SPARK,
    LIVY,
    JUPYTER
  ).distinct
}
