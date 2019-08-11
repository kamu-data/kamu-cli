package dev.kamu.cli.external

object DockerImages {
  val SPARK = "kamu/spark:2.4.0_0.0.1"
  val LIVY = "kamu/spark-py:2.4.0_0.0.1"
  val JUPYTER = "kamu/jupyter:0.0.1"

  val ALL = Array(
    SPARK,
    LIVY,
    JUPYTER
  )
}
