/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.external

object DockerImages {
  val SPARK = "kamudata/engine-spark:0.5.0"
  val FLINK = "kamudata/engine-flink:0.3.3"

  val LIVY = SPARK
  val JUPYTER = "kamudata/jupyter-uber:0.0.1"

  val ALL = Array(
    SPARK,
    FLINK,
    LIVY,
    JUPYTER
  ).distinct
}
