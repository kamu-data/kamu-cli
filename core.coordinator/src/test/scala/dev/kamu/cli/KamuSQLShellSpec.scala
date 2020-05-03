/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import org.scalatest._

class KamuSQLShellSpec extends FlatSpec with Matchers with KamuTestBase {

  import spark.implicits._
  protected override val enableHiveSupport = false

  "kamu sql" should "be able to read a dataset" in {
    withEmptyWorkspace { kamu =>
      val input = sc
        .parallelize(
          Seq(
            ("Vancouver", "123"),
            ("Seattle", "321")
          )
        )
        .toDF("City", "Population")

      val ds = DatasetFactory.newRootDataset()
      kamu.addDataset(ds, input)

      val output = kamu
        .runEx(
          "sql",
          "-c",
          s"select * from parquet.`${ds.id}`",
          "-O",
          "csv"
        )
        .output
        .stripLineEnd

      println(output)

      val expected = """'City','Population'
                       |'Vancouver','123'
                       |'Seattle','321'""".stripMargin

      output shouldEqual expected
    }
  }
}
