/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import dev.kamu.cli.output.OutputFormat
import org.scalatest._

class KamuPullRootSpec extends FlatSpec with Matchers with KamuTestBase {

  import spark.implicits._
  protected override val enableHiveSupport = false

  "kamu pull" should "be able to import simple csv" in {
    withEmptyWorkspace { kamu =>
      val input = sc
        .parallelize(
          Seq(
            ("Vancouver", "123"),
            ("Seattle", "321")
          )
        )
        .toDF("city", "population")

      val inputPath = kamu.writeData(input, OutputFormat.CSV)

      val ds = DatasetFactory.newRootDataset(
        url = Some(inputPath.toUri),
        format = Some("csv"),
        header = true
      )

      kamu.addDataset(ds)
      kamu.run("pull", ds.id.toString)

      val actual = kamu.readDataset(ds.id)

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), "Vancouver", "123"),
            (ts(0), "Seattle", "321")
          )
        )
        .toDF("systemTime", "city", "population")

      assert(expected.schema, actual.schema)

      // Compare ignoring the systemTime column
      assertDataFrameEquals(
        expected.drop("systemTime"),
        actual.drop("systemTime")
      )
    }
  }
}
