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

  "kamu pull" should "be able to import simple csv" in {
    withEmptyWorkspace { kamu =>
      val input = sc
        .parallelize(
          Seq(
            (ts(2000, 1, 1), "Vancouver", 123),
            (ts(2000, 1, 1), "Seattle", 321)
          )
        )
        .toDF("event_time", "city", "population")

      val inputPath = kamu.writeData(input, OutputFormat.CSV)

      val ds = DatasetFactory.newRootDataset(
        url = Some(inputPath.toUri),
        format = Some("csv"),
        header = true,
        schema = Array("event_time TIMESTAMP", "city STRING", "population INT")
      )

      kamu.addDataset(ds)
      kamu.run("pull", ds.id.toString)

      val actual = kamu.readDataset(ds.id)

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), ts(2000, 1, 1), "Vancouver", 123),
            (ts(0), ts(2000, 1, 1), "Seattle", 321)
          )
        )
        .toDF("system_time", "event_time", "city", "population")

      assertSchemasEqual(expected, actual, true)

      // Compare ignoring the system_time column
      assertDataFrameEquals(
        expected.drop("system_time", "event_time"),
        actual.drop("system_time", "event_time"),
        true
      )
    }
  }
}
