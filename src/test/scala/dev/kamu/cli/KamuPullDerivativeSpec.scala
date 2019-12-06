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

class KamuPullDerivativeSpec extends FlatSpec with Matchers with KamuTestBase {
  import spark.implicits._

  "kamu pull" should "produce derivative datasets" in {
    withEmptyWorkspace { kamu =>
      val input = sc
        .parallelize(
          Seq(
            (ts(2000, 1, 1), "Salt Lake City", 312),
            (ts(2000, 1, 1), "Seattle", 321),
            (ts(2000, 1, 1), "Vancouver", 123)
          )
        )
        .toDF("eventTime", "city", "population")

      val inputPath = kamu.writeData(input, OutputFormat.CSV)

      val root = DatasetFactory.newRootDataset(
        url = Some(inputPath.toUri),
        format = Some("csv"),
        header = true,
        schema = Seq("eventTime TIMESTAMP", "city STRING", "population INTEGER")
      )

      kamu.addDataset(root)

      // TODO: systemTime should not be propagated but assigned during transform
      val deriv = DatasetFactory.newDerivativeDataset(
        source = root.id,
        sql = Some(
          s"SELECT systemTime, eventTime, city, (population + 1) as population FROM `${root.id}`"
        )
      )

      kamu.addDataset(deriv)
      kamu.run("pull", "--recursive", deriv.id.toString)

      val actual = kamu
        .readDataset(deriv.id)
        .orderBy("systemTime", "city")

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), ts(2000, 1, 1), "Salt Lake City", 313),
            (ts(0), ts(2000, 1, 1), "Seattle", 322),
            (ts(0), ts(2000, 1, 1), "Vancouver", 124)
          )
        )
        .toDF("systemTime", "eventTime", "city", "population")

      assertSchemasEqual(expected, actual, ignoreNullable = true)

      // Compare ignoring the systemTime column
      assertDataFrameEquals(
        expected.drop("systemTime"),
        actual.drop("systemTime"),
        ignoreNullable = true
      )
    }
  }
}
