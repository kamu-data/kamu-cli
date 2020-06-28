/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest

import dev.kamu.cli.{DatasetFactory, KamuTestBase}
import dev.kamu.core.manifests._
import org.scalatest._

class IngestMultiSourceSpec extends FlatSpec with Matchers with KamuTestBase {
  import spark.implicits._

  "kamu pull" should "be able to ingest glob files" in {
    withEmptyWorkspace { kamu =>
      val inputData1 =
        """1,alex,100
          |2,bob,200
          |3,charlie,300
          |""".stripMargin

      val inputData2 =
        """2,bob,200
          |3,charlie,500
          |4,dan,100
          |""".stripMargin

      kamu.writeData(inputData1, "balances-2001-01-01.csv")
      val dataPath = kamu.writeData(inputData2, "balances-2001-02-01.csv")

      val globPath = dataPath.getParent.resolve("balances-*.csv")

      val ds = DatasetSnapshot(
        id = DatasetFactory.newDatasetID(),
        source = SourceKind.Root(
          fetch = FetchSourceKind.FilesGlob(
            path = globPath,
            eventTime = Some(
              EventTimeKind.FromPath(
                pattern = """balances-(\d+-\d+-\d+)\.csv""",
                timestampFormat = "yyyy-MM-dd"
              )
            )
          ),
          read = ReaderKind.Csv(
            schema = Vector("id INT", "name STRING", "balance INT")
          ),
          merge = MergeStrategyKind.Snapshot(primaryKey = Vector("id"))
        )
      ).postLoad()

      kamu.addDataset(ds)
      kamu.run("pull", ds.id.toString)

      val actual = kamu
        .readDataset(ds.id)
        .orderBy("event_time", "id")

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), ts(2001, 1, 1), "I", 1, "alex", 100),
            (ts(0), ts(2001, 1, 1), "I", 2, "bob", 200),
            (ts(0), ts(2001, 1, 1), "I", 3, "charlie", 300),
            (ts(0), ts(2001, 2, 1), "D", 1, "alex", 100),
            (ts(0), ts(2001, 2, 1), "U", 3, "charlie", 500),
            (ts(0), ts(2001, 2, 1), "I", 4, "dan", 100)
          )
        )
        .toDF("system_time", "event_time", "observed", "id", "name", "balance")

      assertSchemasEqual(expected, actual, true)

      // Compare ignoring the system_time column
      assertDataFrameEquals(
        expected.drop("system_time"),
        actual.drop("system_time"),
        true
      )
    }
  }
}
