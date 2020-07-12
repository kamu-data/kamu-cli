/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest

import java.sql.Timestamp

import dev.kamu.cli.{DatasetFactory, KamuTestBase}
import dev.kamu.core.manifests._
import org.scalatest._
import org.apache.spark.sql.functions

class IngestGeoJsonSpec extends FlatSpec with Matchers with KamuTestBase {
  import spark.implicits._

  "kamu pull" should "be able to ingest GeoJson" in {
    withEmptyWorkspace { kamu =>
      val inputData =
        """{
          |  "type": "FeatureCollection",
          |  "features": [
          |    {
          |      "type": "Feature",
          |      "properties": {
          |        "id": 0,
          |        "zipcode": "00101",
          |        "name": "A"
          |      },
          |      "geometry": {
          |        "type": "Polygon",
          |        "coordinates": [
          |          [
          |            [0.0, 0.0],
          |            [10.0, 0.0],
          |            [10.0, 10.0],
          |            [0.0, 10.0],
          |            [0.0, 0.0]
          |          ]
          |        ]
          |      }
          |    },
          |    {
          |      "type": "Feature",
          |      "properties": {
          |        "id": 1,
          |        "zipcode": "00202",
          |        "name": "B"
          |      },
          |      "geometry": {
          |        "type": "Polygon",
          |        "coordinates": [
          |          [
          |            [0.0, 0.0],
          |            [20.0, 0.0],
          |            [20.0, 20.0],
          |            [0.0, 20.0],
          |            [0.0, 0.0]
          |          ]
          |        ]
          |      }
          |    }
          |  ]
          |}""".stripMargin

      val inputPath = kamu.writeData(inputData, "polygons.json")

      val ds = DatasetSnapshot(
        id = DatasetFactory.newDatasetID(),
        source = DatasetSource.Root(
          fetch = FetchStep.Url(inputPath.toUri),
          read = ReadStep.GeoJson(),
          merge = MergeStrategy.Snapshot(primaryKey = Vector("id"))
        )
      )

      kamu.addDataset(ds)
      kamu.run("pull", ds.id.toString)

      val actual = kamu.readDataset(ds.id)

      val expected = sc
        .parallelize(
          Seq(
            (
              new Timestamp(0),
              new Timestamp(0),
              "I",
              "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
              "0",
              "00101",
              "A"
            ),
            (
              new Timestamp(0),
              new Timestamp(0),
              "I",
              "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))",
              "1",
              "00202",
              "B"
            )
          )
        )
        .toDF(
          "system_time",
          "event_time",
          "observed",
          "geometry",
          "id",
          "zipcode",
          "name"
        )
        .withColumn(
          "geometry",
          functions.callUDF("ST_GeomFromWKT", functions.col("geometry"))
        )

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
