/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import org.scalatest._

class KamuListSpec extends FlatSpec with Matchers with KamuTestBase {
  "kamu list" should "return empty result for empty repo" in {
    withEmptyWorkspace { kamu =>
      val rs = kamu.runEx("list").resultSet.get

      rs.columns shouldEqual Vector(
        "ID",
        "Kind",
        "Records",
        "Size",
        "LastModified"
      )
      rs.rows shouldEqual Vector.empty
    }
  }

  "kamu list" should "display all datasets" in {
    withEmptyWorkspace { kamu =>
      val rootDS = DatasetFactory.newRootDataset()
      val derivDS = DatasetFactory.newDerivativeDataset(rootDS.id)
      kamu.addDataset(rootDS)
      kamu.addDataset(derivDS)

      val rs = kamu.runEx("list").resultSet.get

      rs.columns shouldEqual Vector(
        "ID",
        "Kind",
        "Records",
        "Size",
        "LastModified"
      )
      rs.rows should contain theSameElementsAs Seq(
        Array(
          rootDS.id,
          "Root",
          0,
          0,
          kamu.systemClock.instant()
        ),
        Array(
          derivDS.id,
          "Derivative",
          0,
          0,
          kamu.systemClock.instant()
        )
      )
    }
  }
}
