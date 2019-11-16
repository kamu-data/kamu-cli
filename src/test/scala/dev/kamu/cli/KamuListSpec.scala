package dev.kamu.cli

import org.scalatest._

class KamuListSpec extends FlatSpec with Matchers with KamuTestBase {
  "kamu list" should "return empty result for empty repo" in {
    withEmptyWorkspace { kamu =>
      val rs = kamu.runEx("list").resultSet.get

      rs.columns shouldEqual Vector("ID", "Kind")
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

      rs.columns shouldEqual Vector("ID", "Kind")
      rs.rows should contain theSameElementsAs (Seq(
        Array(rootDS.id, "Root"),
        Array(derivDS.id, "Derivative")
      ))
    }
  }
}
