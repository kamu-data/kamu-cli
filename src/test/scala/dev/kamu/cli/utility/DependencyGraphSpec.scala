/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.utility

import org.scalatest.{Matchers, FunSuite}

class DependencyGraphSpec extends FunSuite with Matchers {
  test("Empty graph or isolated vertices") {
    val deps = List.empty[(Int, Int)]
    def dependsOn(v: Int) = deps.filter(d => d._1 == v).map(d => d._2)
    val g = new DependencyGraph(dependsOn)
    g.resolve(Nil) should be(Nil)
    g.resolve(List(0)) should be(List(0))
    g.resolve(List(1)) should be(List(1))
    g.resolve(List(0, 1)) should be(List(0, 1))
    g.resolve(List(1, 0)) should be(List(1, 0))
  }

  test("Single dep") {
    val deps = List((0, 1))
    def dependsOn(v: Int) = deps.filter(d => d._1 == v).map(d => d._2)
    val g = new DependencyGraph(dependsOn)
    g.resolve(Nil) should be(Nil)
    g.resolve(List(0)) should be(List(1, 0))
    g.resolve(List(1)) should be(List(1))
    g.resolve(List(0, 1)) should be(List(1, 0))
    g.resolve(List(1, 0)) should be(List(1, 0))
  }

  test("Single dep reverse") {
    val deps = List((1, 0))
    def dependsOn(v: Int) = deps.filter(d => d._1 == v).map(d => d._2)
    val g = new DependencyGraph(dependsOn)
    g.resolve(Nil) should be(Nil)
    g.resolve(List(0)) should be(List(0))
    g.resolve(List(1)) should be(List(0, 1))
    g.resolve(List(0, 1)) should be(List(0, 1))
    g.resolve(List(1, 0)) should be(List(0, 1))
  }

  test("A and B depends on C") {
    val deps = List((0, 2), (1, 2))
    def dependsOn(v: Int) = deps.filter(d => d._1 == v).map(d => d._2)
    val g = new DependencyGraph(dependsOn)
    g.resolve(List(0)) should be(List(2, 0))
    g.resolve(List(1)) should be(List(2, 1))
    g.resolve(List(2)) should be(List(2))
    g.resolve(List(0, 1)) should be(List(2, 0, 1))
    g.resolve(List(1, 0)) should be(List(2, 1, 0))
    g.resolve(List(0, 1, 2)) should be(List(2, 0, 1))
    g.resolve(List(1, 0, 2)) should be(List(2, 1, 0))
    g.resolve(List(0, 2, 1)) should be(List(2, 0, 1))
    g.resolve(List(1, 2, 0)) should be(List(2, 1, 0))
    g.resolve(List(2, 0, 1)) should be(List(2, 0, 1))
    g.resolve(List(2, 1, 0)) should be(List(2, 1, 0))
  }

  test("A depends on B and C") {
    val deps = List((0, 1), (0, 2))
    def dependsOn(v: Int) = deps.filter(d => d._1 == v).map(d => d._2)
    val g = new DependencyGraph(dependsOn)
    g.resolve(List(0)) should be(List(1, 2, 0))
    g.resolve(List(1)) should be(List(1))
    g.resolve(List(2)) should be(List(2))
    g.resolve(List(0, 1)) should be(List(1, 2, 0))
    g.resolve(List(1, 0)) should be(List(1, 2, 0))
    g.resolve(List(0, 1, 2)) should be(List(1, 2, 0))
    g.resolve(List(1, 0, 2)) should be(List(1, 2, 0))
    g.resolve(List(0, 2, 1)) should be(List(1, 2, 0))
    g.resolve(List(1, 2, 0)) should be(List(1, 2, 0))
    g.resolve(List(2, 0, 1)) should be(List(2, 1, 0))
    g.resolve(List(2, 1, 0)) should be(List(2, 1, 0))
  }

  test("Rhombus") {
    val deps = List((0, 1), (0, 2), (1, 3), (2, 3))
    def dependsOn(v: Int) = deps.filter(d => d._1 == v).map(d => d._2)
    val g = new DependencyGraph(dependsOn)
    g.resolve(List(0)) should be(List(3, 1, 2, 0))
    g.resolve(List(1)) should be(List(3, 1))
    g.resolve(List(2)) should be(List(3, 2))
    g.resolve(List(3)) should be(List(3))
    g.resolve(List(0, 1)) should be(List(3, 1, 2, 0))
    g.resolve(List(1, 0)) should be(List(3, 1, 2, 0))
    g.resolve(List(2, 0)) should be(List(3, 2, 1, 0))
    g.resolve(List(0, 1, 2)) should be(List(3, 1, 2, 0))
    g.resolve(List(0, 2, 3)) should be(List(3, 1, 2, 0))
    g.resolve(List(0, 3, 1)) should be(List(3, 1, 2, 0))
    g.resolve(List(0, 1, 3)) should be(List(3, 1, 2, 0))
    g.resolve(List(3, 2, 0)) should be(List(3, 2, 1, 0))
    g.resolve(List(2, 3, 0)) should be(List(3, 2, 1, 0))
    g.resolve(List(1, 2, 0)) should be(List(3, 1, 2, 0))
    g.resolve(List(2, 0, 1)) should be(List(3, 2, 1, 0))
    g.resolve(List(2, 1, 0)) should be(List(3, 2, 1, 0))
  }

  test("Rank 2 binary tree") {
    val deps = List((0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6))
    def dependsOn(v: Int) = deps.filter(d => d._1 == v).map(d => d._2)
    val g = new DependencyGraph(dependsOn)
    g.resolve(List(0)) should be(List(3, 4, 1, 5, 6, 2, 0))
    g.resolve(List(0, 1, 2, 3, 4, 5, 6)) should be(List(3, 4, 1, 5, 6, 2, 0))
    g.resolve(List(6, 5, 4, 3, 2, 1, 0)) should be(List(6, 5, 4, 3, 2, 1, 0))
  }
}
