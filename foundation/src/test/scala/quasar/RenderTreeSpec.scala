/*
 * Copyright 2014–2017 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import slamdata.Predef._

import argonaut._, Argonaut._
import argonaut.JsonScalaz._
import org.scalacheck._
import scalaz.Equal

class RenderedTreeSpec extends quasar.Qspec {
  private implicit def RenderedTreeEqual: Equal[RenderedTree]                  = Equal.equalBy(_.asJson)
  private def expectJson(t: RenderedTree, expect: Json)                        = t.asJson must_= expect
  private def expect(t1: RenderedTree, t2: RenderedTree, result: RenderedTree) = (t1 diff t2) must_= result

  private implicit class TreeSymbolOps(sym: scala.Symbol) {
    def unary_- = RenderedTree(List(">>>"), Some(sym.name), Nil)
    def unary_+ = RenderedTree(List("<<<"), Some(sym.name), Nil)

    def /\(children: RenderedTree*): RenderedTree     = RenderedTree(Nil, Some(sym.name), children.toList)
    def apply(tp: String, tps: String*): RenderedTree = RenderedTree(tp :: tps.toList, Some(sym.name), Nil)
  }
  private implicit class RenderedTreeOps(t: RenderedTree) {
    def unary_- = t retype {
      case Nil     => List(">>>")
      case x :: xs => s">>> $x" :: xs
    }
    def unary_+ = t retype {
      case Nil     => List("<<<")
      case x :: xs => s"<<< $x" :: xs
    }

    def :+(child: RenderedTree): RenderedTree  = t.copy(children = t.children :+ child)
    def apply(ts: RenderedTree*): RenderedTree = t.copy(children = t.children ++ ts.toList)
  }
  private implicit def liftSimpleTree(sym: scala.Symbol): RenderedTree = sym /\ ()

  private def anon(tp: String, tps: String*): RenderedTree = RenderedTree(tp :: tps.toList, None, Nil)
  private def diff: RenderedTree                           = anon("[Root differs]")
  private def root: RenderedTree                           = anon("root")

  private def A_green: RenderedTree = 'A("green")
  private def A_blue: RenderedTree  = 'A("blue")

  def genRenderedTree: Gen[RenderedTree] = Gen.oneOf[RenderedTree](
    'A,
    A_green,
    'A /\ ('B, 'C),
    'A /\ (A_green, 'B /\ ('C, 'D)),
    'A /\ 'B
  )
  implicit def arbitraryRenderedTree: Arbitrary[RenderedTree] = Arbitrary(genRenderedTree)

  "RenderedTree.diff" should {

    "find no difference" in prop((t: RenderedTree) => expect(t, t, t))

    "find simple difference" in expect(
      'A,
      'B,
      diff(-'A, +'B)
    )

    "find simple difference in parent" in expect(
      'A /\ 'B,
      'C /\ 'B,
      diff(-'A :+ 'B, +'C :+ 'B)
    )

    "find added child" in expect(
      'A /\ ('B     ),
      'A /\ ('B,  'C),
      'A /\ ('B, +'C)
    )

    "find deleted child" in expect(
      'A /\ ('B,  'C),
      'A /\ ('B     ),
      'A /\ ('B, -'C)
    )

    "find simple difference in child" in expect(
      'A /\ 'B,
      'A /\ 'C,
      'A /\ (-'B, +'C)
    )

    "find multiple changed children" in expect(
      'A /\ ('B, 'C, 'D),
      'A /\ ('C, 'E, 'D),
      'A /\ (-'B, 'C, +'E, 'D)
    )

    "find added grand-child" in expect(
      'A /\ 'B,
      'A /\ ('B /\  'C),
      'A /\ ('B /\ +'C)
    )

    "find deleted grand-child" in expect(
      'A /\ ('B /\  'C),
      'A /\ 'B,
      'A /\ ('B /\ -'C)
    )

    "find different nodeType at root" in expect(
      A_green,
      A_blue,
      diff(-A_green, +A_blue)
    )

    "find different nodeType" in expect(
      root :+ A_green,
      root :+ A_blue,
      root :+ -A_green :+ +A_blue
    )

    "find different nodeType (compound type; no labels)" in expect(
      root(anon("red", "color"),  anon("green", "color")),
      root(anon("red", "color"),  anon("blue", "color")),
      root(anon("red", "color"), -anon("green", "color"), +anon("blue", "color"))
    )
  }

  "RenderedTreeEncodeJson" should {
    "encode Terminal"                            in expectJson('A, Json("label" := "A"))
    "encode Terminal with type"                  in expectJson(A_green, Json("type" := "green", "label" := "A"))
    "encode Terminal with complex type"          in expectJson('A("inner", "outer"), Json("type" := "outer/inner", "label" := "A"))
    "encode NonTerminal with one child"          in expectJson('A /\ 'B, Json("label" := "A", "children" := List(Json("label" := "B"))))
    "encode NonTerminal with one child and type" in expectJson(
      A_green :+ 'B,
      Json(
            "type" := "green",
           "label" := "A",
        "children" := Json("label" := "B") :: Nil
      )
    )
  }
}
