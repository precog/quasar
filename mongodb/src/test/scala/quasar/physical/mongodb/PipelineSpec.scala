/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.physical.mongodb

import quasar._
import slamdata.Predef._
import quasar.common.SortDir
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.selector.Selector
import quasar.physical.mongodb.workflow._

import org.scalacheck._
import scalaz._

class PipelineSpec extends quasar.Qspec with ArbBsonField {
  import CollectionUtil._
  import ArbitraryExprOp._
  import fixExprOp._

  implicit def arbitraryOp: Arbitrary[PipelineOp] = Arbitrary { Gen.resize(5, Gen.sized { size =>
    // Note: Gen.oneOf is overridden and this variant requires two explicit args
    val ops = pipelineOpGens(size - 1)

    Gen.oneOf(ops(0), ops(1), ops.drop(2): _*)
  }) }

  def genProject(size: Int): Gen[$ProjectF[Unit]] = for {
    fields <- Gen.nonEmptyListOf(for {
      c  <- Gen.alphaChar
      cs <- Gen.alphaStr

      field = c.toString + cs

      value <- if (size <= 0) genExpr3_6.map(\/-(_))
      else Gen.oneOf(
        genProject(size - 1).map(p => -\/(p.shape)),
        genExpr3_6.map(\/-(_)))
    } yield BsonField.Name(field) -> value)
    id <- Gen.oneOf(ExcludeId, IncludeId)
  } yield $ProjectF((), Reshape[ExprOp](ListMap(fields: _*)), id)

  implicit def arbProject = Arbitrary[$ProjectF[Unit]](Gen.resize(5, Gen.sized(genProject)))

  def genRedact = for {
    value <- Gen.oneOf($RedactF.DESCEND, $RedactF.KEEP, $RedactF.PRUNE)
  } yield $RedactF((), $var(value))

  def unwindGen = for {
    c <- Gen.alphaChar
  } yield $UnwindF((), DocField(BsonField.Name(c.toString)), None, None)

  def genGroup = for {
    i <- Gen.chooseNum(1, 10)
  } yield $GroupF((),
    Grouped(ListMap(BsonField.Name("docsByAuthor" + i.toString) -> $sum($literal(Bson.Int32(1))))),
    \/-($var(DocField(BsonField.Name("author" + i)))))

  def genGeoNear = for {
    i <- Gen.chooseNum(1, 10)
  } yield $GeoNearF((), (40.0, -105.0), BsonField.Name("distance" + i), None, None, None, None, None, None, None)

  def genOut = for {
    i <- Gen.chooseNum(1, 10)
  } yield $OutF((), CollectionName("result" + i))

  def pipelineOpGens(size: Int): List[Gen[PipelineOp]] = {
    genProject(size).map(op => PipelineOp(op.pipeline)) ::
    genRedact.map(op => PipelineOp(op.pipeline)) ::
    unwindGen.map(op => PipelineOp(op.pipeline)) ::
    genGroup.map(op => PipelineOp(op.pipeline)) ::
    genGeoNear.map(op => PipelineOp(op.pipeline)) ::
    genOut.map(op => PipelineOp(op.shapePreserving)) ::
    arbitraryShapePreservingOpGens.map(g => for { sp <- g } yield sp.op)
  }

  case class ShapePreservingPipelineOp(op: PipelineOp)

  //implicit def arbitraryProject: Arbitrary[Project] = Arbitrary(genProject)

  implicit def arbitraryShapePreservingOp: Arbitrary[ShapePreservingPipelineOp] = Arbitrary {
    // Note: Gen.oneOf is overridden and this variant requires two explicit args
    val gens = arbitraryShapePreservingOpGens
    Gen.oneOf(gens(0), gens(1), gens.drop(2): _*)
  }

  def arbitraryShapePreservingOpGens = {
    def matchGen = for {
      c <- Gen.alphaChar
    } yield ShapePreservingPipelineOp(PipelineOp($MatchF((), Selector.Doc(BsonField.Name(c.toString) -> Selector.Eq(Bson.Int32(-1)))).shapePreserving))

    def skipGen = for {
      i <- Gen.chooseNum(0, Long.MaxValue)
    } yield ShapePreservingPipelineOp(PipelineOp($SkipF((), i).shapePreserving))

    def limitGen = for {
      i <- Gen.chooseNum(1, Long.MaxValue)
    } yield ShapePreservingPipelineOp(PipelineOp($LimitF((), i).shapePreserving))

    def sortGen = for {
      c <- Gen.alphaChar
    } yield ShapePreservingPipelineOp(PipelineOp($SortF((), NonEmptyList(BsonField.Name("name1") -> SortDir.Ascending)).shapePreserving))

    List(matchGen, limitGen, skipGen, sortGen)
  }

  case class PairOfOpsWithSameType(op1: PipelineOp, op2: PipelineOp)

  implicit def arbitraryPair: Arbitrary[PairOfOpsWithSameType] = Arbitrary { Gen.resize(5, Gen.sized { size =>
    for {
      gen <- Gen.oneOf(pipelineOpGens(size))
      op1 <- gen
      op2 <- gen
    } yield PairOfOpsWithSameType(op1, op2)
  }) }

  "Project.id" should {
    "be idempotent" >> prop { (p: $ProjectF[Unit]) =>
      p.id must_== p.id.id
    }
  }

  "Project.get" should {
    "retrieve whatever value it was set to" >> prop { (p: $ProjectF[Unit], f: BsonField) =>
      val One = $literal(Bson.Int32(1))

      p.set(f, \/-(One)).get(DocVar.ROOT(f)) must (beSome(\/-(One)))
    }
  }

  "Project.setAll" should {
    "actually set all" >> prop { (p: $ProjectF[Unit]) =>
      p.setAll(p.getAll.map(t => t._1 -> \/-(t._2))) must_== p
    }.pendingUntilFixed("result could have `_id -> _id` inserted without changing semantics")
  }

  "Project.deleteAll" should {
    "return empty when everything is deleted" >> prop { (p: $ProjectF[Unit]) =>
      p.deleteAll(p.getAll.map(_._1)) must_== p.empty
    }
  }

  "SimpleMap.deleteAll" should {
    import jscore._

    "remove one un-nested field" in {
      val op = $SimpleMapF(
        $read[WorkflowOpCoreF](collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"),
            "b" -> Select(ident("x"), "y"))))),
        ListMap())
      val exp = $SimpleMapF(
        $read[WorkflowOpCoreF](collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"))))),
        ListMap())
      op.deleteAll(List(BsonField.Name("b"))) must_== exp
    }

    "remove one nested field" in {
      val op = $SimpleMapF(
        $read[WorkflowOpCoreF](collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"),
            "b" -> obj(
              "c" -> Select(ident("x"), "y"),
              "d" -> Select(ident("x"), "z")))))),
        ListMap())
      val exp = $SimpleMapF(
        $read[WorkflowOpCoreF](collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"),
            "b" -> obj(
              "d" -> Select(ident("x"), "z")))))),
        ListMap())
      op.deleteAll(List(BsonField.Name("b") \ BsonField.Name("c"))) must_== exp
    }

    "remove whole nested object" in {
      val op = $SimpleMapF(
        $read[WorkflowOpCoreF](collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"),
            "b" -> obj(
              "c" -> Select(ident("x"), "y")))))),
        ListMap())
      val exp = $SimpleMapF(
        $read[WorkflowOpCoreF](collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"))))),
        ListMap())
      op.deleteAll(List(BsonField.Name("b") \ BsonField.Name("c"))) must_== exp
    }
  }
}
