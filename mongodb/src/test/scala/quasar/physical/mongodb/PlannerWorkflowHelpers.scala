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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._, RenderTree.ops._
import quasar.common.{Map => _, _}
import quasar.fp._
import quasar.fp.ski._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._
import quasar.sql , sql.{fixpoint => sqlF, _}

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.scalacheck._
import org.specs2.execute._
import org.specs2.matcher.{Matcher, Expectable}
import pathy.Path._
import scalaz._, Scalaz._


trait PlannerWorkflowHelpers extends PlannerHelpers {

  import fixExprOp._

  /**
    * @return The list of expected names for the projections of the selection
    */
  def columnNames(q: Select[Fix[Sql]]): List[String] =
    // TODO: Replace `get` with `valueOr` and an exception message detailing
    // what was the underlying assumption that proved to be wrong
    projectionNames(q.projections, None).toOption.get.map(_._1)

  def fieldNames(wf: Workflow): Option[List[String]] =
    simpleShape(wf).map(_.map(_.asText))

  val notDistinct = Gen.const(SelectAll)
  val distinct = Gen.const(SelectDistinct)

  val noGroupBy = Gen.const[Option[GroupBy[Fix[Sql]]]](None)
  val groupBySeveral = Gen.nonEmptyListOf(Gen.oneOf(
    sqlF.IdentR("state"),
    sqlF.IdentR("territory"))).map(keys => GroupBy(keys.distinct, None))

  val noFilter = Gen.const[Option[Fix[Sql]]](None)
  val filter = Gen.oneOf(
    for {
      x <- genInnerInt
    } yield sqlF.BinopR(x, sqlF.IntLiteralR(100), sql.Lt),
    for {
      x <- genInnerStr
    } yield sqlF.InvokeFunctionR(CIName("search"), List(x, sqlF.StringLiteralR("^BOULDER"), sqlF.BoolLiteralR(false))),
    Gen.const(sqlF.BinopR(sqlF.IdentR("p"), sqlF.IdentR("q"), sql.Eq)))  // Comparing two fields requires a $project before the $match

  val noOrderBy: Gen[Option[OrderBy[Fix[Sql]]]] = Gen.const(None)

  val orderBySeveral: Gen[Option[OrderBy[Fix[Sql]]]] = {
    val order = Gen.oneOf(ASC, DESC) tuple Gen.oneOf(genInnerInt, genInnerStr)
    (order |@| Gen.listOf(order))((h, t) => Some(OrderBy(NonEmptyList(h, t: _*))))
  }

  val maybeReducingExpr = Gen.oneOf(genOuterInt, genOuterStr)

  def select(distinctGen: Gen[IsDistinct], exprGen: Gen[Fix[Sql]], filterGen: Gen[Option[Fix[Sql]]], groupByGen: Gen[Option[GroupBy[Fix[Sql]]]], orderByGen: Gen[Option[OrderBy[Fix[Sql]]]]): Gen[Select[Fix[Sql]]] =
    for {
      distinct <- distinctGen
      projs    <- (genReduceInt ⊛ Gen.nonEmptyListOf(exprGen))(_ :: _).map(_.zipWithIndex.map {
        case (x, n) => Proj(x, Some("p" + n))
      })
      filter   <- filterGen
      groupBy  <- groupByGen
      orderBy  <- orderByGen
    } yield sql.Select(distinct, projs, Some(TableRelationAST(file("zips"), None)), filter, groupBy, orderBy)

  def genInnerInt = Gen.oneOf(
    sqlF.IdentR("pop"),
    // IntLiteralR(0),  // TODO: exposes bugs (see SD-478)
    sqlF.BinopR(sqlF.IdentR("pop"), sqlF.IntLiteralR(1), Minus), // an ExprOp
    sqlF.InvokeFunctionR(CIName("length"), List(sqlF.IdentR("city")))) // requires JS
  def genReduceInt = genInnerInt.flatMap(x => Gen.oneOf(
    x,
    sqlF.InvokeFunctionR(CIName("min"), List(x)),
    sqlF.InvokeFunctionR(CIName("max"), List(x)),
    sqlF.InvokeFunctionR(CIName("sum"), List(x)),
    sqlF.InvokeFunctionR(CIName("count"), List(sqlF.SpliceR(None)))))
  def genOuterInt = Gen.oneOf(
    Gen.const(sqlF.IntLiteralR(0)),
    genReduceInt,
    genReduceInt.flatMap(sqlF.BinopR(_, sqlF.IntLiteralR(1000), sql.Div)),
    genInnerInt.flatMap(x => sqlF.BinopR(sqlF.IdentR("loc"), sqlF.ArrayLiteralR(List(x)), Concat)))

  def genInnerStr = Gen.oneOf(
    sqlF.IdentR("city"),
    // StringLiteralR("foo"),  // TODO: exposes bugs (see SD-478)
    sqlF.InvokeFunctionR(CIName("lower"), List(sqlF.IdentR("city"))))
  def genReduceStr = genInnerStr.flatMap(x => Gen.oneOf(
    x,
    sqlF.InvokeFunctionR(CIName("min"), List(x)),
    sqlF.InvokeFunctionR(CIName("max"), List(x))))
  def genOuterStr = Gen.oneOf(
    Gen.const(sqlF.StringLiteralR("foo")),
    Gen.const(sqlF.IdentR("state")),  // possibly the grouping key, so never reduced
    genReduceStr,
    genReduceStr.flatMap(x => sqlF.InvokeFunctionR(CIName("lower"), List(x))),   // an ExprOp
    genReduceStr.flatMap(x => sqlF.InvokeFunctionR(CIName("length"), List(x))))  // requires JS

  implicit def shrinkQuery(implicit SS: Shrink[Fix[Sql]]): Shrink[Query] = Shrink { q =>
    fixParser.parseExpr(q.value).fold(κ(Stream.empty), SS.shrink(_).map(sel => Query(pprint(sel))))
  }

  /**
   Shrink a query by reducing the number of projections or grouping expressions. Do not
   change the "shape" of the query, by removing the group by entirely, etc.
   */
  implicit def shrinkExpr: Shrink[Fix[Sql]] = {
    /** Shrink a list, removing a single item at a time, but never producing an empty list. */
    def shortened[A](as: List[A]): Stream[List[A]] =
      if (as.length <= 1) Stream.empty
      else as.toStream.map(a => as.filterNot(_ == a))

    Shrink {
      case Embed(Select(d, projs, rel, filter, groupBy, orderBy)) =>
        val sDistinct = if (d == SelectDistinct) Stream(sqlF.SelectR(SelectAll, projs, rel, filter, groupBy, orderBy)) else Stream.empty
        val sProjs = shortened(projs).map(ps => sqlF.SelectR(d, ps, rel, filter, groupBy, orderBy))
        val sGroupBy = groupBy.map { case GroupBy(keys, having) =>
          shortened(keys).map(ks => sqlF.SelectR(d, projs, rel, filter, Some(GroupBy(ks, having)), orderBy))
        }.getOrElse(Stream.empty)
        sDistinct ++ sProjs ++ sGroupBy
      case expr => Stream(expr)
    }
  }

  def countOps(wf: Workflow, p: PartialFunction[WorkflowF[Fix[WorkflowF]], Boolean]): Int = {
    wf.foldMap(op => if (p.lift(op.unFix).getOrElse(false)) 1 else 0)
  }

  val WC = Inject[WorkflowOpCoreF, WorkflowF]

  def countAccumOps(wf: Workflow) = countOps(wf, { case WC($GroupF(_, _, _)) => true })
  def countUnwindOps(wf: Workflow) = countOps(wf, { case WC($UnwindF(_, _, _, _)) => true })
  def countMatchOps(wf: Workflow) = countOps(wf, { case WC($MatchF(_, _)) => true })

  sealed trait OpType
  case object Agg extends OpType
  case object MapReduce extends OpType

  sealed abstract class MongoOp(val tp: OpType)
  case object PureOp extends MongoOp(Agg)
  case object ReadOp extends MongoOp(Agg)
  case object MatchOp extends MongoOp(Agg)
  case object ProjectOp extends MongoOp(Agg)
  case object RedactOp extends MongoOp(Agg)
  case object LimitOp extends MongoOp(Agg)
  case object SkipOp extends MongoOp(Agg)
  case object UnwindOp extends MongoOp(Agg)
  case object GroupOp extends MongoOp(Agg)
  case object SortOp extends MongoOp(Agg)
  case object GeoNearOp extends MongoOp(Agg)
  case object OutOp extends MongoOp(Agg)
  case object LookupOp extends MongoOp(Agg)
  case object SampleOp extends MongoOp(Agg)
  case object FoldLeftOp extends MongoOp(Agg)

  case object MapOp extends MongoOp(MapReduce)
  case object FlatMapOp extends MongoOp(MapReduce)
  case object SimpleMapOp extends MongoOp(MapReduce)
  case object ReduceOp extends MongoOp(MapReduce)

  implicit val showMongoOp = Show.showFromToString[MongoOp]
  implicit val equalMongoOp = Equal.equalRef[MongoOp]

  val pureOp: MongoOp = PureOp
  val readOp: MongoOp = ReadOp
  val matchOp: MongoOp = MatchOp
  val projectOp: MongoOp = ProjectOp
  val redactOp: MongoOp = RedactOp
  val limitOp: MongoOp = LimitOp
  val skipOp: MongoOp = SkipOp
  val unwindOp: MongoOp = UnwindOp
  val groupOp: MongoOp = GroupOp
  val sortOp: MongoOp = SortOp
  val geoNearOp: MongoOp = GeoNearOp
  val outOp: MongoOp = OutOp
  val lookupOp: MongoOp = LookupOp
  val sampleOp: MongoOp = SampleOp
  val foldLeftOp: MongoOp = FoldLeftOp
  val mapOp: MongoOp = MapOp
  val flatMapOp: MongoOp = FlatMapOp
  val simpleMapOp: MongoOp = SimpleMapOp
  val reduceOp: MongoOp = ReduceOp

  def opAlg: Algebra[WorkflowF, IList[MongoOp]] = {
    case WC($PureF(_)) => IList(PureOp)
    case WC($ReadF(_)) => IList(ReadOp)
    case WC($MatchF(s, _)) => MatchOp :: s
    case WC($ProjectF(s, _, _)) => ProjectOp :: s
    case WC($RedactF(s, _)) => RedactOp :: s
    case WC($LimitF(s, _)) => LimitOp :: s
    case WC($SkipF(s, _)) => SkipOp :: s
    case WC($UnwindF(s, _, _, _)) => UnwindOp :: s
    case WC($GroupF(s, _, _)) => GroupOp :: s
    case WC($SortF(s, _)) => SortOp :: s
    case WC($GeoNearF(s, _, _, _, _, _, _, _, _, _)) => GeoNearOp :: s
    case WC($OutF(s, _)) => OutOp :: s
    case WC($FoldLeftF(s1, s2)) => (FoldLeftOp :: s1) ++ s2.list.flatten
    case WC($LookupF(s, _, _, _, _)) => LookupOp :: s
    case WC($SampleF(s, _)) => SampleOp :: s
    case WC($MapF(s, _, _)) => MapOp :: s
    case WC($FlatMapF(s, _, _)) => FlatMapOp :: s
    case WC($SimpleMapF(s, _, _)) => SimpleMapOp :: s
    case WC($ReduceF(s, _, _)) => ReduceOp :: s
  }

  def opTreeAlg: Algebra[WorkflowF, Tree[MongoOp]] = {
    case WC($PureF(_)) => pureOp.leaf
    case WC($ReadF(_)) => readOp.leaf
    case WC($MatchF(s, _)) => matchOp.node(s)
    case WC($ProjectF(s, _, _)) => projectOp.node(s)
    case WC($RedactF(s, _)) => redactOp.node(s)
    case WC($LimitF(s, _)) => limitOp.node(s)
    case WC($SkipF(s, _)) => skipOp.node(s)
    case WC($UnwindF(s, _, _, _)) => unwindOp.node(s)
    case WC($GroupF(s, _, _)) => groupOp.node(s)
    case WC($SortF(s, _)) => sortOp.node(s)
    case WC($GeoNearF(s, _, _, _, _, _, _, _, _, _)) => geoNearOp.node(s)
    case WC($OutF(s, _)) => outOp.node(s)
    case WC($FoldLeftF(s1, s2)) => foldLeftOp.node((s1 :: s2.list).toList : _*)
    case WC($LookupF(s, _, _, _, _)) => lookupOp.node(s)
    case WC($SampleF(s, _)) => sampleOp.node(s)
    case WC($MapF(s, _, _)) => mapOp.node(s)
    case WC($FlatMapF(s, _, _)) => flatMapOp.node(s)
    case WC($SimpleMapF(s, _, _)) => simpleMapOp.node(s)
    case WC($ReduceF(s, _, _)) => reduceOp.node(s)
  }

  def ops(wf: Workflow): IList[MongoOp] = wf.cata(opAlg).reverse

  def opsTree(wf: Workflow): Tree[MongoOp] = wf.cata(opTreeAlg)

  def foldLeftJoinSubTree(fst: Tree[MongoOp], snd: Tree[MongoOp]): Tree[MongoOp] =
    foldLeftOp.node(
      projectOp.node(groupOp.node(fst)),
      reduceOp.node(mapOp.node(snd)))

  val stdFoldLeftJoinSubTree: Tree[MongoOp] = foldLeftJoinSubTree(readOp.leaf, readOp.leaf)

  case class beOpsTree(expected: Tree[MongoOp]) extends Matcher[Tree[MongoOp]] {
    def apply[S <: Tree[MongoOp]](actual: Expectable[S]) = {
      result(expected === actual.value,
             "\ntrees are equal:\n" + expected.drawTree,
             "\ntrees are not equal:\nexpected\n" + expected.drawTree + "\nactual:\n" + actual.value.drawTree,
             actual,
             FailureDetails(actual.value.drawTree, expected.drawTree))
    }
  }

  def noConsecutiveProjectOps(wf: Workflow) =
    countOps(wf, { case WC($ProjectF(Embed(WC($ProjectF(_, _, _))), _, _)) => true }) aka "the occurrences of consecutive $project ops:" must_== 0
  def noConsecutiveSimpleMapOps(wf: Workflow) =
    countOps(wf, { case WC($SimpleMapF(Embed(WC($SimpleMapF(_, _, _))), _, _)) => true }) aka "the occurrences of consecutive $simpleMap ops:" must_== 0
  def maxAccumOps(wf: Workflow, max: Int) =
    countAccumOps(wf) aka "the number of $group ops:" must beLessThanOrEqualTo(max)
  def maxUnwindOps(wf: Workflow, max: Int) =
    countUnwindOps(wf) aka "the number of $unwind ops:" must beLessThanOrEqualTo(max)
  def maxMatchOps(wf: Workflow, max: Int) =
    countMatchOps(wf) aka "the number of $match ops:" must beLessThanOrEqualTo(max)
  def brokenProjectOps(wf: Workflow) =
    countOps(wf, { case WC($ProjectF(_, Reshape(shape), _)) => shape.isEmpty }) aka "$project ops with no fields"

  def danglingReferences(wf: Workflow) =
    wf.foldMap(_.unFix match {
      case IsSingleSource(op) =>
        simpleShape(op.src).map { shape =>
          val refs = Refs[WorkflowF].refs(op.wf)
          val missing = refs.collect { case v @ DocVar(_, Some(f)) if !shape.contains(f.flatten.head) => v }
          if (missing.isEmpty) Nil
          else List(missing.map(_.bson).mkString(", ") + " missing in\n" + Fix[WorkflowF](op.wf).render.shows)
        }.getOrElse(Nil)
      case _ => Nil
    }) aka "dangling references"

  def notBroken(wf: Workflow, checkDanglingRefs: Boolean) = {
    noConsecutiveProjectOps(wf)
    noConsecutiveSimpleMapOps(wf)
    if (checkDanglingRefs) danglingReferences(wf) must_== Nil else ok
    brokenProjectOps(wf) must_== 0
  }

  def notBrokenWithOps(wf: Workflow, expectedOps: IList[MongoOp], checkDanglingRefs: Boolean = true) = {
    notBroken(wf, checkDanglingRefs)
    ops(wf) must_== expectedOps
  }

  def notBrokenWithOpsTree(wf: Workflow, expectedOps: Tree[MongoOp], checkDanglingRefs: Boolean = true) = {
    notBroken(wf, checkDanglingRefs)
    AsResult(Equal[Tree[MongoOp]].equal(opsTree(wf), expectedOps) must_=== true).updateMessage(s"Expected:\n${expectedOps.drawTree}\nActual:\n${opsTree(wf).drawTree}")
  }

  def rootPushes(wf: Workflow) =
    wf.foldMap(_.unFix match {
      case WC(op @ $GroupF(src, Grouped(map), _)) if map.values.toList.contains($push($$ROOT)) && simpleShape(src).isEmpty => List(op)
      case _ => Nil
    }) aka "group ops pushing $$ROOT"

  def appropriateColumns0(wf: Workflow, q: Select[Fix[Sql]]) = {
    val fields = fieldNames(wf).map(_.filterNot(_ ≟ "_id"))
    fields aka "column order" must beSome(columnNames(q))
  }

  def appropriateColumns(wf: Workflow, q: Select[Fix[Sql]]) = {
    val fields = fieldNames(wf).map(_.filterNot(_ ≟ "_id"))
    (fields aka "column order" must beSome(columnNames(q))) or
      (fields must beSome(List(sigil.Quasar))) // NB: some edge cases (all constant projections) end up under "value" and aren't interesting anyway
  }
}
