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

package quasar.frontend.logicalplan

import slamdata.Predef._
import quasar._
import quasar.common.JoinType
import quasar.common.data.Data
import quasar.common.effect.NameGenerator
import quasar.contrib.shapeless._
import quasar.fp._
import quasar.fp.binder._
import quasar.fp.ski._
import quasar.frontend.logicalplan.{LogicalPlan => LP}

import scala.Predef.$conforms

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{Free => Freez, _}, Scalaz.{ToIdOps => _, _}
import shapeless.{Data => _, :: => _, _}

sealed abstract class Component[T, A] {
  def run(l: T, r: T): A = this match {
    case EquiCond(run0)  => run0(l, r)
    case LeftCond(run0)  => run0(l)
    case RightCond(run0) => run0(r)
    case OtherCond(run0) => run0(l, r)
    case NeitherCond(a)  => a
  }
}

/** A condition that refers to left and right sources using equality, so may be
  * rewritten into the join condition.
  */
final case class EquiCond[T, A](run0: (T, T) => A) extends Component[T, A]
/** A condition which refers only to the left source */
final case class LeftCond[T, A](run0: T => A) extends Component[T, A]
/** A condition which refers only to the right source. */
final case class RightCond[T, A](run0: T => A) extends Component[T, A]
/** A condition which refers to both sources but doesn't have the right shape to
  * become the join condition.
  */
final case class OtherCond[T, A](run0: (T, T) => A) extends Component[T, A]
/** An expression that doesn't refer to any source. */
final case class NeitherCond[T, A](run0: A) extends Component[T, A]

object Component {
  implicit def applicative[T]: Applicative[Component[T, ?]] =
    new Applicative[Component[T, ?]] {
      def point[A](a: => A) = NeitherCond(a)

      def ap[A, B](fa: => Component[T, A])(f: => Component[T, A => B]) =
        (fa, f) match {
          // A             // A => B
          case (NeitherCond(a), NeitherCond(g)) => NeitherCond(g(a))

          // A             // LP => A => B
          case (NeitherCond(a), LeftCond(g))    => LeftCond(g(_)(a))
          // A             // LP => A => B
          case (NeitherCond(a), RightCond(g))   => RightCond(g(_)(a))

          // LP => A       // A => B
          case (LeftCond(a),    NeitherCond(g)) => LeftCond(g <<< a) // lp => g(a(lp))
                                                                     // LP => A       // LP => A => B
          case (LeftCond(a),    LeftCond(g))    => LeftCond(lp => g(lp)(a(lp)))

          // LP => A       // A => B
          case (RightCond(a),   NeitherCond(g)) => RightCond(g <<< a)
          // LP => A       // LP => A => B
          case (RightCond(a),   RightCond(g))   => RightCond(lp => g(lp)(a(lp)))

          case (ca, cg)                         => OtherCond((l, r) => cg.run(l, r)(ca.run(l, r)))
        }
    }
}

final class Optimizer[T: Equal]
  (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) {
  import quasar.std.StdLib._
  import set._
  import structural._

  val lpr = new LogicalPlanR[T]

  private def countUsageƒ(target: Symbol): Algebra[LP, Int] = {
    case Free(symbol) if symbol ≟ target => 1
    case Let(ident, form, _) if ident ≟ target => form
    case x => x.fold
  }

  private def inlineƒ[A](target: Symbol, repl: LP[T]):
      LP[(T, T)] => LP[T] =
  {
    case Free(symbol) if symbol ≟ target => repl
    case Let(ident, form, body) if ident ≟ target =>
      Let(ident, form._2, body._1)
    case x => x.map(_._2)
  }

  val simplifyƒ: LP[T] => Option[LP[T]] = {
    case inv @ Invoke(func, _) => func.simplify(inv)
    case Let(ident, form, in) => form.project match {
      case Constant(_) | Free(_) =>
        in.transPara[T](inlineƒ(ident, form.project)).project.some
      case _ => in.cata(countUsageƒ(ident)) match {
        case 0 => in.project.some
        case 1 => in.transPara[T](inlineƒ(ident, form.project)).project.some
        case _ => None
      }
    }
    case _ => None
  }

  def simplify(t: T): T = t.transCata[T](repeatedly(simplifyƒ))

  /** Like `simplifyƒ`, but eliminates _all_ `Let` (and any bound `Free`) nodes.
    */
  val elideLets:
      LP[T] => Option[LP[T]] = {
    case Let(ident, form, in) =>
      in.transPara[T](inlineƒ(ident, form.project)).project.some
    case _ => None
  }

  val namesƒ: Algebra[LP, Set[Symbol]] = {
    case Free(name) => Set(name)
    case x          => x.fold
  }

  def uniqueName[F[_]: Functor: Foldable](
    prefix: String, plans: F[T]):
      Symbol = {
    val existingNames = plans.map(_.cata(namesƒ)).fold
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def loop(pre: String): Symbol =
      if (existingNames.contains(Symbol(prefix)))
        loop(pre + "_")
      else Symbol(prefix)

    loop(prefix)
  }

  val shapeƒ: GAlgebra[(T, ?), LP, Option[List[T]]] = {
    case Let(_, _, body) => body._2
    case Constant(Data.Obj(map)) =>
      Some(map.keys.map(n => lpr.constant(Data.Str(n))).toList)
    case Sort(src, _) => src._2
    case InvokeUnapply(DeleteKey, Sized(src, key)) =>
      src._2.map(_.filterNot(_ ≟ key._1))
    case InvokeUnapply(MakeMap, Sized(key, _)) => Some(List(key._1))
    case InvokeUnapply(MapConcat, srcs) => srcs.traverse(_._2).map(_.flatten)
    // NB: the remaining Invoke cases simply pass through or combine shapes
    //     from their inputs. It would be great if this information could be
    //     handled generically by the type system.
    case InvokeUnapply(Take, Sized(src, _)) => src._2
    case InvokeUnapply(Drop, Sized(src, _)) => src._2
    case InvokeUnapply(Filter, Sized(src, _)) => src._2
    case Join(_, _, _, _) =>
      Some(List(JoinDir.Left.const, JoinDir.Right.const))
    case InvokeUnapply(GroupBy, Sized(src, _)) => src._2
    case InvokeUnapply(identity.Squash, Sized(src)) => src._2
    case _ => None
  }

  private def preserveFree0[A](x: (T, A))(f: A => T)
      : T =
    x._1.project match {
      case Free(_) => x._1
      case _       => f(x._2)
    }

  // TODO: implement `preferDeletions` for other backends that may have more
  //       efficient deletes. Even better, a single function that takes a
  //       function parameter deciding which way each case should be converted.
  private val preferProjectionsƒ:
      GAlgebra[
        (T, ?),
        LP,
        (T, Option[List[T]])] = { node =>

    def preserveFree(x: (T, (T, Option[List[T]]))) =
      preserveFree0(x)(_._1)

    (node match {
      case InvokeUnapply(DeleteKey, Sized(src, key)) =>
        src._2._2.fold(
          DeleteKey(preserveFree(src), preserveFree(key)).embed) {
          keys =>
            val name = uniqueName("src", keys)
              lpr.let(name, preserveFree(src),
                MakeMapN(keys.filterNot(_ == key._2._1).map(f =>
                  f -> MapProject(lpr.free(name), f).embed): _*).embed)
        }
      case lp => lp.map(preserveFree).embed
    },
      shapeƒ(node.map(_._2)))
  }

  def preferProjections(t: T): T =
    boundPara(t)(preferProjectionsƒ)._1.transCata[T](repeatedly(simplifyƒ))

  /** Rewrite joins and subsequent filtering so that:
    * 1) Filtering that is equivalent to an equi-join is rewritten into the join condition.
    * 2) Filtering that refers to only side of the join is hoisted prior to the join.
    * The input plan must have been simplified already so that the structure
    * is in a canonical form for inspection.
    *
    * TODO: Separate the combining of filter and join from ...
    */
  def rewriteCrossJoinsƒ[F[_]: Monad: NameGenerator]: LP[(T, T)] => F[T] = { node =>
    def preserveFree(x: (T, T)) = preserveFree0(x)(ι)

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def flattenAnd: T => List[T] = {
      case Embed(InvokeUnapply(relations.And, ts)) => ts.unsized.flatMap(flattenAnd)
      case t                                       => List(t)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    def toComp(left: T, right: T)(c: T): (List[Component[T, T]], Component[T, T]) = {

      boundParaM[T, (List[Component[T, T]], ?), LP, Component[T, T]](c) {
        case t if t.map(_._1) ≟ left.project  => (Nil, LeftCond(ι))
        case t if t.map(_._1) ≟ right.project => (Nil, RightCond(ι))

        case InvokeUnapply(relations.Eq, Sized((_, LeftCond(lc)), (_, RightCond(rc)))) =>
          (Nil, EquiCond((l, r) => relations.Eq(lc(l), rc(r)).embed))

        case InvokeUnapply(relations.Eq, Sized((_, RightCond(rc)), (_, LeftCond(lc)))) =>
          (Nil, EquiCond((l, r) => relations.Eq(rc(r), lc(l)).embed))

        case InvokeUnapply(func @ UnaryFunc(_, _, _), Sized(t1)) =>
          (Nil, Func.Input1(t1).traverse(_._2).map(lpr.invoke(func, _)))

        // Preserve the previously-computed components in the `And`.
        // Return a constant `true` which is included as a no-op filter post-join.
        case t @ InvokeUnapply(func @ BinaryFunc(_, _, _), Sized(t1, t2)) if func == relations.And =>
          (List(t1._2, t2._2), NeitherCond(lpr.constant(Data.Bool(true))))

        case InvokeUnapply(func @ BinaryFunc(_, _, _), Sized(t1, t2)) =>
          (Nil, Func.Input2(t1, t2).traverse(_._2).map(lpr.invoke(func, _)))

        case InvokeUnapply(func @ TernaryFunc(_, _, _), Sized(t1, t2, t3)) =>
          (Nil, Func.Input3(t1, t2, t3).traverse(_._2).map(lpr.invoke(func, _)))

        case Let(ident, form, body) =>
          (Nil, (form._2 ⊛ body._2)(lpr.let(ident, _, _)))

        case t =>
          (Nil, NeitherCond(t.map(_._1).embed))
      }
    }

    def assembleCond(conds: List[T]): T =
      conds.foldLeft(lpr.constant(Data.True))(relations.And(_, _).embed)

    def newJoin(lSrc: T, rSrc: T, comps: List[Component[T, T]]): F[T] = {
      val equis    = comps.collect { case c @ EquiCond(_) => c }
      val lefts    = comps.collect { case c @ LeftCond(_) => c }
      val rights   = comps.collect { case c @ RightCond(_) => c }
      val others   = comps.collect { case c @ OtherCond(_) => c }
      val neithers = comps.collect { case c @ NeitherCond(_) => c }

      for {
        lName     <- freshSym[F]("leftSrc")
        rName     <- freshSym[F]("rightSrc")
        lJoinName <- freshSym[F]("leftJoin")
        rJoinName <- freshSym[F]("rightJoin")
        lFName    <- freshSym[F]("left")
        rFName    <- freshSym[F]("right")
        jName     <- freshSym[F]("joined")
      } yield {
        // NB: simplifying eagerly to make matching easier up the tree
        simplify(
          lpr.let(lName, lSrc,
            lpr.let(lFName,
              Filter(lpr.free(lName), assembleCond(lefts.map(_.run0(lpr.free(lName))))).embed,
              lpr.let(rName, rSrc,
                lpr.let(rFName,
                  Filter(lpr.free(rName), assembleCond(rights.map(_.run0(lpr.free(rName))))).embed,
                  lpr.let(jName,
                    Join(lpr.free(lFName), lpr.free(rFName), JoinType.Inner,
                      JoinCondition(lJoinName, rJoinName, assembleCond(equis.map(_.run(lpr.joinSideName(lJoinName), lpr.joinSideName(rJoinName)))))).embed,
                    Filter(lpr.free(jName), assembleCond(
                      others.map(_.run0(JoinDir.Left.projectFrom(lpr.free(jName)), JoinDir.Right.projectFrom(lpr.free(jName)))) ++
                      neithers.map(_.run0))).embed))))))
      }
    }

    node match {
      case InvokeUnapply(Filter, Sized((src, Embed(Join(joinL, joinR, JoinType.Inner, JoinCondition(lName, rName, joinCond0)))), (cond0, _))) =>
        val joinCond = joinCond0.transCata[T](orOriginal(elideLets))
        val cond = cond0.transCata[T](orOriginal(elideLets))
        val comps =
          flattenAnd(joinCond).traverse(toComp(lpr.joinSideName(lName), lpr.joinSideName(rName))).bifoldMap(ι)(ι) ++
          flattenAnd(cond).traverse(toComp(
            JoinDir.Left.projectFrom(src),
            JoinDir.Right.projectFrom(src))).bifoldMap(ι)(ι)
        newJoin(joinL, joinR, comps)

      case Join((srcL, _), (srcR, _), JoinType.Inner, JoinCondition(lName, rName, (_, joinCond0))) =>
        val joinCond = joinCond0.transCata[T](orOriginal(elideLets))
        newJoin(srcL, srcR, flattenAnd(joinCond).traverse(toComp(lpr.joinSideName(lName), lpr.joinSideName(rName))(_)).bifoldMap(ι)(ι))

      case _ => node.map(preserveFree).embed.point[F]
    }
  }

  /** Apply universal, type-oblivious transformations intended to
    * improve the performance of a query regardless of the backend. The
    * input is expected to come straight from the SQL^2 compiler or
    * another source of un-optimized queries.
    */
  val optimize: T => T =
    NonEmptyList[T => T](
      // Eliminate extraneous constants, etc.:
      simplify(_),

      // NB: must precede normalizeLets to eliminate possibility of shadowing:
      lpr.normalizeTempNames(_),

      // NB: must precede rewriteCrossJoins to normalize Filter/Join shapes:
      lpr.normalizeLets(_),

      // Now for the big one:
      (t: T) => boundParaS(t)(rewriteCrossJoinsƒ[State[Long, ?]]).evalZero[Long],

      // Eliminate trivial bindings introduced in rewriteCrossJoins:
      simplify(_),

      // Final pass to normalize the resulting plans for better matching in tests:
      lpr.normalizeLets(_),

      // This time, fix the names last so they will read naturally:
      lpr.normalizeTempNames(_)

    ).foldLeft1(_ >>> _)

  /** In `rewriteJoins` we maintain the association and nested level of let bindings
    * by not calling `normalizeLets` (as we do in `optimize`). We would like `rewriteJoins`
    * to be identical to `optimize`, but can only begin to address this after old mongo
    * is deleted.
    */
  val rewriteJoins: T => T =
    NonEmptyList[T => T](
      // Eliminate extraneous constants, etc.:
      simplify(_),

      // NB: must precede normalizeLets to eliminate possibility of shadowing:
      lpr.normalizeTempNames(_),

      // Now for the big one:
      (t: T) => boundParaS(t)(rewriteCrossJoinsƒ[State[Long, ?]]).evalZero[Long],

      // Eliminate trivial bindings introduced in rewriteCrossJoins:
      simplify(_),

      // This time, fix the names last so they will read naturally:
      lpr.normalizeTempNames(_)

    ).foldLeft1(_ >>> _)

  /** Pulls a nested GroupBy up to the immediate child of the nearest Arbitrary,
    * applying any intermediate expressions to the source of the GroupBy.
    */
  def pullUpGroupBy
    (tree: T)
    (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
      : T = {

    val paritionArbitraryƒ: T => Option[CoEnv[(T, T), LP, T]] = _.project match {
      case InvokeUnapply(agg.Arbitrary, Sized(_)) => none
      case InvokeUnapply(set.GroupBy, Sized(src, keys)) => some(CoEnv(-\/((src, keys))))
      case other => some(CoEnv(\/-(other)))
    }

    val partitionArbitrary: T => Option[Freez[LP, (T, T)]] =
      _.anaM[Freez[LP, (T, T)]](paritionArbitraryƒ)

    def unfold: LP[T] => LP[T] = {
      case InvokeUnapply(agg.Arbitrary, Sized(arg)) =>
        val partitioned = partitionArbitrary(arg)

        val existsNonMapping =
          partitioned.exists(ftt =>
            Recursive[Freez[LP, (T, T)], CoEnv[(T, T), LP, ?]].any(ftt)(_.project match {
              case CoEnv(\/-(InvokeUnapply(fn, _))) =>
                fn.effect =/= Mapping
              case _ => false
            }))

        val arbArg = if (existsNonMapping) {
          partitioned.flatMap(part => part.findLeft(_ => true) map {
            case (src, keys) =>
              Invoke(set.GroupBy, Func.Input2(
                part.cata(interpret(_ => src, (_: LP[T]).embed)),
                keys))
          }).fold(arg)(_.embed)
        } else {
          arg
        }

        Invoke(agg.Arbitrary, Func.Input1(arbArg))

      case other => other
    }

    tree.transAna[T](unfold)
  }
}
