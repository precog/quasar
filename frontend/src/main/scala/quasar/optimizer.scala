/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.contrib.shapeless._
import quasar.fp.binder._
import quasar.namegen._
import quasar.sql.JoinDir

import matryoshka._, Recursive.ops._, FunctorT.ops._, TraverseT.ownOps._
import scalaz._, Scalaz._
import shapeless.{Data => _, :: => _, _}

object Optimizer {
  import LogicalPlan._
  import quasar.std.StdLib._
  import set._
  import structural._

  private def countUsageƒ(target: Symbol): Algebra[LogicalPlan, Int] = {
    case FreeF(symbol) if symbol == target => 1
    case LetF(ident, form, _) if ident == target => form
    case x => x.fold
  }

  private def inlineƒ[T[_[_]], A](target: Symbol, repl: LogicalPlan[T[LogicalPlan]]):
      LogicalPlan[(T[LogicalPlan], T[LogicalPlan])] => LogicalPlan[T[LogicalPlan]] =
  {
    case FreeF(symbol) if symbol == target => repl
    case LetF(ident, form, body) if ident == target =>
      LetF(ident, form._2, body._1)
    case x => x.map(_._2)
  }

  def simplifyƒ[T[_[_]]: Recursive: Corecursive]:
      LogicalPlan[T[LogicalPlan]] => Option[LogicalPlan[T[LogicalPlan]]] = {
    case inv @ InvokeF(func, _) => func.simplify(inv)
    case LetF(ident, form, in) => form.project match {
      case ConstantF(_)
         | FreeF(_) => in.transPara(inlineƒ(ident, form.project)).project.some
      case _ => in.cata(countUsageƒ(ident)) match {
        case 0 => in.project.some
        case 1 => in.transPara(inlineƒ(ident, form.project)).project.some
        case _ => None
      }
    }
    case _ => None
  }

  /** Like `simplifyƒ`, but eliminates _all_ `Let` (and any bound `Free`) nodes.
    */
  def elideLets[T[_[_]]: Recursive: FunctorT]:
      LogicalPlan[T[LogicalPlan]] => Option[LogicalPlan[T[LogicalPlan]]] = {
    case LetF(ident, form, in) =>
      in.transPara(inlineƒ(ident, form.project)).project.some
    case _ => None
  }

  def simplify(t: Fix[LogicalPlan]): Fix[LogicalPlan] = t.transCata(repeatedly(simplifyƒ))

  val namesƒ: Algebra[LogicalPlan, Set[Symbol]] = {
    case FreeF(name) => Set(name)
    case x           => x.fold
  }

  def uniqueName[F[_]: Functor: Foldable](
    prefix: String, plans: F[Fix[LogicalPlan]]):
      Symbol = {
    val existingNames = plans.map(_.cata(namesƒ)).fold
    def loop(pre: String): Symbol =
      if (existingNames.contains(Symbol(prefix)))
        loop(pre + "_")
      else Symbol(prefix)

    loop(prefix)
  }

  val shapeƒ: GAlgebra[(Fix[LogicalPlan], ?), LogicalPlan, Option[List[Fix[LogicalPlan]]]] = {
    case LetF(_, _, body) => body._2
    case ConstantF(Data.Obj(map)) =>
      Some(map.keys.map(n => Constant(Data.Str(n))).toList)
    case InvokeFUnapply(DeleteField, Sized(src, field)) =>
      src._2.map(_.filterNot(_ == field._1))
    case InvokeFUnapply(MakeObject, Sized(field, _)) => Some(List(field._1))
    case InvokeFUnapply(ObjectConcat, srcs) => srcs.traverse(_._2).map(_.flatten)
    // NB: the remaining InvokeF cases simply pass through or combine shapes
    //     from their inputs. It would be great if this information could be
    //     handled generically by the type system.
    case InvokeFUnapply(OrderBy, Sized(src, _, _)) => src._2
    case InvokeFUnapply(Take, Sized(src, _)) => src._2
    case InvokeFUnapply(Drop, Sized(src, _)) => src._2
    case InvokeFUnapply(Filter, Sized(src, _)) => src._2
    case InvokeFUnapply(InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin, _) =>
      Some(List(Constant(Data.Str("left")), Constant(Data.Str("right"))))
    case InvokeFUnapply(GroupBy, Sized(src, _)) => src._2
    case InvokeFUnapply(Distinct, Sized(src, _)) => src._2
    case InvokeFUnapply(DistinctBy, Sized(src, _)) => src._2
    case InvokeFUnapply(identity.Squash, Sized(src)) => src._2
    case _ => None
  }

  def preserveFree0[A](x: (Fix[LogicalPlan], A))(f: A => Fix[LogicalPlan]):
      Fix[LogicalPlan] = x._1.unFix match {
    case FreeF(_) => x._1
    case _        => f(x._2)
  }

  // TODO: implement `preferDeletions` for other backends that may have more
  //       efficient deletes. Even better, a single function that takes a
  //       function parameter deciding which way each case should be converted.
  private val preferProjectionsƒ:
      GAlgebra[
        (Fix[LogicalPlan], ?),
        LogicalPlan,
        (Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]])] = { node =>

    def preserveFree(x: (Fix[LogicalPlan], (Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]]))) =
      preserveFree0(x)(_._1)

    (node match {
      case InvokeFUnapply(DeleteField, Sized(src, field)) =>
        src._2._2.fold(
          Invoke(DeleteField, Func.Input2(preserveFree(src), preserveFree(field)))) {
          fields =>
            val name = uniqueName("src", fields)
              Let(name, preserveFree(src),
                Fix(MakeObjectN(fields.filterNot(_ == field._2._1).map(f =>
                  f -> Invoke(ObjectProject, Func.Input2(Free(name), f))): _*)))
        }
      case lp => Fix(lp.map(preserveFree))
    },
      shapeƒ(node.map(_._2)))
  }

  def preferProjections(t: Fix[LogicalPlan]): Fix[LogicalPlan] =
    boundPara(t)(preferProjectionsƒ)._1.transCata(repeatedly(simplifyƒ))

  val elideTypeCheckƒ: Algebra[LogicalPlan, Fix[LogicalPlan]] = {
    case LetF(n, b, Fix(TypecheckF(Fix(FreeF(nf)), _, cont, _)))
        if n == nf =>
      Let(n, b, cont)
    case x => Fix(x)
  }

  sealed trait Component[A] {
    def run(l: Fix[LogicalPlan], r: Fix[LogicalPlan]): A
  }
  // A condition that refers to left and right sources using equality, so may
  // be rewritten into the join condition:
  final case class EquiCond[A](run0: (Fix[LogicalPlan], Fix[LogicalPlan]) => A) extends Component[A] {
    def run(l: Fix[LogicalPlan], r: Fix[LogicalPlan]) = run0(l,r)
  }
  // A condition which refers only to the left source:
  final case class LeftCond[A](run0: Fix[LogicalPlan] => A) extends Component[A] {
    def run(l: Fix[LogicalPlan], r: Fix[LogicalPlan]) = run0(l)
  }
  // A condition which refers only to the right source:
  final case class RightCond[A](run0: Fix[LogicalPlan] => A) extends Component[A] {
    def run(l: Fix[LogicalPlan], r: Fix[LogicalPlan]) = run0(r)
  }
  // A condition which refers to both sources but doesn't have the right shape
  // to become the join condition:
  final case class OtherCond[A](run0: (Fix[LogicalPlan], Fix[LogicalPlan]) => A) extends Component[A] {
    def run(l: Fix[LogicalPlan], r: Fix[LogicalPlan]) = run0(l,r)
  }
  // An expression that doesn't refer to any source.
  final case class NeitherCond[A](run0: A) extends Component[A] {
    def run(l: Fix[LogicalPlan], r: Fix[LogicalPlan]) = run0
  }

  // TODO add scalaz propery test
  implicit val ComponentApplicative: Applicative[Component] =
    new Applicative[Component] {
      def point[A](a: => A): Component[A] = NeitherCond(a)

      def ap[A, B](fa: => Component[A])(f: => Component[A => B]): Component[B] =
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

  /** Rewrite joins and subsequent filtering so that:
    * 1) Filtering that is equivalent to an equi-join is rewritten into the join condition.
    * 2) Filtering that refers to only side of the join is hoisted prior to the join.
    * The input plan must have been simplified already so that the structure
    * is in a canonical form for inspection.
    */
  val rewriteCrossJoinsƒ: LogicalPlan[(Fix[LogicalPlan], Fix[LogicalPlan])] => State[NameGen, Fix[LogicalPlan]] = { node =>
    import quasar.fp._

    def preserveFree(x: (Fix[LogicalPlan], Fix[LogicalPlan])) = preserveFree0(x)(ι)

    def flattenAnd: Fix[LogicalPlan] => List[Fix[LogicalPlan]] = {
      case Fix(InvokeFUnapply(relations.And, ts)) => ts.unsized.flatMap(flattenAnd)
      case t                                      => List(t)
    }

    def toComp(left: Fix[LogicalPlan], right: Fix[LogicalPlan])(c: Fix[LogicalPlan]):
        Component[Fix[LogicalPlan]] = {
      c.para[Component[Fix[LogicalPlan]]] {
        case t if t.map(_._1) ≟ left.unFix  => LeftCond(ι)
        case t if t.map(_._1) ≟ right.unFix => RightCond(ι)

        case InvokeFUnapply(relations.Eq, Sized((_, LeftCond(lc)), (_, RightCond(rc)))) =>
          EquiCond((l, r) => Fix(relations.Eq(lc(l), rc(r))))
        case InvokeFUnapply(relations.Eq, Sized((_, RightCond(rc)), (_, LeftCond(lc)))) =>
          EquiCond((l, r) => Fix(relations.Eq(rc(r), lc(l))))

        case InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(t1)) =>
          Func.Input1(t1).map(_._2).sequence[Component, Fix[LogicalPlan]].map(ts => Fix(InvokeF(func, ts)))

        case InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(t1, t2)) =>
          Func.Input2(t1, t2).map(_._2).sequence[Component, Fix[LogicalPlan]].map(ts => Fix(InvokeF(func, ts)))

        case InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(t1, t2, t3)) =>
          Func.Input3(t1, t2, t3).map(_._2).sequence[Component, Fix[LogicalPlan]].map(ts => Fix(InvokeF(func, ts)))

        case t => NeitherCond(Fix(t.map(_._1)))
      }
    }

    def assembleCond(conds: List[Fix[LogicalPlan]]): Fix[LogicalPlan] =
      conds.foldLeft(Constant(Data.True))((acc, c) => Fix(relations.And(acc, c)))

    def newJoin(lSrc: Fix[LogicalPlan], rSrc: Fix[LogicalPlan], comps: List[Component[Fix[LogicalPlan]]]):
        State[NameGen, Fix[LogicalPlan]] = {
      val equis    = comps.collect { case c @ EquiCond(_) => c }
      val lefts    = comps.collect { case c @ LeftCond(_) => c }
      val rights   = comps.collect { case c @ RightCond(_) => c }
      val others   = comps.collect { case c @ OtherCond(_) => c }
      val neithers = comps.collect { case c @ NeitherCond(_) => c }

      for {
        lName  <- freshName("leftSrc")
        lFName <- freshName("left")
        rName  <- freshName("rightSrc")
        rFName <- freshName("right")
        jName  <- freshName("joined")
      } yield {
        // NB: simplifying eagerly to make matching easier up the tree
        simplify(
          Let(lName, lSrc,
            Let(lFName, Fix(Filter(Free(lName), assembleCond(lefts.map(_.run0(Free(lName)))))),
              Let(rName, rSrc,
                Let(rFName, Fix(Filter(Free(rName), assembleCond(rights.map(_.run0(Free(rName)))))),
                  Let(jName,
                    Fix(InnerJoin(Free(lFName), Free(rFName),
                      assembleCond(equis.map(_.run(Free(lFName), Free(rFName)))))),
                    Fix(Filter(Free(jName), assembleCond(
                      others.map(_.run0(JoinDir.Left.projectFrom(Free(jName)), JoinDir.Right.projectFrom(Free(jName)))) ++
                      neithers.map(_.run0))))))))))
      }
    }


    node match {
      case InvokeFUnapply(Filter, Sized((src, Fix(InvokeFUnapply(InnerJoin, Sized(joinL, joinR, joinCond)))), (cond, _))) =>
        val comps = flattenAnd(joinCond).map(toComp(joinL, joinR)) ++
                    flattenAnd(cond).map(toComp(JoinDir.Left.projectFrom(src), JoinDir.Right.projectFrom(src)))
        newJoin(joinL, joinR, comps)
      case InvokeFUnapply(InnerJoin, Sized((srcL, _), (srcR, _), (_, joinCond))) =>
        newJoin(srcL, srcR, flattenAnd(joinCond).map(toComp(srcL, srcR)))
      case _ => State.state(Fix(node.map(preserveFree)))
    }
  }

  /** Apply universal, type-oblivious transformations intended to
    * improve the performance of a query regardless of the backend. The
    * input is expected to come straight from the SQL^2 compiler or
    * another source of un-optimized queries.
    */
  val optimize: Fix[LogicalPlan] => Fix[LogicalPlan] =
    NonEmptyList[Fix[LogicalPlan] => Fix[LogicalPlan]](
      // Eliminate extraneous constants, etc.:
      _.transCata(repeatedly(simplifyƒ)),

      // NB: must precede normalizeLets to eliminate possibility of shadowing:
      normalizeTempNames,

      // NB: must precede rewriteCrossJoins to normalize Filter/Join shapes:
      normalizeLets,

      // Now for the big one:
      boundParaS(_)(rewriteCrossJoinsƒ).evalZero,

      // Eliminate trivial bindings introduced in rewriteCrossJoins:
      _.transCata(repeatedly(simplifyƒ)),

      // Final pass to normalize the resulting plans for better matching in tests:
      normalizeLets,

      // This time, fix the names last so they will read naturally:
      normalizeTempNames

    ).foldLeft1(_ >>> _)
}
