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

package quasar.frontend.logicalplan

import quasar.Predef._
import quasar._, SemanticError.TypeError
import quasar.common.SortDir
import quasar.contrib.pathy.FPath
import quasar.contrib.shapeless._
import quasar.fp._
import quasar.fp.ski._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}
import quasar.namegen._

import scala.Predef.$conforms
import scala.Symbol

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._, Validation.{failure, success}, Validation.FlatMap._
import shapeless.{nat, Nat, Sized}

final case class NamedConstraint[T]
  (name: Symbol, inferred: Type, term: T)
final case class ConstrainedPlan[T]
  (inferred: Type, constraints: List[NamedConstraint[T]], plan: T)

final class LogicalPlanR[T]
  (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) {
  import quasar.std.DateLib._, quasar.std.StdLib, StdLib._, structural._

  def read(path: FPath) = lp.read[T](path).embed
  def constant(data: Data) = lp.constant[T](data).embed
  def invoke[N <: Nat](func: GenericFunc[N], values: Func.Input[T, N]) =
    Invoke(func, values).embed
  def invoke1(func: GenericFunc[nat._1], v1: T) =
    invoke[nat._1](func, Func.Input1(v1))
  def invoke2(func: GenericFunc[nat._2], v1: T, v2: T) =
    invoke[nat._2](func, Func.Input2(v1, v2))
  def invoke3(func: GenericFunc[nat._3], v1: T, v2: T, v3: T) =
    invoke[nat._3](func, Func.Input3(v1, v2, v3))
  def free(name: Symbol) = lp.free[T](name).embed
  def let(name: Symbol, form: T, in: T) =
    lp.let(name, form, in).embed
  def sort(src: T, order: NonEmptyList[(T, SortDir)]) =
    lp.sort(src, order).embed
  def typecheck(expr: T, typ: Type, cont: T, fallback: T) =
    lp.typecheck(expr, typ, cont, fallback).embed
  def temporalTrunc(part: TemporalPart, src: T) =
    lp.temporalTrunc(part, src).embed

  // NB: this can't currently be generalized to Binder, because the key type
  //     isn't exposed there.
  def renameƒ[M[_]: Monad](f: Symbol => M[Symbol])
      : CoalgebraM[M, LP, (Map[Symbol, Symbol], T)] = {
    case (bound, t) =>
      t.project match {
        case Let(sym, expr, body) =>
          f(sym).map(sym1 =>
            lp.let(sym1, (bound, expr), (bound + (sym -> sym1), body)))
        case Free(sym) =>
          lp.free(bound.get(sym).getOrElse(sym)).point[M]
        case t => t.strengthL(bound).point[M]
      }
  }

  def rename[M[_]: Monad](f: Symbol => M[Symbol])(t: T): M[T] =
    (Map[Symbol, Symbol](), t).anaM[T](renameƒ(f))

  def normalizeTempNames(t: T) =
    rename[State[NameGen, ?]](κ(freshName("tmp")))(t).evalZero

  /** Per the following:
    * 1. Successive Lets are re-associated to the right:
    *    (let a = (let b = x1 in x2) in x3) becomes
    *    (let b = x1 in (let a = x2 in x3))
    * 2. Lets are "hoisted" outside of Invoke and Typecheck nodes:
    *    (add (let a = x1 in x2) (let b = x3 in x4)) becomes
    *    (let a = x1 in (let b = x3 in (add x2 x4))
    * Note that this is safe only if all bound names are unique; otherwise
    * it could create spurious shadowing. normalizeTempNames is recommended.
    * NB: at the moment, Lets are only hoisted one level.
    */
  val normalizeLetsƒ: LP[T] => Option[LP[T]] = {
    case Let(b, Embed(Let(a, x1, x2)), x3) =>
      lp.let(a, x1, let(b, x2, x3)).some

    // TODO generalize the following three `GenericFunc` cases
    case InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1)) => a1 match {
      case Embed(Let(a, x1, x2)) =>
        lp.let(a, x1, invoke[nat._1](func, Func.Input1(x2))).some
      case _ => None
    }

    case InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2)) => (a1, a2) match {
      case (Embed(Let(a, x1, x2)), a2) =>
        lp.let(a, x1, invoke[nat._2](func, Func.Input2(x2, a2))).some
      case (a1, Embed(Let(a, x1, x2))) =>
        lp.let(a, x1, invoke[nat._2](func, Func.Input2(a1, x2))).some
      case _ => None
    }

    case InvokeUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3)) => (a1, a2, a3) match {
      case (Embed(Let(a, x1, x2)), a2, a3) =>
        lp.let(a, x1, invoke[nat._3](func, Func.Input3(x2, a2, a3))).some
      case (a1, Embed(Let(a, x1, x2)), a3) =>
        lp.let(a, x1, invoke[nat._3](func, Func.Input3(a1, x2, a3))).some
      case (a1, a2, Embed(Let(a, x1, x2))) =>
        lp.let(a, x1, invoke[nat._3](func, Func.Input3(a1, a2, x2))).some
      case _ => None
    }

    case Typecheck(Embed(Let(a, x1, x2)), typ, cont, fallback) =>
      lp.let(a, x1, typecheck(x2, typ, cont, fallback)).some
    case Typecheck(expr, typ, Embed(Let(a, x1, x2)), fallback) =>
      lp.let(a, x1, typecheck(expr, typ, x2, fallback)).some
    case Typecheck(expr, typ, cont, Embed(Let(a, x1, x2))) =>
      lp.let(a, x1, typecheck(expr, typ, cont, x2)).some
    case t => None
  }

  def normalizeLets(t: T) = t.transAna[T](repeatedly(normalizeLetsƒ))

  type Typed[F[_]] = Cofree[F, Type]
  type SemValidation[A] = ValidationNel[SemanticError, A]
  type SemDisj[A] = NonEmptyList[SemanticError] \/ A

  def inferTypes(typ: Type, term: T):
      SemValidation[Typed[LP]] = {

    (term.project match {
      case Read(c) => success(lp.read[Typed[LP]](c))

      case Constant(d) => success(lp.constant[Typed[LP]](d))

      case Invoke(func, args) => for {
        types <- func.untpe(typ)
        args0 <- types.zip(args).traverse {
          case (t, arg) => inferTypes(t, arg)
        }
      } yield lp.invoke(func, args0)

      case Free(n) => success(lp.free[Typed[LP]](n))

      case Let(n, form, in) =>
        inferTypes(typ, in).flatMap { in0 =>
          val fTyp = in0.collect {
            case Cofree(typ0, Free(n0)) if n0 == n => typ0
          }.concatenate(Type.TypeGlbMonoid)
          inferTypes(fTyp, form).map(lp.let[Typed[LP]](n, _, in0))
        }

      case Sort(src, ords) =>
        (inferTypes(typ, src) ⊛ ords.traverse { case (a, d) => inferTypes(Type.Top, a) strengthR d })(lp.sort[Typed[LP]](_, _))

      case TemporalTrunc(part, src) =>
        typ.contains(Type.Temporal).fold(
          inferTypes(Type.Temporal, src) ∘ (lp.temporalTrunc[Typed[LP]](part, _)),
          failure((TypeError(Type.Temporal, typ, none): SemanticError).wrapNel)
        )

      case Typecheck(expr, t, cont, fallback) =>
        (inferTypes(t, expr) ⊛ inferTypes(typ, cont) ⊛ inferTypes(typ, fallback))(
          lp.typecheck[Typed[LP]](_, t, _, _))

    }).map(Cofree(typ, _))
  }

  private def lift[A](v: SemDisj[A]): NameT[SemDisj, A] =
    quasar.namegen.lift[SemDisj](v)

  /** This function compares the inferred (required) type with the possible type
    * from the collection.
    * • if it’s a const type, replace the node with a constant
    * • if the possible is a subtype of the inferred, we’re good
    * • if the inferred is a subtype of the possible, we need a runtime check
    * • otherwise, we fail
    */
  private def unifyOrCheck(inf: Type, poss: Type, term: T)
      : NameT[SemDisj, ConstrainedPlan[T]] = {
    if (inf.contains(poss))
      emit(ConstrainedPlan(poss, Nil, poss match {
        case Type.Const(d) => constant(d)
        case _             => term
      }))
      else if (poss.contains(inf)) {
        emitName(freshName("check").map(name =>
          ConstrainedPlan(inf, List(NamedConstraint(name, inf, term)), free(name))))
      }
      else lift((SemanticError.genericError(s"You provided a ${poss.shows} where we expected a ${inf.shows} in $term")).wrapNel.left)
  }

  private def appConst
    (constraints: ConstrainedPlan[T], fallback: T) =
    constraints.constraints.foldLeft(constraints.plan)((acc, con) =>
      let(con.name, con.term,
        typecheck(free(con.name), con.inferred, acc, fallback)))

  /** This inserts a constraint on a node that might not strictly require a type
    * check. It protects operations (EG, array flattening) that need a certain
    * shape.
    */
  private def ensureConstraint
    (constraints: ConstrainedPlan[T], fallback: T)
      : State[NameGen, T] = {
    val ConstrainedPlan(typ, consts, term) = constraints
      (consts match {
        case Nil =>
          freshName("check").map(name =>
            ConstrainedPlan(typ, List(NamedConstraint(name, typ, term)), free(name)))
        case _   => constraints.point[State[NameGen, ?]]
      }).map(appConst(_, fallback))
  }

  // TODO: This can perhaps be decomposed into separate folds for annotating
  //       with “found” types, folding constants, and adding runtime checks.
  // FIXME: No exhaustiveness checking here
  val checkTypesƒ:
      ((Type, LP[ConstrainedPlan[T]])) => NameT[SemDisj, ConstrainedPlan[T]] = {
    case (inf, term) =>
      def applyConstraints
        (poss: Type, constraints: ConstrainedPlan[T])
        (f: T => T) =
        unifyOrCheck(
          inf,
          poss,
          f(appConst(constraints, constant(Data.NA))))

      term match {
        case Read(c)         => unifyOrCheck(inf, Type.Top, read(c))
        case Constant(d)     => unifyOrCheck(inf, Type.Const(d), constant(d))
        case InvokeUnapply(MakeObject, Sized(name, value)) =>
          lift(MakeObject.tpe(Func.Input2(name, value).map(_.inferred)).disjunction).flatMap(
            applyConstraints(_, value)(MakeObject(name.plan, _).embed))
        case InvokeUnapply(MakeArray, Sized(value)) =>
          lift(MakeArray.tpe(Func.Input1(value).map(_.inferred)).disjunction).flatMap(
            applyConstraints(_, value)(MakeArray(_).embed))
        // TODO: Move this case to the Mongo planner once type information is
        //       available there.
        case InvokeUnapply(ConcatOp, Sized(left, right)) =>
          val (types, constraints0, terms) = Func.Input2(left, right).map {
            case ConstrainedPlan(in, con, pl) => (in, (con, pl))
          }.unzip3[Type, List[NamedConstraint[T]], T]

          val constraints = constraints0.unsized.flatten

          lift(ConcatOp.tpe(types).disjunction).flatMap[NameGen, ConstrainedPlan[T]](poss => poss match {
            case t if Type.Str.contains(t) => unifyOrCheck(inf, poss, invoke(string.Concat, terms))
            case t if t.arrayLike => unifyOrCheck(inf, poss, invoke(ArrayConcat, terms))
            case _                => lift(-\/(NonEmptyList(SemanticError.GenericError("can't concat mixed/unknown types"))))
          }).map(cp =>
            cp.copy(constraints = cp.constraints ++ constraints))
        case InvokeUnapply(relations.Or, Sized(left, right)) =>
          lift(relations.Or.tpe(Func.Input2(left, right).map(_.inferred)).disjunction).flatMap(unifyOrCheck(inf, _, relations.Or(left, right).map(appConst(_, constant(Data.NA))).embed))
        case InvokeUnapply(structural.FlattenArray, Sized(arg)) =>
          for {
            types <- lift(structural.FlattenArray.tpe(Func.Input1(arg).map(_.inferred)).disjunction)
            consts <- emitName[SemDisj, Func.Input[T, nat._1]](Func.Input1(arg).traverse(ensureConstraint(_, constant(Data.Arr(List(Data.NA))))))
            plan  <- unifyOrCheck(inf, types, invoke[nat._1](structural.FlattenArray, consts))
          } yield plan
        case InvokeUnapply(structural.FlattenMap, Sized(arg)) => for {
          types <- lift(structural.FlattenMap.tpe(Func.Input1(arg).map(_.inferred)).disjunction)
          consts <- emitName[SemDisj, Func.Input[T, nat._1]](Func.Input1(arg).traverse(ensureConstraint(_, constant(Data.Obj(ListMap("" -> Data.NA))))))
          plan  <- unifyOrCheck(inf, types, invoke(structural.FlattenMap, consts))
        } yield plan
        case InvokeUnapply(func @ NullaryFunc(_, _, _, _), Sized()) =>
          handleGenericInvoke(inf, func, Sized[List]())
        case InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1)) =>
          handleGenericInvoke(inf, func, Func.Input1(a1))
        case InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2)) =>
          handleGenericInvoke(inf, func, Func.Input2(a1, a2))
        case InvokeUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3)) =>
          handleGenericInvoke(inf, func, Func.Input3(a1, a2, a3))
        case Typecheck(expr, typ, cont, fallback) =>
          unifyOrCheck(inf, Type.glb(cont.inferred, typ), typecheck(expr.plan, typ, cont.plan, fallback.plan))
        case Let(name, value, in) =>
          unifyOrCheck(inf, in.inferred, let(name, appConst(value, constant(Data.NA)), appConst(in, constant(Data.NA))))
        // TODO: Get the possible type from the LetF
        case Free(v) => emit(ConstrainedPlan(inf, Nil, free(v)))
        case Sort(expr, ords) =>
          unifyOrCheck(inf, expr.inferred, sort(appConst(expr, constant(Data.NA)), ords map (_ leftMap (appConst(_, constant(Data.NA))))))
        case TemporalTrunc(part, src) =>
          val typer: Func.Typer[Nat._1] = StdLib.partialTyperV[nat._1] {
              case Sized(Type.Const(d @ Data.Date(_)))      => truncDate(part, d).validationNel ∘ (Type.Const(_))
              case Sized(Type.Const(t @ Data.Time(_)))      => truncTime(part, t).validationNel ∘ (Type.Const(_))
              case Sized(Type.Const(t @ Data.Timestamp(_))) => truncTimestamp(part, t).validationNel ∘ (Type.Const(_))
              case Sized(t)                                 => t.success
            }

          val constructLPNode: Func.Input[T, Nat._1] => T = { case Sized(i) => temporalTrunc(part, i) }

          handleInvoke(inf, typer, constructLPNode, Func.Input1(src))
      }
  }

  private def handleInvoke[N <: Nat](
    inf: Type,
    typer: Func.Typer[N],
    constructLPNode: Func.Input[T, N] => T,
    args: Func.Input[ConstrainedPlan[T], N]
  ): NameT[SemDisj, ConstrainedPlan[T]] = {
    val (types, constraints0, terms) = args.map {
      case ConstrainedPlan(in, con, pl) => (in, (con, pl))
    }.unzip3[Type, List[NamedConstraint[T]], T]

    val constraints = constraints0.unsized.flatten

    lift(typer(types).disjunction).flatMap(
      unifyOrCheck(inf, _, constructLPNode(terms))).map(cp =>
      cp.copy(constraints = cp.constraints ++ constraints))
  }

  private def handleGenericInvoke[N <: Nat](
    inf: Type, func: GenericFunc[N], args: Func.Input[ConstrainedPlan[T], N]
  ): NameT[SemDisj, ConstrainedPlan[T]] = {
    val constructLPNode = invoke(func, _: Func.Input[T, N])
    func.effect match {
      case Mapping =>
        handleInvoke(inf, func.typer0, constructLPNode, args)
      case _ =>
        lift(func.typer0(args.map(_.inferred)).disjunction).flatMap(
          unifyOrCheck(inf, _, constructLPNode(args.map(appConst(_, constant(Data.NA))))))
    }
  }

  type SemNames[A] = NameT[SemDisj, A]

  def ensureCorrectTypes(term: T):
      ValidationNel[SemanticError, T] = {
    // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
    import StateT.stateTMonadState

    inferTypes(Type.Top, term).flatMap(
      _.cataM(liftTM(checkTypesƒ)).map(appConst(_, constant(Data.NA))).evalZero.validation)
  }

  // TODO: Generalize this to Binder
  def lpParaZygoHistoM[M[_]: Monad, A, B](
    t: T)(
    f: LP[(T, B)] => B,
      g: LP[Cofree[LP, (B, A)]] => M[A]):
      M[A] = {
    def loop(t: T, bind: Map[Symbol, Cofree[LP, (B, A)]]):
        M[Cofree[LP, (B, A)]] = {
      lazy val default: M[Cofree[LP, (B, A)]] = for {
        lp <- t.project.traverse(x => for {
          co <- loop(x, bind)
        } yield ((x, co.head._1), co))
        (xb, co) = lp.unfzip
        b = f(xb)
        a <- g(co)
      } yield Cofree((b, a), co)

      t.project match {
        case Free(name)            => bind.get(name).fold(default)(_.point[M])
        case Let(name, form, body) => for {
          form1 <- loop(form, bind)
          rez   <- loop(body, bind + (name -> form1))
        } yield rez
        case _                     => default
      }
    }

    for {
      rez <- loop(t, Map())
    } yield rez.head._2
  }

  def lpParaZygoHistoS[S, A, B] = lpParaZygoHistoM[State[S, ?], A, B] _
  def lpParaZygoHisto[A, B] = lpParaZygoHistoM[Id, A, B] _

  /** The set of paths referenced in the given plan. */
  def paths(lp: T): Set[FPath] =
    lp.foldMap(_.cata[Set[FPath]] {
      case Read(p) => Set(p)
      case other   => other.fold
    })
}
