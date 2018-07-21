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
import quasar.common.{phase, CIName, JoinType, PhaseResultTell, SortDir}
import quasar.common.data.{Data, DataDateTimeExtractors}
import quasar.common.effect.NameGenerator
import quasar.contrib.pathy._
import quasar.contrib.shapeless._
import quasar.fp.ski._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}

import scala.Predef.$conforms
import scala.Symbol

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import scalaz._, Scalaz._, Validation.{failureNel, success}, Validation.FlatMap._
import shapeless.{nat, Nat, Sized}

final case class NamedConstraint[T](name: Symbol, inferred: Type, term: T)

final case class ConstrainedPlan[T](
    inferred: Type,
    constraints: List[NamedConstraint[T]],
    plan: T)

// TODO: Move constraints to methods and/or pull the constructors into own class.
final class LogicalPlanR[T](implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) {
  import quasar.std.StdLib, StdLib._, structural._
  import quasar.time.TemporalPart

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

  def joinSideName(name: Symbol) = lp.joinSideName[T](name).embed

  def join(left: T, right: T, tpe: JoinType, cond: JoinCondition[T]) =
    lp.join(left, right, tpe, cond).embed

  def free(name: Symbol) = lp.free[T](name).embed

  def let(name: Symbol, form: T, in: T) =
    lp.let(name, form, in).embed

  def sort(src: T, order: NonEmptyList[(T, SortDir)]) =
    lp.sort(src, order).embed

  def typecheck(expr: T, typ: Type, cont: T, fallback: T) =
    lp.typecheck(expr, typ, cont, fallback).embed

  def temporalTrunc(part: TemporalPart, src: T) =
    lp.temporalTrunc(part, src).embed

  object ArrayInflation {
    def unapply[N <: Nat](func: GenericFunc[N]): Option[UnaryFunc] =
      some(func) collect {
        case structural.FlattenArray => structural.FlattenArray
        case structural.FlattenArrayIndices => structural.FlattenArrayIndices
        case structural.ShiftArray => structural.ShiftArray
        case structural.ShiftArrayIndices => structural.ShiftArrayIndices
      }
  }

  object MapInflation {
    def unapply[N <: Nat](func: GenericFunc[N]): Option[UnaryFunc] =
      some(func) collect {
        case structural.FlattenMap => structural.FlattenMap
        case structural.FlattenMapKeys => structural.FlattenMapKeys
        case structural.ShiftMap => structural.ShiftMap
        case structural.ShiftMapKeys => structural.ShiftMapKeys
      }
  }

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
    rename(κ(freshSym[State[Long, ?]]("tmp")))(t).evalZero[Long]

  def bindFree(vars: Map[CIName, T])(t: T): T =
    t.cata[T] {
      case Free(sym) => vars.get(CIName(sym.name)).getOrElse((Free(sym):LP[T]).embed)
      case other     => other.embed
    }

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
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
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

    // NB: avoids illegally rewriting the continuation
    case InvokeUnapply(relations.Cond, Sized(a1, a2, a3)) => (a1, a2, a3) match {
      case (Embed(Let(a, x1, x2)), a2, a3) =>
        lp.let(a, x1, invoke[nat._3](relations.Cond, Func.Input3(x2, a2, a3))).some
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

    case Join(l, r, tpe, cond) =>
      (l, r) match {
        case (Embed(Let(a, x1, x2)), r) =>
          lp.let(a, x1, join(x2, r, tpe, cond)).some
        case (l, Embed(Let(a, x1, x2))) =>
          lp.let(a, x1, join(l, x2, tpe, cond)).some
        case _ => None
      }

    // we don't rewrite a `Let` as the `cont` to avoid illegally rewriting the continuation
    case Typecheck(Embed(Let(a, x1, x2)), typ, cont, fallback) =>
      lp.let(a, x1, typecheck(x2, typ, cont, fallback)).some
    case Typecheck(expr, typ, cont, Embed(Let(a, x1, x2))) =>
      lp.let(a, x1, typecheck(expr, typ, cont, x2)).some

    case t => None
  }

  def normalizeLets(t: T) = t.transAna[T](repeatedly(normalizeLetsƒ))

  type Typed[F[_]] = Cofree[F, Type]

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def inferTypes(typ: Type, term: T): ValidationNel[ArgumentError, Typed[LP]] = {
    (term.project match {
      case Read(c) => success(lp.read[Typed[LP]](c))

      case Constant(d) => success(lp.constant[Typed[LP]](d))

      case Invoke(func, args) =>
        for {
          types <- func.untpe(typ)
          args0 <- types.zip(args).traverse {
            case (t, arg) => inferTypes(t, arg)
          }
        } yield lp.invoke(func, args0)

      case JoinSideName(n) => success(lp.joinSideName[Typed[LP]](n))

      case Join(l, r, joinType, JoinCondition(lName, rName, cond)) =>
        inferTypes(Type.Bool, cond).flatMap { cond0 =>
          // FIXME: single pass for both names
          val lTyp = cond0.collect {
            case Cofree(typ0, JoinSideName(`lName`)) => typ0
          }.concatenate(Type.TypeGlbMonoid)

          val rTyp = cond0.collect {
            case Cofree(typ0, JoinSideName(`rName`)) => typ0
          }.concatenate(Type.TypeGlbMonoid)

          (inferTypes(Type.glb(lTyp, typ), l) |@| inferTypes(Type.glb(rTyp, typ), r))(
            lp.join(_, _, joinType, JoinCondition(lName, rName, cond0)))
        }

      case Free(n) => success(lp.free[Typed[LP]](n))

      case Let(n, form, in) =>
        inferTypes(typ, in).flatMap { in0 =>
          val formTyp = in0.collect {
            case Cofree(typ0, Free(n0)) if n0 == n => typ0
          }.concatenate(Type.TypeGlbMonoid)
          inferTypes(formTyp, form).map(lp.let[Typed[LP]](n, _, in0))
        }

      case Sort(src, ords) =>
        (inferTypes(typ, src) ⊛ ords.traverse {
          case (a, d) => inferTypes(Type.Top, a) strengthR d
        })(lp.sort[Typed[LP]](_, _))

      case TemporalTrunc(part, src) =>
        typ.contains(Type.Temporal).fold(
          inferTypes(Type.Temporal, src) ∘ (lp.temporalTrunc[Typed[LP]](part, _)),
          failureNel(ArgumentError.typeError(UnificationError(Type.Temporal, typ, none))))

      case Typecheck(expr, t, cont, fallback) =>
        (inferTypes(t, expr) ⊛ inferTypes(typ, cont) ⊛ inferTypes(typ, fallback))(
          lp.typecheck[Typed[LP]](_, t, _, _))

    }).map(Cofree(typ, _))
  }

  def constrain(inf: Type, poss: Type): Option[Type] = {
    @inline def coproductMembers(ty: Type): Option[NonEmptyList[Type]] = ty match {
      case tc: Type.Coproduct => Some(tc.flatten)
      case _                  => None
    }
    (coproductMembers(inf) |@| coproductMembers(poss)) { (is, ps) =>
      is.list.toList.intersect(ps.list.toList) match {
        case (x :: xs) => Some(xs.foldLeft(x)(Type.Coproduct(_, _)))
        case Nil => None
      }
    }.flatten.orElse(Some(inf).filter(poss.contains))
  }

  /** This function compares the inferred (required) type with the possible type
    * from the collection.
    * • if it’s a const type, replace the node with a constant
    * • if the possible is a subtype of the inferred, we’re good
    * • if the inferred is a subtype of the possible, we need a runtime check
    * • otherwise, we fail
    */
  private def unifyOrCheck[F[_]: Monad: MonadArgumentErrs: NameGenerator](
      inf: Type,
      poss: Type,
      term: T)
      : F[ConstrainedPlan[T]] =
    if (inf.contains(poss)) {
      ConstrainedPlan(poss, Nil, poss match {
        case Type.Const(d) => constant(d)
        case _             => term
      }).point[F]
    } else {
      constrain(inf, poss).fold(
        MonadArgumentErrs[F].raiseError[ConstrainedPlan[T]](
          ArgumentError.typeError(UnificationError(inf, poss, None)).wrapNel)
      )(constraint =>
        freshSym[F]("checku").map(name =>
          ConstrainedPlan(inf, List(NamedConstraint(name, constraint, term)), free(name))))
    }

  private def appConst(constraints: ConstrainedPlan[T], fallback: T) =
    constraints.constraints.foldLeft(constraints.plan) { (acc, con) =>
      val body = typecheck(free(con.name), con.inferred, acc, fallback)
      let(con.name, con.term, body)
    }

  /** This inserts a constraint on a node that might not strictly require a type
    * check. It protects operations (EG, array flattening) that need a certain
    * shape.
    */
  private def ensureConstraint[F[_]: Applicative: NameGenerator](
      constraints: ConstrainedPlan[T],
      fallback: T)
      : F[T] = {

    val named = constraints match {
      case ConstrainedPlan(typ, Nil, term) =>
        freshSym[F]("checke") map { name =>
          ConstrainedPlan(typ, List(NamedConstraint(name, typ, term)), free(name))
        }

      case otherwise => otherwise.point[F]
    }

    named map (appConst(_, fallback))
  }

  // TODO: This can perhaps be decomposed into separate folds for annotating
  //       with “found” types, folding constants, and adding runtime checks.
  // FIXME: No exhaustiveness checking here
  def checkTypesƒ[F[_]: Monad: MonadArgumentErrs: NameGenerator]
      : ((Type, LP[ConstrainedPlan[T]])) => F[ConstrainedPlan[T]] = {
    case (inf, term) =>
      def applyConstraints(poss: Type, constraints: ConstrainedPlan[T])(f: T => T) =
        unifyOrCheck[F](inf, poss, f(appConst(constraints, constant(Data.NA))))

      term match {
        case Read(c) =>
          unifyOrCheck[F](inf, Type.Top, read(c))

        case Constant(d) =>
          unifyOrCheck[F](inf, Type.Const(d), constant(d))

        case InvokeUnapply(MakeMap, Sized(name, value)) =>
          unattemptV[F](MakeMap.tpe(Func.Input2(name, value).map(_.inferred)))
            .flatMap(applyConstraints(_, value)(MakeMap(name.plan, _).embed))

        case InvokeUnapply(MakeArray, Sized(value)) =>
          unattemptV[F](MakeArray.tpe(Func.Input1(value).map(_.inferred)))
            .flatMap(applyConstraints(_, value)(MakeArray(_).embed))

        // TODO: Move this case to the Mongo planner once type information is
        //       available there.
        case InvokeUnapply(ConcatOp, Sized(left, right)) =>
          val (types, constraints0, terms) = Func.Input2(left, right).map {
            case ConstrainedPlan(in, con, pl) => (in, (con, pl))
          }.unzip3[Type, List[NamedConstraint[T]], T]

          val constraints = constraints0.unsized.flatten

          unattemptV[F](ConcatOp.tpe(types)).flatMap({
            case t if Type.Str.contains(t) =>
              unifyOrCheck[F](inf, t, invoke(string.Concat, terms))

            case t if t.arrayLike =>
              unifyOrCheck[F](inf, t, invoke(ArrayConcat, terms))

            case _ =>
              MonadArgumentErrs[F].raiseError[ConstrainedPlan[T]](
                ArgumentError.invalidArgumentError("can't concat mixed/unknown types").wrapNel)
          }).map(cp => cp.copy(constraints = cp.constraints ++ constraints))

        case InvokeUnapply(relations.Or, Sized(left, right)) =>
          unattemptV[F](relations.Or.tpe(Func.Input2(left, right).map(_.inferred)))
            .flatMap(unifyOrCheck[F](inf, _, relations.Or(left, right).map(appConst(_, constant(Data.NA))).embed))

        case InvokeUnapply(ArrayInflation(f), Sized(arg)) =>
          for {
            types <- unattemptV[F](f.tpe(Func.Input1(arg).map(_.inferred)))
            consts <- Func.Input1(arg).traverse(ensureConstraint[F](_, constant(Data.Arr(List(Data.NA)))))
            plan  <- unifyOrCheck[F](inf, types, invoke[nat._1](f, consts))
          } yield plan

        case InvokeUnapply(MapInflation(f), Sized(arg)) =>
          for {
            types <- unattemptV[F](f.tpe(Func.Input1(arg).map(_.inferred)))
            consts <- Func.Input1(arg).traverse(ensureConstraint[F](_, constant(Data.Obj(ListMap("" -> Data.NA)))))
            plan  <- unifyOrCheck[F](inf, types, invoke(f, consts))
          } yield plan

        case InvokeUnapply(relations.IfUndefined, Sized(condition, fallback)) =>
          val args = Func.Input2(condition, fallback)
          val constructLPNode = invoke(relations.IfUndefined, _: Func.Input[T, nat._2])
          unattemptV[F](relations.IfUndefined.tpe(args.map(_.inferred)))
            .flatMap(unifyOrCheck[F](inf, _, constructLPNode(args.map(appConst(_, constant(Data.NA))))))

        case InvokeUnapply(func @ NullaryFunc(_, _, _, _), Sized()) =>
          checkGenericInvoke[F](inf, func, Sized[List]())

        case InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1)) =>
          checkGenericInvoke[F](inf, func, Func.Input1(a1))

        case InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2)) =>
          checkGenericInvoke[F](inf, func, Func.Input2(a1, a2))

        case InvokeUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3)) =>
          checkGenericInvoke[F](inf, func, Func.Input3(a1, a2, a3))

        case Typecheck(expr, typ, cont, fallback) =>
          val typer: Func.Domain[Nat._3] => Func.VCodomain = {
            case Sized(_, t2, _) => Type.glb(t2, typ).success
          }

          val construct: Func.Input[T, Nat._3] => T = {
            case Sized(a1, a2, a3) => typecheck(a1, typ, a2, a3)
          }

          val (constrs, plan): (List[NamedConstraint[T]], T) = expr.constraints.foldLeftM(expr.plan) {
            case (acc, constr) =>
              if ((Free(constr.name) == expr.plan) && (constr.inferred == typ))
                (Nil, constr.term)
              else
                (List(constr), expr.plan)
          }

          val expr0 = ConstrainedPlan(expr.inferred, constrs, plan)
          checkInvoke[F, Nat._3](inf, typer, construct, Func.Input3(expr0, cont, fallback))

        case Let(name, value, in) =>
          unifyOrCheck[F](inf, in.inferred, let(name, appConst(value, constant(Data.NA)), appConst(in, constant(Data.NA))))

        case JoinSideName(v) =>
          ConstrainedPlan(inf, Nil, joinSideName(v)).point[F]

        case Join(l, r, tpe, JoinCondition(lName, rName, c)) =>
          checkJoin[F](inf, Func.Input3(l, r, c), lName, rName, tpe)

        // TODO: Get the possible type from the LetF
        case Free(v) =>
          ConstrainedPlan(inf, Nil, free(v)).point[F]

        case Sort(expr, ords) =>
          unifyOrCheck[F](inf, expr.inferred, sort(appConst(expr, constant(Data.NA)), ords map (_ leftMap (appConst(_, constant(Data.NA))))))

        case TemporalTrunc(part, src) =>
          import DataDateTimeExtractors._
          val typer: Func.Domain[nat._1] => Func.VCodomain = {
            case Sized(Type.Const(CanLensDateTime(s))) => Type.Const(s.peeks(time.truncDateTime(part, _))).success
            case Sized(Type.Const(CanLensTime(s))) => Type.Const(s.peeks(time.truncTime(part, _))).success
            case Sized(Type.Const(CanLensDate(s))) => Type.Const(s.peeks(time.truncDate(part, _))).success
            case Sized(t) => t.success
          }

          val constructLPNode: Func.Input[T, Nat._1] => T = { case Sized(i) => temporalTrunc(part, i) }

          checkInvoke[F, Nat._1](inf, typer, constructLPNode, Func.Input1(src))
      }
  }

  private object unattemptV {
    def apply[F[_]] = new PartiallyApplied[F]
    final class PartiallyApplied[F[_]] {
      def apply[A](vnel: ValidationNel[ArgumentError, A])(
          implicit F0: Applicative[F], F1: MonadArgumentErrs[F])
          : F[A] =
        vnel.fold(F1.raiseError(_), F0.point(_))
    }
  }

  private def checkInvoke[F[_]: Monad: MonadArgumentErrs: NameGenerator, N <: Nat](
      inf: Type,
      typer: Func.Domain[N] => Func.VCodomain,
      constructLPNode: Func.Input[T, N] => T,
      args: Func.Input[ConstrainedPlan[T], N])
      : F[ConstrainedPlan[T]] = {

    val (types, constraints0, terms) = args.map {
      case ConstrainedPlan(in, con, pl) => (in, (con, pl))
    }.unzip3[Type, List[NamedConstraint[T]], T]

    val constraints = constraints0.unsized.flatten

    unattemptV[F](typer(types))
      .flatMap(unifyOrCheck[F](inf, _, constructLPNode(terms)))
      .map(cp => cp.copy(constraints = cp.constraints ++ constraints))
  }

  private object checkGenericInvoke {
    def apply[F[_]] = new PartiallyApplied[F]
    final class PartiallyApplied[F[_]] {
      def apply[N <: Nat](
          inf: Type,
          func: GenericFunc[N],
          args: Func.Input[ConstrainedPlan[T], N])(
          implicit
          F0: Monad[F],
          F1: MonadArgumentErrs[F],
          F2: NameGenerator[F])
          : F[ConstrainedPlan[T]] = {

        val constructLPNode = invoke(func, _: Func.Input[T, N])

        func.effect match {
          case Mapping =>
            checkInvoke[F, N](inf, func.tpe, constructLPNode, args)

          case _ =>
            unattemptV[F](func.tpe(args.map(_.inferred)))
              .flatMap(unifyOrCheck[F](inf, _, constructLPNode(args.map(appConst(_, constant(Data.NA))))))
        }
      }
    }
  }

  private def checkJoin[F[_]: Monad: MonadArgumentErrs: NameGenerator](
      inf: Type,
      args: Func.Input[ConstrainedPlan[T], nat._3],
      lName: Symbol,
      rName: Symbol,
      tpe: JoinType)
      : F[ConstrainedPlan[T]] = {

    val const = args.map(appConst(_, constant(Data.NA)))

    unattemptV[F](set.joinTyper(tpe)(args.map(_.inferred)).getOrElse(success(Type.Top)))
      .flatMap(unifyOrCheck[F](inf, _, join(const(0), const(1), tpe, JoinCondition(lName, rName, const(2)))))
  }

  def ensureCorrectTypes[F[_]: Monad: MonadArgumentErrs: PhaseResultTell](term: T): F[T] = {
    type G[A] = StateT[F, Long, A]

    val runEnvT = (_: EnvT[Type, LogicalPlan, ConstrainedPlan[T]]).run

    // Firstly we infer the types in the LogicalPlan. We start with the assumption
    // that the entire thing returns `Type.Top`, then we feed that into the outermost untyper,
    // asking "if this expression returns `Type.Top`, what are the types of the arguments?"
    // We then continue in a top-down fashion, calling untypers exclusively to infer all of
    // the types. This process is (partially) documented in InferTypesSpec
    // Only the type *checker* calls the typers, because untypers always have the option to
    //"give up" and return a fixed input type regardless of the expected output.
    // In several other areas where we have no type information, we feed in `Type.Top` as well.

    // Secondly we check the types. Type checking means calling typers in a bottom-up
    // fashion, to verify that all of the argument types returned by untypers actually correspond
    // to the result types returned by untypers. This includes propagation of constant types.

    // Note that this means there is *no reason* to return a constant type from an untyper.
    // Not only do they not exist yet, because `Type.Top` is the starting assumption of
    // type inference, but even if they did, they could only propagate backwards.
    // Elaborating, there is no way to work back from the knowledge that a function returns
    // a constant to the knowledge that the function's parameters are constants, because
    // there is no way to know a function returns a constant unless you already know
    // the parameters are constants!
    val ensured = for {
      typed   <- unattemptV[G](inferTypes(Type.Top, term))
      _       <- phase[G]("Inferred Types", typed)
      checked <- typed.cataM(checkTypesƒ[G] <<< runEnvT)
    } yield appConst(checked, constant(Data.NA))

    ensured.evalZero[Long]
  }

  /** The set of paths referenced in the given plan. */
  def paths(lp: T): ISet[FPath] =
    lp.foldMap(_.cata[ISet[FPath]] {
      case Read(p) => ISet singleton p
      case other   => other.fold
    })

  /** The set of absolute paths referenced in the given plan. */
  def absolutePaths(lp: T): ISet[APath] =
    paths(lp) foldMap (p => ISet fromFoldable refineTypeAbs(p).swap)
}
