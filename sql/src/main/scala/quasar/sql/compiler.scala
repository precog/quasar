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

package quasar.sql

import quasar.Predef._
import quasar.{BinaryFunc, Data, Func, GenericFunc, Reduction, SemanticError, Sifting, TernaryFunc, UnaryFunc, VarName},
  SemanticError._
import quasar.contrib.pathy._
import quasar.contrib.shapeless._
import quasar.fp._
import quasar.fp.ski._
import quasar.fp.binder._
import quasar.logicalplan.{LogicalPlan => LP, _}
import quasar.std.StdLib, StdLib._
import quasar.sql.{SemanticAnalysis => SA}, SA._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import pathy.Path._
import scalaz.{Tree => _, _}, Scalaz._
import shapeless.{Annotations => _, Data => _, :: => _, _}

trait Compiler[F[_]] {
  import identity._
  import JoinDir._

  val lpr = new LogicalPlanR[Fix]

  // HELPERS
  private type M[A] = EitherT[F, SemanticError, A]

  private type CompilerStateT[F[_],A] = StateT[F, CompilerState, A]
  private type CompilerM[A] = CompilerStateT[M, A]

  private def syntheticOf(node: CoExpr): List[Option[Synthetic]] =
    node.head._1

  private def provenanceOf(node: CoExpr): Provenance =
    node.head._2

  private final case class TableContext(
    root: Option[Fix[LP]],
    full: () => Fix[LP],
    subtables: Map[String, Fix[LP]]) {
    def ++(that: TableContext): TableContext =
      TableContext(
        None,
        () => Fix(structural.ObjectConcat(this.full(), that.full())),
        this.subtables ++ that.subtables)
  }

  private final case class BindingContext(
    subbindings: Map[String, Fix[LP]]) {
    def ++(that: BindingContext): BindingContext =
      BindingContext(this.subbindings ++ that.subbindings)
  }

  private final case class Context(
    bindingContext: List[BindingContext],
    tableContext: List[TableContext]) {

    def add(bc: BindingContext, tc: TableContext): Context = {
      val modBindingContext: List[BindingContext] =
        this.bindingContext match {
          case head :: tail => head ++ bc :: head :: tail
          case Nil => bc :: Nil
        }

      val modTableContext: List[TableContext] =
        tc :: this.tableContext

      Context(modBindingContext, modTableContext)
    }

    def dropHead: Context =
      Context(this.bindingContext.drop(1), this.tableContext.drop(1))
  }

  private final case class CompilerState(
    fields: List[String],
    context: Context,
    nameGen: Int)

  private object CompilerState {

    /** Runs a computation inside a binding/table context, which contains
      * compilation data for the bindings/tables in scope.
      */
    def contextual[A](bc: BindingContext, tc: TableContext)(
      compM: CompilerM[A])(
      implicit m: Monad[F]): CompilerM[A] = {

      def preMod: CompilerState => CompilerState =
        (state: CompilerState) => state.copy(context = state.context.add(bc, tc))

      def postMod: CompilerState => CompilerState =
        (state: CompilerState) => state.copy(context = state.context.dropHead)

      mod(preMod) *> compM <* mod(postMod)
    }

    def addFields[A](add: List[String])(f: CompilerM[A])(implicit m: Monad[F]):
        CompilerM[A] =
      for {
        curr <- read[CompilerState, List[String]](_.fields)
        _    <- mod((s: CompilerState) => s.copy(fields = curr ++ add))
        a    <- f
      } yield a

    def fields(implicit m: Monad[F]): CompilerM[List[String]] =
      read[CompilerState, List[String]](_.fields)

    def rootTable(implicit m: Monad[F]): CompilerM[Option[Fix[LP]]] =
      read[CompilerState, Option[Fix[LP]]](_.context.tableContext.headOption.flatMap(_.root))

    def rootTableReq(implicit m: Monad[F]): CompilerM[Fix[LP]] =
      rootTable.flatMap(_.map(emit).getOrElse(fail(CompiledTableMissing)))

    // prioritize binding context - when we want to prioritize a table,
    // we will have the table reference already in the binding context
    def subtable(name: String)(implicit m: Monad[F]):
        CompilerM[Option[Fix[LP]]] =
      read[CompilerState, Option[Fix[LP]]]{ state =>
        state.context.bindingContext.headOption.flatMap { bc =>
          bc.subbindings.get(name) match {
            case None =>
              state.context.tableContext.headOption.flatMap(_.subtables.get(name))
            case s => s
          }
        }
      }

    def subtableReq(name: String)(implicit m: Monad[F]):
        CompilerM[Fix[LP]] =
      subtable(name).flatMap(
        _.map(emit).getOrElse(fail(CompiledSubtableMissing(name))))

    def fullTable(implicit m: Monad[F]): CompilerM[Option[Fix[LP]]] =
      read[CompilerState, Option[Fix[LP]]](_.context.tableContext.headOption.map(_.full()))

    /** Generates a fresh name for use as an identifier, e.g. tmp321. */
    def freshName(prefix: String)(implicit m: Monad[F]): CompilerM[Symbol] =
      read[CompilerState, Int](_.nameGen).map(n => Symbol(prefix + n.toString)) <*
        mod((s: CompilerState) => s.copy(nameGen = s.nameGen + 1))
  }

  private def read[A, B](f: A => B)(implicit m: Monad[F]):
      StateT[M, A, B] =
    StateT((s: A) => (s, f(s)).point[M])

  private def fail[A](error: SemanticError)(implicit m: Monad[F]):
      CompilerM[A] =
    lift(error.left)

  private def emit[A](value: A)(implicit m: Monad[F]): CompilerM[A] =
    value.point[CompilerM]

  private def lift[A](v: SemanticError \/ A)(implicit m: Monad[F]):
      CompilerM[A] =
    StateT[M, CompilerState, A]((s: CompilerState) =>
      EitherT.eitherT(v.map(s -> _).point[F]))

  private def whatif[S, A](f: StateT[M, S, A])(implicit m: Monad[F]):
      StateT[M, S, A] =
    read((s: S) => s).flatMap(oldState => f.imap(κ(oldState)))

  private def mod(f: CompilerState => CompilerState)(implicit m: Monad[F]):
      CompilerM[Unit] =
    StateT[M, CompilerState, Unit](s => (f(s), ()).point[M])

  type CoAnn[F[_]] = Cofree[F, SA.Annotations]
  type CoExpr = CoAnn[Sql]

  // CORE COMPILER
  private def compile0(node: CoExpr)(implicit M: Monad[F]):
      CompilerM[Fix[LP]] = {

    val functionMapping = Map[String, GenericFunc[_]](
      "count"                   -> agg.Count,
      "sum"                     -> agg.Sum,
      "min"                     -> agg.Min,
      "max"                     -> agg.Max,
      "avg"                     -> agg.Avg,
      "arbitrary"               -> agg.Arbitrary,
      "array_length"            -> array.ArrayLength,
      "extract_century"         -> date.ExtractCentury,
      "extract_day_of_month"    -> date.ExtractDayOfMonth,
      "extract_decade"          -> date.ExtractDecade,
      "extract_day_of_week"     -> date.ExtractDayOfWeek,
      "extract_day_of_year"     -> date.ExtractDayOfYear,
      "extract_epoch"           -> date.ExtractEpoch,
      "extract_hour"            -> date.ExtractHour,
      "extract_iso_day_of_week" -> date.ExtractIsoDayOfWeek,
      "extract_iso_year"        -> date.ExtractIsoYear,
      "extract_microseconds"    -> date.ExtractMicroseconds,
      "extract_millennium"      -> date.ExtractMillennium,
      "extract_milliseconds"    -> date.ExtractMilliseconds,
      "extract_minute"          -> date.ExtractMinute,
      "extract_month"           -> date.ExtractMonth,
      "extract_quarter"         -> date.ExtractQuarter,
      "extract_second"          -> date.ExtractSecond,
      "extract_timezone"        -> date.ExtractTimezone,
      "extract_timezone_hour"   -> date.ExtractTimezoneHour,
      "extract_timezone_minute" -> date.ExtractTimezoneMinute,
      "extract_week"            -> date.ExtractWeek,
      "extract_year"            -> date.ExtractYear,
      "date"                    -> date.Date,
      "time"                    -> date.Time,
      "timestamp"               -> date.Timestamp,
      "interval"                -> date.Interval,
      "time_of_day"             -> date.TimeOfDay,
      "to_timestamp"            -> date.ToTimestamp,
      "squash"                  -> identity.Squash,
      "oid"                     -> identity.ToId,
      "between"                 -> relations.Between,
      "where"                   -> set.Filter,
      "distinct"                -> set.Distinct,
      "within"                  -> set.Within,
      "constantly"              -> set.Constantly,
      "concat"                  -> string.Concat,
      "like"                    -> string.Like,
      "search"                  -> string.Search,
      "length"                  -> string.Length,
      "lower"                   -> string.Lower,
      "upper"                   -> string.Upper,
      "substring"               -> string.Substring,
      "boolean"                 -> string.Boolean,
      "integer"                 -> string.Integer,
      "decimal"                 -> string.Decimal,
      "null"                    -> string.Null,
      "to_string"               -> string.ToString,
      "make_object"             -> structural.MakeObject,
      "make_array"              -> structural.MakeArray,
      "object_concat"           -> structural.ObjectConcat,
      "array_concat"            -> structural.ArrayConcat,
      "delete_field"            -> structural.DeleteField,
      "flatten_map"             -> structural.FlattenMap,
      "flatten_array"           -> structural.FlattenArray,
      "shift_map"               -> structural.ShiftMap,
      "shift_array"             -> structural.ShiftArray)

    def compileCases(cases: List[Case[CoExpr]], default: Fix[LP])(f: Case[CoExpr] => CompilerM[(Fix[LP], Fix[LP])]) =
      cases.traverse(f).map(_.foldRight(default) {
        case ((cond, expr), default) => Fix(relations.Cond(cond, expr, default))
      })

    def flattenJoins(term: Fix[LP], relations: SqlRelation[CoExpr]):
        Fix[LP] = relations match {
      case _: NamedRelation[_]             => term
      case JoinRelation(left, right, _, _) =>
        Fix(structural.ObjectConcat(
          flattenJoins(Left.projectFrom(term), left),
          flattenJoins(Right.projectFrom(term), right)))
    }

    def buildJoinDirectionMap(relations: SqlRelation[CoExpr]):
        Map[String, List[JoinDir]] = {
      def loop(rel: SqlRelation[CoExpr], acc: List[JoinDir]):
          Map[String, List[JoinDir]] = rel match {
        case t: NamedRelation[_] => Map(t.aliasName -> acc)
        case JoinRelation(left, right, tpe, clause) =>
          loop(left, Left :: acc) ++ loop(right, Right :: acc)
      }

      loop(relations, Nil)
    }

    def compileTableRefs(joined: Fix[LP], relations: SqlRelation[CoExpr]):
        Map[String, Fix[LP]] =
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldRight(
            joined)(
            (dir, acc) => dir.projectFrom(acc))
      }

    def tableContext(joined: Fix[LP], relations: SqlRelation[CoExpr]):
        TableContext =
      TableContext(
        Some(joined),
        () => flattenJoins(joined, relations),
        compileTableRefs(joined, relations))

    def step(relations: SqlRelation[CoExpr]):
        (Option[CompilerM[Fix[LP]]] =>
          CompilerM[Fix[LP]] =>
          CompilerM[Fix[LP]]) = {
      (current: Option[CompilerM[Fix[LP]]]) =>
      (next: CompilerM[Fix[LP]]) =>
      current.map { current =>
        for {
          stepName <- CompilerState.freshName("tmp")
          current  <- current
          bc        = relations match {
            case ExprRelationAST(_, name)        => BindingContext(Map(name -> lpr.free(stepName)))
            case TableRelationAST(_, Some(name)) => BindingContext(Map(name -> lpr.free(stepName)))
            case id @ IdentRelationAST(_, _)     => BindingContext(Map(id.aliasName -> lpr.free(stepName)))
            case r                               => BindingContext(Map())
          }
          next2    <- CompilerState.contextual(bc, tableContext(lpr.free(stepName), relations))(next)
        } yield lpr.let(stepName, current, next2)
      }.getOrElse(next)
    }

    def relationName(node: CoExpr): SemanticError \/ String = {
      val namedRel = provenanceOf(node).namedRelations
      val relations =
        if (namedRel.size <= 1) namedRel
        else {
          val filtered = namedRel.filter(x => x._1 ≟ pprint(node.convertTo[Fix]))
          if (filtered.isEmpty) namedRel else filtered
        }
      relations.toList match {
        case Nil             => -\/ (NoTableDefined(node.convertTo[Fix]))
        case List((name, _)) =>  \/-(name)
        case x               => -\/ (AmbiguousReference(node.convertTo[Fix], x.map(_._2).join))
      }
    }

    def compileFunction[N <: Nat](func: GenericFunc[N], args: Func.Input[CoExpr, N]):
        CompilerM[Fix[LP]] =
      args.traverse(compile0).map(args => Fix(func.applyGeneric(args)))

    def buildRecord(names: List[Option[String]], values: List[Fix[LP]]):
        Fix[LP] = {
      val fields = names.zip(values).map {
        case (Some(name), value) =>
          Fix(structural.MakeObject(lpr.constant(Data.Str(name)), value))
        case (None, value) => value
      }

      fields.reduceOption((a,b) => Fix(structural.ObjectConcat(a, b)))
        .getOrElse(lpr.constant(Data.Obj()))
    }

    def compileRelation(r: SqlRelation[CoExpr]): CompilerM[Fix[LP]] =
      r match {
        case IdentRelationAST(name, _) =>
          CompilerState.subtableReq(name)

        case VariRelationAST(vari, _) =>
          fail(UnboundVariable(VarName(vari.symbol)))

        case TableRelationAST(path, _) =>
          sandboxCurrent(canonicalize(path)).cata(
            p => emit(lpr.read(p)),
            fail(InvalidPathError(path, None)))

        case ExprRelationAST(expr, _) => compile0(expr)

        case JoinRelation(left, right, tpe, clause) =>
          (CompilerState.freshName("left") ⊛ CompilerState.freshName("right"))((leftName, rightName) => {
            val leftFree: Fix[LP] = lpr.free(leftName)
            val rightFree: Fix[LP] = lpr.free(rightName)

            (compileRelation(left) ⊛
              compileRelation(right) ⊛
              CompilerState.contextual(
                BindingContext(Map()),
                tableContext(leftFree, left) ++ tableContext(rightFree, right))(
                compile0(clause).map(c =>
                  lpr.invoke(
                    tpe match {
                      case LeftJoin             => set.LeftOuterJoin
                      case quasar.sql.InnerJoin => set.InnerJoin
                      case RightJoin            => set.RightOuterJoin
                      case FullJoin             => set.FullOuterJoin
                    },
                    Func.Input3(leftFree, rightFree, c)))))((left0, right0, join) =>
              lpr.let(leftName, left0,
                lpr.let(rightName, right0, join)))
            }).join
      }

    node.tail match {
      case s @ Select(isDistinct, projections, relations, filter, groupBy, orderBy) =>
        /* 1. Joins, crosses, subselects (FROM)
         * 2. Filter (WHERE)
         * 3. Group by (GROUP BY)
         * 4. Filter (HAVING)
         * 5. Select (SELECT)
         * 6. Squash
         * 7. Sort (ORDER BY)
         * 8. Distinct (DISTINCT/DISTINCT BY)
         * 9. Prune synthetic fields
         */

        // Selection of wildcards aren't named, we merge them into any other
        // objects created from other columns:
        val namesOrError: SemanticError \/ List[Option[String]] =
          projectionNames[CoAnn](projections, relationName(node).toOption).map(_.map {
            case (name, Embed(expr)) => expr match {
              case Splice(_) => None
              case _         => name.some
            }
          })

        namesOrError.fold(
          err => EitherT.left[F, SemanticError, Fix[LP]](err.point[F]).liftM[CompilerStateT],
          names => {

            val projs = projections.map(_.expr)

            val syntheticNames: List[String] =
              names.zip(syntheticOf(node)).flatMap {
                case (Some(name), Some(_)) => List(name)
                case (_, _) => Nil
              }

            relations.foldRight(
              projs.traverse(compile0).map(buildRecord(names, _)))(
              (relations, select) => {
                val stepBuilder = step(relations)
                stepBuilder(compileRelation(relations).some) {
                  val filtered = filter.map(filter =>
                    (CompilerState.rootTableReq ⊛ compile0(filter))(
                      set.Filter(_, _).embed))

                  stepBuilder(filtered) {
                    val grouped = groupBy.map(groupBy =>
                      (CompilerState.rootTableReq ⊛
                        groupBy.keys.traverse(compile0)) ((src, keys) =>
                        Fix(set.GroupBy(src, Fix(structural.MakeArrayN(keys: _*))))))

                    stepBuilder(grouped) {
                      val having = groupBy.flatMap(_.having).map(having =>
                        (CompilerState.rootTableReq ⊛ compile0(having))(
                          set.Filter(_, _).embed))

                      stepBuilder(having) {
                        val squashed = select.map(set => Fix(Squash(set)))

                        stepBuilder(squashed.some) {
                          val sort = orderBy.map(orderBy =>
                            (CompilerState.rootTableReq ⊛
                              CompilerState.addFields(names.foldMap(_.toList))(orderBy.keys.traverse { case (_, key) => compile0(key) }))((t, keys) =>
                              Fix(set.OrderBy(
                                t,
                                Fix(structural.MakeArrayN(keys: _*)),
                                Fix(structural.MakeArrayN(orderBy.keys.map { case (order, _) => lpr.constant(Data.Str(order.shows)) }: _*))))))

                          stepBuilder(sort) {
                            val distincted = isDistinct match {
                              case SelectDistinct =>
                                CompilerState.rootTableReq.map(t =>
                                  if (syntheticNames.nonEmpty)
                                    Fix(set.DistinctBy(t, syntheticNames.foldLeft(t)((acc, field) =>
                                      Fix(structural.DeleteField(acc, lpr.constant(Data.Str(field)))))))
                                  else Fix(set.Distinct(t))).some
                              case _ => None
                            }

                            stepBuilder(distincted) {
                              val pruned =
                                CompilerState.rootTableReq.map(
                                  syntheticNames.foldLeft(_)((acc, field) =>
                                    Fix(structural.DeleteField(acc,
                                      lpr.constant(Data.Str(field))))))

                              pruned
                            }
                          }
                        }
                      }
                    }
                  }
                }
              })
          })

      case Let(name, form, body) => {
        val rel = ExprRelationAST(form, name)
        step(rel)(compile0(form).some)(compile0(body))
      }

      case SetLiteral(values0) =>
        values0.traverse(compile0).map(vs =>
          structural.ShiftArray(structural.MakeArrayN(vs: _*).embed).embed)

      case ArrayLiteral(exprs) =>
        exprs.traverse(compile0).map(elems => Fix(structural.MakeArrayN(elems: _*)))

      case MapLiteral(exprs) =>
        exprs.traverse(_.bitraverse(compile0, compile0)).map(elems =>
          Fix(structural.MakeObjectN(elems: _*)))

      case Splice(expr) =>
        expr.fold(
          CompilerState.fullTable.flatMap(_.map(emit _).getOrElse(fail(GenericError("Not within a table context so could not find table expression for wildcard")))))(
          compile0)

      case Binop(left, right, op) =>
        ((op match {
          case IfUndefined   => relations.IfUndefined.left
          case Range         => set.Range.left
          case Or            => relations.Or.left
          case And           => relations.And.left
          case Eq            => relations.Eq.left
          case Neq           => relations.Neq.left
          case Ge            => relations.Gte.left
          case Gt            => relations.Gt.left
          case Le            => relations.Lte.left
          case Lt            => relations.Lt.left
          case Concat        => structural.ConcatOp.left
          case Plus          => math.Add.left
          case Minus         => math.Subtract.left
          case Mult          => math.Multiply.left
          case Div           => math.Divide.left
          case Mod           => math.Modulo.left
          case Pow           => math.Power.left
          case In            => set.In.left
          case FieldDeref    => structural.ObjectProject.left
          case IndexDeref    => structural.ArrayProject.left
          case Limit         => set.Take.left
          case Offset        => set.Drop.left
          case UnshiftMap    => structural.UnshiftMap.left
          case Except        => set.Except.left
          case UnionAll      => set.Union.left
          case IntersectAll  => set.Intersect.left
          // TODO: These two cases are eliminated by `normalizeƒ` and would be
          //       better represented in a Coproduct.
          case f @ Union     => fail(FunctionNotFound(f.name)).right
          case f @ Intersect => fail(FunctionNotFound(f.name)).right
        }): GenericFunc[nat._2] \/ CompilerM[Fix[LP]])
          .valueOr(compileFunction[nat._2](_, Func.Input2(left, right)))

      case Unop(expr, op) =>
        ((op match {
          case Not                 => relations.Not.left
          case f @ Exists          => fail(FunctionNotFound(f.name)).right
          // TODO: NOP, but should we ensure we have a Num or Interval here?
          case Positive            => compile0(expr).right
          case Negative            => math.Negate.left
          case Distinct            => set.Distinct.left
          case FlattenMapKeys      => structural.FlattenMapKeys.left
          case FlattenMapValues    => structural.FlattenMap.left
          case ShiftMapKeys        => structural.ShiftMapKeys.left
          case ShiftMapValues      => structural.ShiftMap.left
          case FlattenArrayIndices => structural.FlattenArrayIndices.left
          case FlattenArrayValues  => structural.FlattenArray.left
          case ShiftArrayIndices   => structural.ShiftArrayIndices.left
          case ShiftArrayValues    => structural.ShiftArray.left
          case UnshiftArray        => structural.UnshiftArray.left
        }): GenericFunc[nat._1] \/ CompilerM[Fix[LP]])
          .valueOr(compileFunction[nat._1](_, Func.Input1(expr)))

      case Ident(name) =>
        CompilerState.fields.flatMap(fields =>
          if (fields.any(_ == name))
            CompilerState.rootTableReq.map(obj =>
              Fix(structural.ObjectProject(obj, lpr.constant(Data.Str(name)))))
          else
            for {
              rName <- relationName(node).fold(fail, emit)
              table <- CompilerState.subtableReq(rName)
            } yield
              if ((rName: String) ≟ name) table
              else Fix(structural.ObjectProject(table, lpr.constant(Data.Str(name)))))

      case InvokeFunction(name, args) if name.toLowerCase ≟ "date_part" =>
        args.traverse(compile0).flatMap {
          case Embed(Constant(Data.Str(part))) :: expr :: Nil =>
            (part.some collect {
              case "century"      => date.ExtractCentury
              case "day"          => date.ExtractDayOfMonth
              case "decade"       => date.ExtractDecade
              case "dow"          => date.ExtractDayOfWeek
              case "doy"          => date.ExtractDayOfYear
              case "epoch"        => date.ExtractEpoch
              case "hour"         => date.ExtractHour
              case "isodow"       => date.ExtractIsoDayOfWeek
              case "isoyear"      => date.ExtractIsoYear
              case "microseconds" => date.ExtractMicroseconds
              case "millennium"   => date.ExtractMillennium
              case "milliseconds" => date.ExtractMilliseconds
              case "minute"       => date.ExtractMinute
              case "month"        => date.ExtractMonth
              case "quarter"      => date.ExtractQuarter
              case "second"       => date.ExtractSecond
              case "week"         => date.ExtractWeek
              case "year"         => date.ExtractYear
            }).cata(
              f => emit(Fix(f(expr))),
              fail(UnexpectedDatePart("\"" + part + "\"")))

          case _ :: _ :: Nil =>
            fail(UnexpectedDatePart(pprint[Cofree[?[_], Annotations]](args(0))))

          case _ =>
            fail(WrongArgumentCount("DATE_PART", 2, args.length))
        }

      case InvokeFunction(name, List(a1)) =>
        functionMapping.get(name.toLowerCase).fold[CompilerM[Fix[LP]]](
          fail(FunctionNotFound(name))) {
          case func @ UnaryFunc(_, _, _, _, _, _, _) =>
            compileFunction[nat._1](func, Func.Input1(a1))
          case func => fail(WrongArgumentCount(name, func.arity, 1))
        }

      case InvokeFunction(name, List(a1, a2)) =>
        (name.toLowerCase ≟ "coalesce").fold((CompilerState.freshName("c") ⊛ compile0(a1) ⊛ compile0(a2))((name, c1, c2) =>
          lpr.let(name, c1,
            relations.Cond(
              // TODO: Ideally this would use `is null`, but that doesn’t makes it
              //       this far (but it should).
              relations.Eq(lpr.free(name), lpr.constant(Data.Null)).embed,
              c2,
              lpr.free(name)).embed)),
          functionMapping.get(name.toLowerCase).fold[CompilerM[Fix[LP]]](
            fail(FunctionNotFound(name))) {
            case func @ BinaryFunc(_, _, _, _, _, _, _) =>
              compileFunction[nat._2](func, Func.Input2(a1, a2))
            case func => fail(WrongArgumentCount(name, func.arity, 2))
          })

      case InvokeFunction(name, List(a1, a2, a3)) =>
        functionMapping.get(name.toLowerCase).fold[CompilerM[Fix[LP]]](
          fail(FunctionNotFound(name))) {
          case func @ TernaryFunc(_, _, _, _, _, _, _) =>
            compileFunction[nat._3](func, Func.Input3(a1, a2, a3))
          case func => fail(WrongArgumentCount(name, func.arity, 3))
        }

      case InvokeFunction(name, args) =>
        functionMapping.get(name.toLowerCase).fold[CompilerM[Fix[LP]]](
          fail(FunctionNotFound(name)))(
          func => fail(WrongArgumentCount(name, func.arity, args.length)))

      case Match(expr, cases, default0) =>
        for {
          expr    <- compile0(expr)
          default <- default0.fold(emit(lpr.constant(Data.Null)))(compile0)
          cases   <- compileCases(cases, default) {
            case Case(cse, expr2) =>
              (compile0(cse) ⊛ compile0(expr2))((cse, expr2) =>
                (relations.Eq(expr, cse).embed, expr2))
          }
        } yield cases

      case Switch(cases, default0) =>
        default0.fold(emit(lpr.constant(Data.Null)))(compile0).flatMap(
          compileCases(cases, _) {
            case Case(cond, expr2) =>
              (compile0(cond) ⊛ compile0(expr2))((_, _))
          })

      case IntLiteral(value) => emit(lpr.constant(Data.Int(value)))
      case FloatLiteral(value) => emit(lpr.constant(Data.Dec(value)))
      case StringLiteral(value) => emit(lpr.constant(Data.Str(value)))
      case BoolLiteral(value) => emit(lpr.constant(Data.Bool(value)))
      case NullLiteral() => emit(lpr.constant(Data.Null))
      case Vari(name) => emit(lpr.free(Symbol(name)))
    }
  }

  def compile(tree: Cofree[Sql, SA.Annotations])(
      implicit F: Monad[F]): F[SemanticError \/ Fix[LP]] = {
    compile0(tree).eval(CompilerState(Nil, Context(Nil, Nil), 0)).run.map(_.map(Compiler.reduceGroupKeys))
  }
}

object Compiler {
  def apply[F[_]]: Compiler[F] = new Compiler[F] {}

  def trampoline = apply[scalaz.Free.Trampoline]

  def compile(tree: Cofree[Sql, SA.Annotations]):
      SemanticError \/ Fix[LP] =
    trampoline.compile(tree).run

  /** Emulate SQL semantics by reducing any projection which trivially
    * matches a key in the "group by".
    */
  def reduceGroupKeys(tree: Fix[LP]): Fix[LP] = {
    // Step 0: identify key expressions, and rewrite them by replacing the
    // group source with the source at the point where they might appear.
    def keysƒ(t: LP[(Fix[LP], List[Fix[LP]])]):
        (Fix[LP], List[Fix[LP]]) =
    {
      def groupedKeys(t: LP[Fix[LP]], newSrc: Fix[LP]): Option[List[Fix[LP]]] = {
        t match {
          case InvokeUnapply(set.GroupBy, Sized(src, structural.MakeArrayN(keys))) =>
            Some(keys.map(_.transCataT(t => if (t ≟ src) newSrc else t)))
          case InvokeUnapply(func, Sized(src, _)) if func.effect ≟ Sifting =>
            groupedKeys(src.unFix, newSrc)
          case _ => None
        }
      }

      (Fix(t.map(_._1)),
        groupedKeys(t.map(_._1), Fix(t.map(_._1))).getOrElse(t.foldMap(_._2)))
    }
    val keys: List[Fix[LP]] = boundCata(tree)(keysƒ)._2

    // Step 1: annotate nodes containing the keys.
    val ann: Cofree[LP, Boolean] = boundAttribute(tree)(keys.contains)

    // Step 2: transform from the top, inserting Arbitrary where a key is not
    // otherwise reduced.
    def rewriteƒ: Coalgebra[LP, Cofree[LP, Boolean]] = {
      def strip(v: Cofree[LP, Boolean]) = Cofree(false, v.tail)

      t => t.tail match {
        case InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(arg)) if func.effect ≟ Reduction =>
          Invoke[Cofree[LP, Boolean], nat._1](func, Func.Input1(strip(arg)))

        case _ =>
          if (t.head) Invoke(agg.Arbitrary, Func.Input1(strip(t)))
          else t.tail
      }
    }
    ann.ana[Fix, LP](rewriteƒ)
  }
}
