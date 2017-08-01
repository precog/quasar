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

package quasar.sql

import slamdata.Predef._
import quasar.{Data, Func, GenericFunc, HomomorphicFunction, Reduction, SemanticError, Sifting, UnaryFunc, VarName},
  SemanticError._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.contrib.shapeless._
import quasar.common.SortDir
import quasar.fp._
import quasar.fp.binder._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}
import quasar.std.StdLib, StdLib._
import quasar.std.TemporalPart
import quasar.sql.{SemanticAnalysis => SA}, SA._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import pathy.Path._
import scalaz.{Tree => _, _}, Scalaz._
import shapeless.{Annotations => _, Data => _, :: => _, _}

final case class TableContext[T]
  (root: Option[T], full: () => T, subtables: Map[String, T])
  (implicit T: Corecursive.Aux[T, LP]) {
  def ++(that: TableContext[T]): TableContext[T] =
    TableContext(
      None,
      () => structural.ObjectConcat(this.full(), that.full()).embed,
      this.subtables ++ that.subtables)
}

final case class BindingContext[T]
  (subbindings: Map[String, T]) {
  def ++(that: BindingContext[T]): BindingContext[T] =
    BindingContext(this.subbindings ++ that.subbindings)
}

final case class Context[T]
  (bindingContext: List[BindingContext[T]],
    tableContext: List[TableContext[T]]) {

  def add(bc: BindingContext[T], tc: TableContext[T]): Context[T] = {
    val modBindingContext: List[BindingContext[T]] =
      this.bindingContext match {
        case head :: tail => head ++ bc :: head :: tail
        case Nil => bc :: Nil
      }

    val modTableContext: List[TableContext[T]] =
      tc :: this.tableContext

    Context(modBindingContext, modTableContext)
  }

  def dropHead: Context[T] =
    Context(this.bindingContext.drop(1), this.tableContext.drop(1))
}

final case class CompilerState[T]
  (fields: List[String], context: Context[T], nameGen: Int)

private object CompilerState {
  /** Runs a computation inside a binding/table context, which contains
    * compilation data for the bindings/tables in scope.
    */
  def contextual[M[_], T, A]
    (bc: BindingContext[T], tc: TableContext[T])
    (compM: M[A])
    (implicit m: MonadState[M, CompilerState[T]])
      : M[A] = {

    def preMod: CompilerState[T] => CompilerState[T] =
      (state: CompilerState[T]) => state.copy(context = state.context.add(bc, tc))

    def postMod: CompilerState[T] => CompilerState[T] =
      (state: CompilerState[T]) => state.copy(context = state.context.dropHead)

    m.modify(preMod) *> compM <* m.modify(postMod)
  }

  def addFields[M[_], T, A]
    (add: List[String])(f: M[A])(implicit m: MonadState[M, CompilerState[T]])
      : M[A] =
    for {
      curr <- fields
      _    <- m.modify((s: CompilerState[T]) => s.copy(fields = curr ++ add))
      a    <- f
    } yield a

  def fields[M[_], T](implicit m: MonadState[M, CompilerState[T]])
      : M[List[String]] =
    m.get ∘ (_.fields)

  def rootTable[M[_], T](implicit m: MonadState[M, CompilerState[T]])
      : M[Option[T]] =
    m.get ∘ (_.context.tableContext.headOption.flatMap(_.root))

  def rootTableReq[M[_], T]
    (implicit
      MErr: MonadError_[M, SemanticError],
      MState: MonadState[M, CompilerState[T]])
      : M[T] =
    rootTable >>=
      (_.fold(MErr.raiseError[T](CompiledTableMissing))(_.point[M]))

  // prioritize binding context - when we want to prioritize a table,
  // we will have the table reference already in the binding context
  def subtable[M[_], T]
    (name: String)
    (implicit m: MonadState[M, CompilerState[T]])
      : M[Option[T]] =
    m.get ∘ { state =>
      state.context.bindingContext.headOption.flatMap { bc =>
        bc.subbindings.get(name) match {
          case None =>
            state.context.tableContext.headOption.flatMap(_.subtables.get(name))
          case s => s
        }
      }
    }

  def subtableReq[M[_], T]
    (name: String)
    (implicit
      MErr: MonadError_[M, SemanticError],
      MState: MonadState[M, CompilerState[T]])
      : M[T] =
    subtable(name) >>=
      (_.fold(
        MErr.raiseError[T](CompiledSubtableMissing(name)))(
        _.point[M]))

  def fullTable[M[_], T](implicit m: MonadState[M, CompilerState[T]])
      : M[Option[T]] =
    m.get ∘ (_.context.tableContext.headOption.map(_.full()))

  /** Generates a fresh name for use as an identifier, e.g. tmp321. */
  def freshName[M[_], T](prefix: String)(implicit m: MonadState[M, CompilerState[T]]): M[scala.Symbol] =
    m.get ∘ (s => scala.Symbol(prefix + s.nameGen.toString)) <*
      m.modify((s: CompilerState[T]) => s.copy(nameGen = s.nameGen + 1))
}

final class Compiler[M[_], T: Equal]
  (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) {
  import identity._
  import JoinDir._

  val lpr = new LogicalPlanR[T]

  private def syntheticOf(node: CoExpr): List[Option[Synthetic]] =
    node.head._1

  private def provenanceOf(node: CoExpr): Provenance =
    node.head._2

  private def fail[A]
    (error: SemanticError)
    (implicit m: MonadError_[M, SemanticError]):
      M[A] =
    m.raiseError(error)

  private def emit[A](value: A)(implicit m: Monad[M]): M[A] =
    value.point[M]

  type CoExpr = Cofree[Sql, SA.Annotations]

  // CORE COMPILER
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def compile0
    (node: CoExpr)
    (implicit
      MErr: MonadError_[M, SemanticError],
      MState: MonadState[M, CompilerState[T]])
      : M[T] = {
    // NB: When there are multiple names for the same function, we may mark one
    //     with an `*` to indicate that it’s the “preferred” name, and others
    //     are for compatibility with other SQL dialects.
    val functionMapping = Map[CIName, GenericFunc[_]](
      CIName("count")                   -> agg.Count,
      CIName("sum")                     -> agg.Sum,
      CIName("min")                     -> agg.Min,
      CIName("max")                     -> agg.Max,
      CIName("avg")                     -> agg.Avg,
      CIName("arbitrary")               -> agg.Arbitrary,
      CIName("array_length")            -> array.ArrayLength,
      CIName("date")                    -> date.Date,
      CIName("clock_timestamp")         -> date.Now, // Postgres (instantaneous)
      CIName("current_timestamp")       -> date.Now, // *, SQL92
      CIName("getdate")                 -> date.Now, // SQL Server
      CIName("localtimestamp")          -> date.Now, // MySQL, Postgres (trans start)
      CIName("now")                     -> date.Now, // MySQL, Postgres (trans start)
      CIName("statement_timestamp")     -> date.Now, // Postgres (statement start)
      CIName("transaction_timestamp")   -> date.Now, // Postgres (trans start)
      CIName("time")                    -> date.Time,
      CIName("timestamp")               -> date.Timestamp,
      CIName("interval")                -> date.Interval,
      CIName("start_of_day")            -> date.StartOfDay,
      CIName("time_of_day")             -> date.TimeOfDay,
      CIName("to_timestamp")            -> date.ToTimestamp,
      CIName("squash")                  -> identity.Squash,
      CIName("oid")                     -> identity.ToId,
      CIName("type_of")                 -> identity.TypeOf,
      CIName("abs")                     -> math.Abs,
      CIName("ceil")                    -> math.Ceil,
      CIName("floor")                   -> math.Floor,
      CIName("trunc")                   -> math.Trunc,
      CIName("between")                 -> relations.Between,
      CIName("where")                   -> set.Filter,
      CIName("distinct")                -> set.Distinct,
      CIName("within")                  -> set.Within,
      CIName("constantly")              -> set.Constantly,
      CIName("concat")                  -> string.Concat,
      CIName("like")                    -> string.Like,
      CIName("search")                  -> string.Search,
      CIName("length")                  -> string.Length,
      CIName("lower")                   -> string.Lower,
      CIName("upper")                   -> string.Upper,
      CIName("substring")               -> string.Substring,
      CIName("boolean")                 -> string.Boolean,
      CIName("integer")                 -> string.Integer,
      CIName("decimal")                 -> string.Decimal,
      CIName("null")                    -> string.Null,
      CIName("to_string")               -> string.ToString,
      CIName("make_object")             -> structural.MakeObject,
      CIName("make_array")              -> structural.MakeArray,
      CIName("object_concat")           -> structural.ObjectConcat,
      CIName("array_concat")            -> structural.ArrayConcat,
      CIName("delete_field")            -> structural.DeleteField,
      CIName("flatten_map")             -> structural.FlattenMap,
      CIName("flatten_array")           -> structural.FlattenArray,
      CIName("shift_map")               -> structural.ShiftMap,
      CIName("shift_array")             -> structural.ShiftArray,
      CIName("meta")                    -> structural.Meta)

    def compileCases
      (cases: List[Case[CoExpr]], default: T)
      (f: Case[CoExpr] => M[(T, T)]) =
      cases.traverse(f).map(_.foldRight(default) {
        case ((cond, expr), default) =>
          relations.Cond(cond, expr, default).embed
      })

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def flattenJoins(term: T, relations: SqlRelation[CoExpr]):
        T = relations match {
      case _: NamedRelation[_]             => term
      case JoinRelation(left, right, _, _) =>
        structural.ObjectConcat(
          flattenJoins(Left.projectFrom(term), left),
          flattenJoins(Right.projectFrom(term), right)).embed
    }

    def buildJoinDirectionMap(relations: SqlRelation[CoExpr]):
        Map[String, List[JoinDir]] = {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def loop(rel: SqlRelation[CoExpr], acc: List[JoinDir]):
          Map[String, List[JoinDir]] = rel match {
        case t: NamedRelation[_] => Map(t.aliasName -> acc)
        case JoinRelation(left, right, tpe, clause) =>
          loop(left, Left :: acc) ++ loop(right, Right :: acc)
      }

      loop(relations, Nil)
    }

    def compileTableRefs(joined: T, relations: SqlRelation[CoExpr]):
        Map[String, T] =
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldRight(
            joined)(
            (dir, acc) => dir.projectFrom(acc))
      }

    def tableContext(joined: T, relations: SqlRelation[CoExpr]):
        TableContext[T] =
      TableContext(
        Some(joined),
        () => flattenJoins(joined, relations),
        compileTableRefs(joined, relations))

    def step(relations: SqlRelation[CoExpr])
        : (Option[M[T]] => M[T] => M[T]) = {
      (current: Option[M[T]]) =>
      (next: M[T]) =>
      current.map { current =>
        for {
          stepName <- CompilerState.freshName("tmp")
          current  <- current
          bc        = relations match {
            case ExprRelationAST(_, name)        => BindingContext(Map(name -> lpr.free(stepName)))
            case TableRelationAST(_, Some(name)) => BindingContext(Map(name -> lpr.free(stepName)))
            case id @ IdentRelationAST(_, _)     => BindingContext(Map(id.aliasName -> lpr.free(stepName)))
            case r                               => BindingContext[T](Map())
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
          val filtered = namedRel.filter(x => x._1 ≟ pprint(forgetAnnotation[CoExpr, Fix[Sql], Sql, SA.Annotations](node)))
          if (filtered.isEmpty) namedRel else filtered
        }
      relations.toList match {
        case Nil             => -\/ (NoTableDefined(forgetAnnotation[CoExpr, Fix[Sql], Sql, SA.Annotations](node)))
        case List((name, _)) =>  \/-(name)
        case x               => -\/ (AmbiguousReference(forgetAnnotation[CoExpr, Fix[Sql], Sql, SA.Annotations](node), x.map(_._2).join))
      }
    }

    def compileFunction[N <: Nat](func: GenericFunc[N], args: Func.Input[CoExpr, N]):
        M[T] =
      args.traverse(compile0).map(func.applyGeneric(_).embed)

    def buildRecord(names: List[Option[String]], values: List[T]):
        T = {
      val fields = names.zip(values).map {
        case (Some(name), value) =>
          structural.MakeObject(lpr.constant(Data.Str(name)), value).embed
        case (None, value) => value
      }

      fields.reduceOption(structural.ObjectConcat(_, _).embed)
        .getOrElse(lpr.constant(Data.Obj()))
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def compileRelation(r: SqlRelation[CoExpr]): M[T] =
      r match {
        case IdentRelationAST(name, _) =>
          CompilerState.subtableReq[M, T](name)

        case VariRelationAST(vari, _) =>
          fail(UnboundVariable(VarName(vari.symbol)))

        case TableRelationAST(path, _) =>
          sandboxCurrent(canonicalize(path)).cata(
            p => emit(lpr.read(p)),
            fail(InvalidPathError(path, None)))

        case ExprRelationAST(expr, _) => compile0(expr)

        case JoinRelation(left, right, tpe, clause) =>
          (CompilerState.freshName("left") ⊛ CompilerState.freshName("right"))((leftName, rightName) => {
            val leftFree: T = lpr.joinSideName(leftName)
            val rightFree: T = lpr.joinSideName(rightName)

            (compileRelation(left) ⊛
              compileRelation(right) ⊛
              CompilerState.contextual(
                BindingContext(Map()),
                tableContext(leftFree, left) ++ tableContext(rightFree, right))
              (compile0(clause)))((left0, right0, clause0) =>
                lpr.join(left0, right0, tpe, JoinCondition(leftName, rightName, clause0)))
          }).join
      }

    def extractFunc(part: String): Option[UnaryFunc] =
      part.some collect {
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
      }

    def temporalPart(part: String): Option[TemporalPart] = {
      import TemporalPart._

      part.some collect {
        case "century"         => Century
        case "day"             => Day
        case "decade"          => Decade
        case "hour"            => Hour
        case "microseconds"    => Microsecond
        case "millennium"      => Millennium
        case "milliseconds"    => Millisecond
        case "minute"          => Minute
        case "month"           => Month
        case "quarter"         => Quarter
        case "second"          => Second
        case "week"            => Week
        case "year"            => Year
      }
    }

    def temporalPartFunc[A](
      name: CIName, args: List[CoExpr], f1: String => Option[A], f2: (A, T) => M[T]
    ): M[T] =
      args.traverse(compile0).flatMap {
        case Embed(Constant(Data.Str(part))) :: expr :: Nil =>
          f1(part).cata(
            f2(_, expr),
            fail(UnexpectedDatePart("\"" + part + "\"")))
        case _ :: _ :: Nil =>
          fail(UnexpectedDatePart(pprint(forgetAnnotation[CoExpr, Fix[Sql], Sql, SA.Annotations](args(0)))))
        case _ =>
          fail(WrongArgumentCount(name, 2, args.length))
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
          projectionNames[Fix[Sql]](projections.map(_.map(forgetAnnotation[CoExpr, Fix[Sql], Sql, SA.Annotations])), relationName(node).toOption).map(_.map {
            case (name, Embed(expr)) => expr match {
              case Splice(_) => None
              case _         => name.some
            }
          })

        namesOrError.fold(
          MErr.raiseError,
          names => {

            val syntheticNames: List[String] =
              names.zip(syntheticOf(node)).flatMap {
                case (Some(name), Some(_)) => List(name)
                case (_, _) => Nil
              }

            val (nam, initial) =
              projections match {
                case List(Proj(Cofree(_, Splice(_)), None)) =>
                  (names.some,
                    projections
                      .map(_.expr)
                      .traverse(compile0)
                      .map(buildRecord(names, _)))
                case List(Proj(expr, None)) => (none, compile0(expr))
                case _ =>
                  (names.some,
                    projections
                      .map(_.expr)
                      .traverse(compile0)
                      .map(buildRecord(names, _)))
              }

            relations.foldRight(
              initial)(
              (relations, select) => {
                val stepBuilder = step(relations)
                stepBuilder(compileRelation(relations).some) {
                  val filtered = filter.map(filter =>
                    (CompilerState.rootTableReq[M, T] ⊛ compile0(filter))(
                      set.Filter(_, _).embed))

                  stepBuilder(filtered) {
                    val grouped = groupBy.map(groupBy =>
                      (CompilerState.rootTableReq[M, T] ⊛
                        groupBy.keys.traverse(compile0)) ((src, keys) =>
                        set.GroupBy(src, structural.MakeArrayN(keys: _*).embed).embed))

                    stepBuilder(grouped) {
                      val having = groupBy.flatMap(_.having).map(having =>
                        (CompilerState.rootTableReq[M, T] ⊛ compile0(having))(
                          set.Filter(_, _).embed))

                      stepBuilder(having) {
                        val squashed = select.map(Squash(_).embed)

                        stepBuilder(squashed.some) {
                          val sort = orderBy.map(orderBy =>
                            CompilerState.rootTableReq[M, T] >>= (t =>
                              nam.fold(
                                orderBy.keys.traverse(p => (t, p._1).point[M]))(
                                n => CompilerState.addFields(n.foldMap(_.toList))(orderBy.keys.traverse { case (ot, key) => compile0(key) strengthR ot }))
                                .map(ks => lpr.sort(t, ks map {
                                  case (k, ASC ) => (k, SortDir.Ascending)
                                  case (k, DESC) => (k, SortDir.Descending)
                                }))))

                          stepBuilder(sort) {
                            val distincted = isDistinct match {
                              case SelectDistinct =>
                                CompilerState.rootTableReq[M, T].map(t =>
                                  if (syntheticNames.nonEmpty)
                                    set.DistinctBy(t, syntheticNames.foldLeft(t)((acc, field) =>
                                      structural.DeleteField(acc, lpr.constant(Data.Str(field))).embed)).embed
                                  else set.Distinct(t).embed).some
                              case _ => None
                            }

                            stepBuilder(distincted) {
                              val pruned =
                                CompilerState.rootTableReq[M, T].map(
                                  syntheticNames.foldLeft(_)((acc, field) =>
                                    structural.DeleteField(acc,
                                      lpr.constant(Data.Str(field))).embed))

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
        val rel = ExprRelationAST(form, name.value)
        step(rel)(compile0(form).some)(compile0(body))
      }

      case SetLiteral(values0) =>
        values0.traverse(compile0).map(vs =>
          structural.ShiftArray(structural.MakeArrayN(vs: _*).embed).embed)

      case ArrayLiteral(exprs) =>
        exprs.traverse(compile0).map(structural.MakeArrayN(_: _*).embed)

      case MapLiteral(exprs) =>
        exprs.traverse(_.bitraverse(compile0, compile0)) ∘
        (structural.MakeObjectN(_: _*).embed)

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
          case Sample        => set.Sample.left
          case UnshiftMap    => structural.UnshiftMap.left
          case Except        => set.Except.left
          case UnionAll      => set.Union.left
          case IntersectAll  => set.Intersect.left
          // TODO: These two cases are eliminated by `normalizeƒ` and would be
          //       better represented in a Coproduct.
          case f @ Union     => fail(GenericError("Should not have encountered a union at this point in compilation")).right
          case f @ Intersect => fail(GenericError("Should not have encountered an intersect at this point in compilation")).right
        }): GenericFunc[nat._2] \/ M[T])
          .valueOr(compileFunction[nat._2](_, Func.Input2(left, right)))

      case Unop(expr, op) =>
        ((op match {
          case Not                 => relations.Not.left
          case f @ Exists          => fail(GenericError("Should not have encountered an exists at this point in compilation")).right
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
        }): GenericFunc[nat._1] \/ M[T])
          .valueOr(compileFunction[nat._1](_, Func.Input1(expr)))

      case Ident(name) =>
        CompilerState.fields.flatMap(fields =>
          if (fields.any(_ ≟ name))
            CompilerState.rootTableReq[M, T] ∘
            (structural.ObjectProject(_, lpr.constant(Data.Str(name))).embed)
          else
            for {
              rName <- relationName(node).fold(fail, emit)
              table <- CompilerState.subtableReq[M, T](rName)
            } yield
              if ((rName: String) ≟ name) table
              else structural.ObjectProject(table, lpr.constant(Data.Str(name))).embed)

      case InvokeFunction(name, args) if
          name ≟ CIName("date_part")  || name ≟ CIName("temporal_part") =>
        temporalPartFunc(name, args, extractFunc, (f: UnaryFunc, expr: T) => emit(f(expr).embed))

      case InvokeFunction(name, args) if
          name ≟ CIName("date_trunc") || name ≟ CIName("temporal_trunc") =>
        temporalPartFunc(name, args, temporalPart, (p: TemporalPart, expr: T) => emit(TemporalTrunc(p, expr).embed))

      // A call to the SQL coalesce function does not map to an invocation of a function in Logical Plan
      // so here we inline the logical plan that it should produce
      case InvokeFunction(name, args) if name ≟ CIName("coalesce") =>
        args match {
          case List(a1, a2) =>
            (CompilerState.freshName("c") ⊛ compile0(a1) ⊛ compile0(a2))((name, c1, c2) =>
              lpr.let(name, c1,
                relations.Cond(
                  // TODO: Ideally this would use `is null`, but that doesn’t makes it
                  //       this far (but it should).
                  relations.Eq(lpr.free(name), lpr.constant(Data.Null)).embed,
                  c2,
                  lpr.free(name)).embed))
          case xs => fail(WrongArgumentCount(name, 2, xs.size))
        }

      case InvokeFunction(name, args) =>
        val function: Option[HomomorphicFunction[T, T]] = functionMapping.mapValues(_.toFunction[T].andThen(_.embed)).lift.apply(name)
        function.cata[M[T]](
          func => args.traverse(compile0).flatMap(func.apply(_).cata(
            successfulInvoke => successfulInvoke.point[M],
            fail(WrongArgumentCount(name, func.arity, args.size)))),
          fail(FunctionNotFound(name)))

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
      case Vari(name) => emit(lpr.free(scala.Symbol(name)))
    }
  }

  // TODO: This could have fewer constraints if we didn’t have to use the same
  //       Monad as `compile0`.
  def compile
    (tree: Cofree[Sql, SA.Annotations])
    (implicit
      MErr: MonadError_[M, SemanticError],
      MState: MonadState[M, CompilerState[T]])
      : M[T] =
    compile0(tree).map(Compiler.reduceGroupKeys[T])
}

object Compiler {
  def apply[M[_], T: Equal]
    (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
    new Compiler[M, T]

  def trampoline[T: Equal]
    (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
    apply[StateT[EitherT[scalaz.Free.Trampoline, SemanticError, ?], CompilerState[T], ?], T]

  def compile[T: Equal]
    (tree: Cofree[Sql, SA.Annotations])
    (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
      : SemanticError \/ T =
    trampoline[T].compile(tree).eval(CompilerState(Nil, Context(Nil, Nil), 0)).run.run

  /** Emulate SQL semantics by reducing any projection which trivially
    * matches a key in the "group by".
    */
  def reduceGroupKeys[T: Equal]
    (tree: T)
    (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
      : T = {
    // Step 0: identify key expressions, and rewrite them by replacing the
    // group source with the source at the point where they might appear.
    def keysƒ(t: LP[(T, List[T])]):
        (T, List[T]) =
    {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def groupedKeys(t: LP[T], newSrc: T): Option[List[T]] = {
        t match {
          case InvokeUnapply(set.GroupBy, Sized(src, structural.MakeArrayN(keys))) =>
            Some(keys.map(_.transCataT(t => if (t ≟ src) newSrc else t)))
          case InvokeUnapply(func, Sized(src, _)) if func.effect ≟ Sifting =>
            groupedKeys(src.project, newSrc)
          case _ => None
        }
      }

      (t.map(_._1).embed,
        groupedKeys(t.map(_._1), t.map(_._1).embed).getOrElse(t.foldMap(_._2)))
    }

    // use `scalaz.IList` so we can use `scalaz.Equal[LP]`
    val keys: IList[T] = IList.fromList(boundCata(tree)(keysƒ)._2)

    // Step 1: annotate nodes containing the keys.
    val ann: Cofree[LP, Boolean] = boundAttribute(tree)(keys.element)

    // Step 2: transform from the top, inserting Arbitrary where a key is not
    // otherwise reduced.
    def rewriteƒ: Coalgebra[LP, Cofree[LP, Boolean]] = {
      def strip(v: Cofree[LP, Boolean]) = Cofree(false, v.tail)

      t => t.tail match {
        case InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(arg)) if func.effect ≟ Reduction =>
          Invoke[nat._1, Cofree[LP, Boolean]](func, Func.Input1(strip(arg)))

        case _ =>
          if (t.head) Invoke(agg.Arbitrary, Func.Input1(strip(t)))
          else t.tail
      }
    }
    ann.ana[T](rewriteƒ)
  }
}
