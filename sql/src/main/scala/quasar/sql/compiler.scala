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
import quasar.{BinaryFunc, Data, Func, GenericFunc, LogicalPlan, Reduction, SemanticError, Sifting, TernaryFunc, UnaryFunc, VarName},
  SemanticError._
import quasar.contrib.pathy._
import quasar.contrib.shapeless._
import quasar.fp._
import quasar.fp.binder._
import quasar.std.StdLib, StdLib._
import quasar.sql.{SemanticAnalysis => SA}, SA._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import pathy.Path._
import scalaz.{Tree => _, _}, Scalaz._
import shapeless.{Data => _, :: => _, _}

trait Compiler[F[_]] {
  import identity._
  import set._
  import structural._
  import JoinDir._

  // HELPERS
  private type M[A] = EitherT[F, SemanticError, A]

  private type CompilerStateT[F[_],A] = StateT[F, CompilerState, A]
  private type CompilerM[A] = CompilerStateT[M, A]

  private def syntheticOf(node: CoExpr): List[Option[Synthetic]] =
    node.head._1

  private def provenanceOf(node: CoExpr): Provenance =
    node.head._2

  private final case class TableContext(
    root: Option[Fix[LogicalPlan]],
    full: () => Fix[LogicalPlan],
    subtables: Map[String, Fix[LogicalPlan]]) {
    def ++(that: TableContext): TableContext =
      TableContext(
        None,
        () => Fix(ObjectConcat(this.full(), that.full())),
        this.subtables ++ that.subtables)
  }

  private final case class BindingContext(
    subbindings: Map[String, Fix[LogicalPlan]]) {
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

    def rootTable(implicit m: Monad[F]): CompilerM[Option[Fix[LogicalPlan]]] =
      read[CompilerState, Option[Fix[LogicalPlan]]](_.context.tableContext.headOption.flatMap(_.root))

    def rootTableReq(implicit m: Monad[F]): CompilerM[Fix[LogicalPlan]] =
      rootTable.flatMap(_.map(emit).getOrElse(fail(CompiledTableMissing)))

    // prioritize binding context - when we want to prioritize a table,
    // we will have the table reference already in the binding context
    def subtable(name: String)(implicit m: Monad[F]):
        CompilerM[Option[Fix[LogicalPlan]]] =
      read[CompilerState, Option[Fix[LogicalPlan]]]{ state =>
        state.context.bindingContext.headOption.flatMap { bc =>
          bc.subbindings.get(name) match {
            case None =>
              state.context.tableContext.headOption.flatMap(_.subtables.get(name))
            case s => s
          }
        }
      }

    def subtableReq(name: String)(implicit m: Monad[F]):
        CompilerM[Fix[LogicalPlan]] =
      subtable(name).flatMap(
        _.map(emit).getOrElse(fail(CompiledSubtableMissing(name))))

    def fullTable(implicit m: Monad[F]): CompilerM[Option[Fix[LogicalPlan]]] =
      read[CompilerState, Option[Fix[LogicalPlan]]](_.context.tableContext.headOption.map(_.full()))

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
    lift(value.right)

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

  // TODO: parameterize this
  val library = StdLib

  type CoAnn[F[_]] = Cofree[F, SA.Annotations]
  type CoExpr = CoAnn[Sql]

  // CORE COMPILER
  private def compile0(node: CoExpr)(implicit M: Monad[F]):
      CompilerM[Fix[LogicalPlan]] = {

    def findUnaryFunction(name: String): CompilerM[GenericFunc[nat._1]] =
      library.functions.find(f => f.name.toLowerCase === name.toLowerCase).fold[CompilerM[GenericFunc[nat._1]]](
        fail(FunctionNotFound(name))) {
          case func @ UnaryFunc(_, _, _, _, _, _, _, _) => emit(func)
          case func => fail(WrongArgumentCount(name, func.arity, 1))
        }

    def findBinaryFunction(name: String): CompilerM[GenericFunc[nat._2]] =
      library.functions.find(f => f.name.toLowerCase === name.toLowerCase).fold[CompilerM[GenericFunc[nat._2]]](
        fail(FunctionNotFound(name))) {
          case func @ BinaryFunc(_, _, _, _, _, _, _, _) => emit(func)
          case func => fail(WrongArgumentCount(name, func.arity, 2))
        }

    def findTernaryFunction(name: String): CompilerM[GenericFunc[nat._3]] =
      library.functions.find(f => f.name.toLowerCase === name.toLowerCase).fold[CompilerM[GenericFunc[nat._3]]](
        fail(FunctionNotFound(name))) {
          case func @ TernaryFunc(_, _, _, _, _, _, _, _) => emit(func)
          case func => fail(WrongArgumentCount(name, func.arity, 3))
        }

    def findNaryFunction(name: String, length: Int): CompilerM[Fix[LogicalPlan]] =
      library.functions.find(f => f.name.toLowerCase === name.toLowerCase).fold[CompilerM[Fix[LogicalPlan]]](
        fail(FunctionNotFound(name))) {
          case func => fail(WrongArgumentCount(name, func.arity, length))
        }

    def compileCases(cases: List[Case[CoExpr]], default: Fix[LogicalPlan])(f: Case[CoExpr] => CompilerM[(Fix[LogicalPlan], Fix[LogicalPlan])]) =
      cases.traverse(f).map(_.foldRight(default) {
        case ((cond, expr), default) => Fix(relations.Cond(cond, expr, default))
      })

    def flattenJoins(term: Fix[LogicalPlan], relations: SqlRelation[CoExpr]):
        Fix[LogicalPlan] = relations match {
      case _: NamedRelation[_]             => term
      case JoinRelation(left, right, _, _) =>
        Fix(ObjectConcat(
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

    def compileTableRefs(joined: Fix[LogicalPlan], relations: SqlRelation[CoExpr]):
        Map[String, Fix[LogicalPlan]] =
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldRight(
            joined)(
            (dir, acc) => dir.projectFrom(acc))
      }

    def tableContext(joined: Fix[LogicalPlan], relations: SqlRelation[CoExpr]):
        TableContext =
      TableContext(
        Some(joined),
        () => flattenJoins(joined, relations),
        compileTableRefs(joined, relations))

    def step(relations: SqlRelation[CoExpr]):
        (Option[CompilerM[Fix[LogicalPlan]]] =>
          CompilerM[Fix[LogicalPlan]] =>
          CompilerM[Fix[LogicalPlan]]) = {
      (current: Option[CompilerM[Fix[LogicalPlan]]]) =>
      (next: CompilerM[Fix[LogicalPlan]]) =>
      current.map { current =>
        for {
          stepName <- CompilerState.freshName("tmp")
          current  <- current
          bc        = relations match {
            case ExprRelationAST(_, name)        => BindingContext(Map(name -> LogicalPlan.Free(stepName)))
            case TableRelationAST(_, Some(name)) => BindingContext(Map(name -> LogicalPlan.Free(stepName)))
            case id @ IdentRelationAST(_, _) => BindingContext(Map(id.aliasName -> LogicalPlan.Free(stepName)))
            case r                               => BindingContext(Map())
          }
          next2    <- CompilerState.contextual(bc, tableContext(LogicalPlan.Free(stepName), relations))(next)
        } yield LogicalPlan.Let(stepName, current, next2)
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
        CompilerM[Fix[LogicalPlan]] =
      args.traverse(compile0).map(args => Fix(func.applyGeneric(args)))

    def buildRecord(names: List[Option[String]], values: List[Fix[LogicalPlan]]):
        Fix[LogicalPlan] = {
      val fields = names.zip(values).map {
        case (Some(name), value) =>
          Fix(MakeObject(LogicalPlan.Constant(Data.Str(name)), value))
        case (None, value) => value
      }

      fields.reduceOption((a,b) => Fix(ObjectConcat(a, b)))
        .getOrElse(LogicalPlan.Constant(Data.Obj(ListMap())))
    }

    def compileRelation(r: SqlRelation[CoExpr]): CompilerM[Fix[LogicalPlan]] =
      r match {
        case IdentRelationAST(name, _) =>
          CompilerState.subtableReq(name)

        case VariRelationAST(vari, _) =>
          fail(UnboundVariable(VarName(vari.symbol)))

        case TableRelationAST(path, _) =>
          sandboxCurrent(canonicalize(path)).cata(
            p => emit(LogicalPlan.Read(p)),
            fail(InvalidPathError(path, None)))

        case ExprRelationAST(expr, _) => compile0(expr)

        case JoinRelation(left, right, tpe, clause) =>
          for {
            leftName <- CompilerState.freshName("left")
            rightName <- CompilerState.freshName("right")
            leftFree = LogicalPlan.Free(leftName)
            rightFree = LogicalPlan.Free(rightName)
            left0 <- compileRelation(left)
            right0 <- compileRelation(right)
            join <- CompilerState.contextual(
              BindingContext(Map()),
              tableContext(leftFree, left) ++ tableContext(rightFree, right))(
              compile0(clause).map(c =>
                LogicalPlan.Invoke(
                  tpe match {
                    case LeftJoin             => LeftOuterJoin
                    case quasar.sql.InnerJoin => InnerJoin
                    case RightJoin            => RightOuterJoin
                    case FullJoin             => FullOuterJoin
                  },
                  Func.Input3(leftFree, rightFree, c))))
          } yield LogicalPlan.Let(leftName, left0,
            LogicalPlan.Let(rightName, right0, join))
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
          err => EitherT.left[F, SemanticError, Fix[LogicalPlan]](err.point[F]).liftM[CompilerStateT],
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
                    (CompilerState.rootTableReq ⊛ compile0(filter)) ((set, filt) =>
                      Fix(Filter(set, filt))))

                  stepBuilder(filtered) {
                    val grouped = groupBy.map(groupBy =>
                      (CompilerState.rootTableReq ⊛
                        groupBy.keys.traverse(compile0)) ((src, keys) =>
                        Fix(GroupBy(src, Fix(MakeArrayN(keys: _*))))))

                    stepBuilder(grouped) {
                      val having = groupBy.flatMap(_.having).map(having =>
                        (CompilerState.rootTableReq ⊛ compile0(having)) ((set, filt) =>
                          Fix(Filter(set, filt))))

                      stepBuilder(having) {
                        val squashed = select.map(set => Fix(Squash(set)))

                        stepBuilder(squashed.some) {
                          val sort = orderBy.map(orderBy =>
                            for {
                              t <- CompilerState.rootTableReq
                              flat = names.foldMap(_.toList)
                              keys <- CompilerState.addFields(flat)(orderBy.keys.traverse { case (_, key) => compile0(key) })
                              orders = orderBy.keys.map { case (order, _) => LogicalPlan.Constant(Data.Str(order.toString)) }
                            } yield Fix(OrderBy(t, Fix(MakeArrayN(keys: _*)), Fix(MakeArrayN(orders: _*)))))

                          stepBuilder(sort) {
                            val distincted = isDistinct match {
                              case SelectDistinct =>
                                CompilerState.rootTableReq.map(t =>
                                  if (syntheticNames.nonEmpty)
                                    Fix(DistinctBy(t, syntheticNames.foldLeft(t)((acc, field) =>
                                      Fix(DeleteField(acc, LogicalPlan.Constant(Data.Str(field)))))))
                                  else Fix(Distinct(t))).some
                              case _ => None
                            }

                            stepBuilder(distincted) {
                              val pruned =
                                CompilerState.rootTableReq.map(
                                  syntheticNames.foldLeft(_)((acc, field) =>
                                    Fix(DeleteField(acc,
                                      LogicalPlan.Constant(Data.Str(field))))))

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
          ShiftArray(MakeArrayN(vs: _*).embed).embed)

      case ArrayLiteral(exprs) =>
        exprs.traverse(compile0).map(elems => Fix(MakeArrayN(elems: _*)))

      case MapLiteral(exprs) =>
        exprs.traverse(_.bitraverse(compile0, compile0)).map(elems =>
          Fix(MakeObjectN(elems: _*)))

      case Splice(expr) =>
        expr.fold(
          CompilerState.fullTable.flatMap(_.map(emit _).getOrElse(fail(GenericError("Not within a table context so could not find table expression for wildcard")))))(
          compile0)

      case Binop(left, right, op) =>
        findBinaryFunction(op.name).flatMap(compileFunction[nat._2](_, Func.Input2(left, right)))

      case Unop(expr, op) =>
        findUnaryFunction(op.name).flatMap(compileFunction[nat._1](_, Func.Input1(expr)))

      case Ident(name) =>
        CompilerState.fields.flatMap(fields =>
          if (fields.any(_ == name))
            CompilerState.rootTableReq.map(obj =>
              Fix(ObjectProject(obj, LogicalPlan.Constant(Data.Str(name)))))
          else
            for {
              rName <- relationName(node).fold(fail, emit)
              table <- CompilerState.subtableReq(rName)
            } yield
              if ((rName: String) ≟ name) table
              else Fix(ObjectProject(table, LogicalPlan.Constant(Data.Str(name)))))

      case InvokeFunction(name, List(a1)) =>
        findUnaryFunction(name).flatMap(compileFunction[nat._1](_, Func.Input1(a1)))

      case InvokeFunction(name, List(a1, a2)) =>
        findBinaryFunction(name).flatMap(compileFunction[nat._2](_, Func.Input2(a1, a2)))

      case InvokeFunction(name, List(a1, a2, a3)) =>
        findTernaryFunction(name).flatMap(compileFunction[nat._3](_, Func.Input3(a1, a2, a3)))

      case InvokeFunction(name, args) =>
        findNaryFunction(name, args.length)

      case Match(expr, cases, default0) =>
        for {
          expr    <- compile0(expr)
          default <- default0.fold(emit(LogicalPlan.Constant(Data.Null)))(compile0)
          cases   <- compileCases(cases, default) {
            case Case(cse, expr2) =>
              (compile0(cse) ⊛ compile0(expr2))((cse, expr2) =>
                (Fix(relations.Eq(expr, cse)), expr2))
          }
        } yield cases

      case Switch(cases, default0) =>
        default0.fold(emit(LogicalPlan.Constant(Data.Null)))(compile0).flatMap(
          compileCases(cases, _) {
            case Case(cond, expr2) =>
              (compile0(cond) ⊛ compile0(expr2))((_, _))
          })

      case IntLiteral(value) => emit(LogicalPlan.Constant(Data.Int(value)))
      case FloatLiteral(value) => emit(LogicalPlan.Constant(Data.Dec(value)))
      case StringLiteral(value) => emit(LogicalPlan.Constant(Data.Str(value)))
      case BoolLiteral(value) => emit(LogicalPlan.Constant(Data.Bool(value)))
      case NullLiteral() => emit(LogicalPlan.Constant(Data.Null))
      case Vari(name) => emit(LogicalPlan.Free(Symbol(name)))
    }
  }

  def compile(tree: Cofree[Sql, SA.Annotations])(
      implicit F: Monad[F]): F[SemanticError \/ Fix[LogicalPlan]] = {
    compile0(tree).eval(CompilerState(Nil, Context(Nil, Nil), 0)).run.map(_.map(Compiler.reduceGroupKeys))
  }
}

object Compiler {
  import LogicalPlan._

  def apply[F[_]]: Compiler[F] = new Compiler[F] {}

  def trampoline = apply[scalaz.Free.Trampoline]

  def compile(tree: Cofree[Sql, SA.Annotations]):
      SemanticError \/ Fix[LogicalPlan] =
    trampoline.compile(tree).run

  /** Emulate SQL semantics by reducing any projection which trivially
    * matches a key in the "group by".
    */
  def reduceGroupKeys(tree: Fix[LogicalPlan]): Fix[LogicalPlan] = {
    // Step 0: identify key expressions, and rewrite them by replacing the
    // group source with the source at the point where they might appear.
    def keysƒ(t: LogicalPlan[(Fix[LogicalPlan], List[Fix[LogicalPlan]])]):
        (Fix[LogicalPlan], List[Fix[LogicalPlan]]) =
    {
      def groupedKeys(t: LogicalPlan[Fix[LogicalPlan]], newSrc: Fix[LogicalPlan]): Option[List[Fix[LogicalPlan]]] = {
        t match {
          case InvokeFUnapply(set.GroupBy, Sized(src, structural.MakeArrayN(keys))) =>
            Some(keys.map(_.transCataT(t => if (t ≟ src) newSrc else t)))
          case InvokeFUnapply(func, Sized(src, _)) if func.effect ≟ Sifting =>
            groupedKeys(src.unFix, newSrc)
          case _ => None
        }
      }

      (Fix(t.map(_._1)),
        groupedKeys(t.map(_._1), Fix(t.map(_._1))).getOrElse(t.foldMap(_._2)))
    }
    val keys: List[Fix[LogicalPlan]] = boundCata(tree)(keysƒ)._2

    // Step 1: annotate nodes containing the keys.
    val ann: Cofree[LogicalPlan, Boolean] = boundAttribute(tree)(keys.contains)

    // Step 2: transform from the top, inserting Arbitrary where a key is not
    // otherwise reduced.
    def rewriteƒ: Coalgebra[LogicalPlan, Cofree[LogicalPlan, Boolean]] = {
      def strip(v: Cofree[LogicalPlan, Boolean]) = Cofree(false, v.tail)

      t => t.tail match {
        case InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(arg)) if func.effect ≟ Reduction =>
          InvokeF[Cofree[LogicalPlan, Boolean], nat._1](func, Func.Input1(strip(arg)))

        case _ =>
          if (t.head) InvokeF(agg.Arbitrary, Func.Input1(strip(t)))
          else t.tail
      }
    }
    ann.ana[Fix, LogicalPlan](rewriteƒ)
  }
}
