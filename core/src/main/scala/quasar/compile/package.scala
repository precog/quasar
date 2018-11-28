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

package quasar

import slamdata.Predef._
import quasar.CIString._
import quasar.common.{phase, phaseM, PhaseResultTell}
import quasar.common.data.Data
import quasar.contrib.pathy.ADir
import quasar.contrib.scalaz.MonadError_
import quasar.fp._
import quasar.fp.numeric.{Natural, Positive}
import quasar.fp.ski._
import quasar.frontend.logicalplan.{constant, preparePlan, ArgumentErrors, LogicalPlan => LP}
import quasar.sql._
import quasar.std.SetLib.{Drop, Take}

import matryoshka._
import matryoshka.implicits._
import pathy.Path.posixCodec
import scalaz.{Failure => _, _}, Scalaz._

package object compile {
  type SemanticErrors = NonEmptyList[SemanticError]

  type MonadSemanticErr[F[_]] = MonadError_[F, SemanticError]

  def MonadSemanticErr[F[_]](implicit ev: MonadSemanticErr[F]): MonadSemanticErr[F] = ev

  type MonadSemanticErrs[F[_]] = MonadError_[F, SemanticErrors]

  def MonadSemanticErrs[F[_]](implicit ev: MonadSemanticErrs[F]): MonadSemanticErrs[F] = ev

  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  def addOffsetLimit[T]
      (lp: T, off: Natural, lim: Option[Positive])
      (implicit T: Corecursive.Aux[T, LP])
      : T = {
    val skipped = Drop(lp, constant[T](Data.Int(off.value)).embed).embed
    lim.fold(
      skipped)(
      l => Take(skipped, constant[T](Data.Int(l.value)).embed).embed)
  }

  def allVariables: Algebra[Sql, List[VarName]] = {
    case Vari(name) => List(VarName(name))

    case sel @ Select(_, _, rel, _, _, _) =>
      rel.toList.collect {
        case VariRelationAST(vari, _) => VarName(vari.symbol)
      } ++ (sel: Sql[List[VarName]]).fold

    case other => other.fold
  }

  /** Compiles a query into raw LogicalPlan, which has not yet been optimized or
    * typechecked.
    */
  def precompile[
      F[_]: Monad: PhaseResultTell: MonadSemanticErrs,
      U[_[_]]: BirecursiveT: EqualT: RenderTreeT,
      T: Equal: RenderTree: Show]
      (query: U[Sql], vars: Variables, basePath: ADir)
      (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
      : F[T] = {

    import SemanticAnalysis._
    val sqlParser = parser[U]

    for {
      ast      <- phase[F]("SQL AST", query)
      substAst <- phaseM[F]("Variables Substituted", substVars[F, U](sqlParser, ast, vars))
      absAst   <- phase[F]("Absolutized", substAst.mkPathsAbsolute(basePath))
      sortProj <- phase[F]("Sort Keys Projected", projectSortKeys[U[Sql]](absAst))
      annAst   <- phaseM[F]("Annotated Tree", annotate[F, U[Sql]](sortProj))
      compRes  =  Compiler.compile[T](annAst).leftMap(_.wrapNel)
      logical  <- phaseM[F]("Logical Plan", MonadSemanticErrs[F].unattempt(compRes.point[F]))
    } yield logical
  }


  /** Returns the name of the expression when viewed as a projection
    * (of the optional relation), if available.
    */
  def projectionName[T](
    expr: T,
    relationName: Option[String]
  )(implicit
    T: Recursive.Aux[T, Sql]
  ): Option[String] = {
    val flattening = Set("flatten_map".ci, "flatten_array".ci)
    val loop: T => (Option[String] \/ (Option[String] \/ T)) =
      _.project match {
        case Ident(name) => some(name).left
        case Binop(_, Embed(StringLiteral(v)), KeyDeref) => some(v).left
        case Unop(arg, FlattenMapValues) => arg.right.right
        case Unop(arg, FlattenArrayValues) => arg.right.right
        case InvokeFunction(fnName, List(arg)) if flattening.contains(fnName) => arg.right.right
        case _ => None.left
      }

    \/.loopRight(expr.right, ι[Option[String]], loop)
  }

  sealed trait Ordinal
  val Ordinal = Tag.of[Ordinal]

  sealed trait Inferred
  val Inferred = Tag.of[Inferred]

  type ProjectionName = (String @@ Ordinal) \/ (String @@ Inferred)

  def inferred(s: String): ProjectionName =
    Inferred(s).right

  def ordinal(s: String): ProjectionName =
    Ordinal(s).left

  def nameOf(p: ProjectionName): String = p match {
    case \/-(Inferred(inf)) => inf
    case -\/(Ordinal(ord)) => ord
  }

  def projectionNames[T]
    (projections: List[Proj[T]], relName: Option[String])
    (implicit T: Recursive.Aux[T, Sql])
      : SemanticError \/ List[(ProjectionName, T)] = {
    val aliases = projections.flatMap(_.alias.toList)

    (aliases diff aliases.distinct).headOption.cata[SemanticError \/ List[(ProjectionName, T)]](
      duplicateAlias => SemanticError.DuplicateAlias(duplicateAlias).left,
      projections.zipWithIndex.mapAccumLeftM[(ProjectionName, T), Set[String], SemanticError \/ ?](aliases.toSet)
        { case (used, (Proj(expr, alias), index)) =>
          alias.cata(
            a => (used, Inferred(a).right -> expr).right,
            {
              val indexName = Ordinal(index.toString)
              val tentativeName: ProjectionName =
                projectionName(expr, relName)
                  .map(Inferred(_).right).getOrElse(Ordinal(index.toString).left)

              val alternatives: Stream[ProjectionName] =
                Stream.from(0).map(suffix => tentativeName match {
                  case \/-(Inferred(inferred)) => Inferred(inferred.concat(suffix.toString)).right
                  case -\/(Ordinal(ordinal)) => Ordinal(ordinal.concat(suffix.toString)).left
                })

              (tentativeName #:: alternatives).dropWhile(n => used.contains(nameOf(n))).headOption.map { name =>
                val newUsed = used + nameOf(name)
                (newUsed, name -> expr)
              } \/> SemanticError.GenericError("Could not generate alias for a relation") // unlikely since we know it's an quasi-infinite stream
            })
        }.map(_._2))
  }

  /** Returns the `LogicalPlan` for the given SQL^2 query */
  def queryPlan[
      F[_]: Monad: PhaseResultTell: MonadSemanticErrs,
      U[_[_]]: BirecursiveT: EqualT: RenderTreeT,
      T: Equal: RenderTree: Show](
      expr: U[Sql], vars: Variables, basePath: ADir, off: Natural, lim: Option[Positive])(
      implicit TC: Corecursive.Aux[T, LP], TR: Recursive.Aux[T, LP])
      : F[T] =
    precompile[F, U, T](expr, vars, basePath) flatMap { lp =>
      MonadSemanticErrs[F].unattempt(
        preparePlan[EitherT[F, ArgumentErrors, ?], T](addOffsetLimit(lp, off, lim))
          .leftMap(_.map(SemanticError.argError(_)))
          .run)
    }

  def substituteRelationVariable[F[_]: Monad: MonadSemanticErr, T](
      select: Select[T])(
      mapping: Vari[T] => F[T])(
      implicit
      T0: Recursive.Aux[T, Sql],
      T1: Corecursive.Aux[T, Sql])
      : F[Select[T]] = {

    val newRelation = select.relation.traverse(_.transformM[F, T]({
      case VariRelationAST(vari, alias) =>
        mapping(vari).flatMap(_.project match {
          case Ident(name) =>
            posixCodec.parsePath(Some(_), Some(_), κ(None), κ(None))(name)
              .cata(
                p => (TableRelationAST(p, alias): SqlRelation[T]).point[F],
                MonadSemanticErr[F].raiseError(SemanticError.genericError(
                  s"bad path: $name (note: absolute file path required)"))) // FIXME

          // If the variable points to another variable, substitute the old one for the new one
          case Vari(symbol) =>
            (VariRelationAST(Vari(symbol), alias): SqlRelation[T]).point[F]

          case x =>
            MonadSemanticErr[F].raiseError(
              SemanticError.genericError(s"not a valid table name: ${pprint(x.embed)}")) // FIXME
        })

      case otherRelation => otherRelation.point[F]
    }, _.point[F]))

    newRelation.map(r => select.copy(relation = r))
  }

  // FIXME: This traverses `expr` twice and parses all the bound expression twice, once to
  //        check for errors and another time to actual substitute them.
  def substVars[F[_]: Monad: MonadSemanticErrs, T[_[_]]: BirecursiveT](
      parser: SQLParser[T], expr: T[Sql], variables: Variables)
      : F[T[Sql]] = {

    val allVars =
      expr.cata(allVariables)

    val errors =
      allVars.map(binding(parser, variables, _))
        .collect { case -\/(semErr) => semErr }.toNel

    errors.cata(
      MonadSemanticErrs[F].raiseError(_),
      MonadSemanticErrs[F].unattempt(
        expr.cataM[SemanticError \/ ?, T[Sql]](substVarsƒ(parser, variables))
          .leftMap(_.wrapNel).point[F]))
  }

  def substVarsƒ[T[_[_]]: BirecursiveT](parser: SQLParser[T], vars: Variables)
      : AlgebraM[SemanticError \/ ?, Sql, T[Sql]] = {
    case Vari(name) =>
      binding(parser, vars, VarName(name))

    case sel: Select[T[Sql]] =>
      substituteRelationVariable(sel)(v =>
        binding(parser, vars, VarName(v.symbol))).map(_.embed)

    case x => x.embed.right
  }

  ////

  private def binding[T[_[_]]](parser: SQLParser[T], vs: Variables, n: VarName): SemanticError \/ T[Sql] =
    vs.lookup(n)
      .toRightDisjunction(SemanticError.unboundVariable(n))
      .flatMap(v => parser.parseExpr(v.value).leftMap(SemanticError.VariableParseError(n, v, _)))
}
