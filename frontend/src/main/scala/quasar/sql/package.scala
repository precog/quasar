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
import quasar.common.JoinType
import quasar.fp._
import quasar.fp.ski._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._

import contextual._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import monocle.Prism
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.Liskov._

package object sql {

  type CIName = quasar.CIString

  object CIName {
    def apply(value: String): CIName =
      CIString(value)

    def unapply(name: CIName): Option[String] =
      CIString.unapply(name)
  }

  def select[A] = Prism.partial[Sql[A], (IsDistinct, List[Proj[A]], Option[SqlRelation[A]], Option[A], Option[GroupBy[A]], Option[OrderBy[A]])] {
    case Select(d, p, r, f, g, o) => (d, p, r, f, g, o)
  } ((Select[A] _).tupled)

  def vari[A] = Prism.partial[Sql[A], String] { case Vari(sym) => sym } (Vari(_))
  def setLiteral[A] = Prism.partial[Sql[A], List[A]] { case SetLiteral(exprs) => exprs } (SetLiteral(_))
  def arrayLiteral[A] = Prism.partial[Sql[A], List[A]] { case ArrayLiteral(exprs) => exprs } (ArrayLiteral(_))
  def mapLiteral[A] = Prism.partial[Sql[A], List[(A, A)]] { case MapLiteral(exprs) => exprs } (MapLiteral(_))
  def splice[A] = Prism.partial[Sql[A], Option[A]] { case Splice(exprs) => exprs } (Splice(_))
  def binop[A] = Prism.partial[Sql[A], (A, A, BinaryOperator)] { case Binop(l, r, op) => (l, r, op) } ((Binop[A] _).tupled)
  def unop[A] = Prism.partial[Sql[A], (A, UnaryOperator)] { case Unop(a, op) => (a, op) } ((Unop[A] _).tupled)
  def ident[A] = Prism.partial[Sql[A], String] { case Ident(name) => name } (Ident(_))
  def invokeFunction[A] = Prism.partial[Sql[A], (CIName, List[A])] { case InvokeFunction(name, args) => (name, args) } ((InvokeFunction[A] _).tupled)
  def matc[A] = Prism.partial[Sql[A], (A, List[Case[A]], Option[A])] { case Match(expr, cases, default) => (expr, cases, default) } ((Match[A] _).tupled)
  def switch[A] = Prism.partial[Sql[A], (List[Case[A]], Option[A])] { case Switch(cases, default) => (cases, default) } ((Switch[A] _).tupled)
  def let[A] = Prism.partial[Sql[A], (CIName, A, A)] { case Let(n, f, b) => (n, f, b) } ((Let[A](_, _, _)).tupled)
  def intLiteral[A] = Prism.partial[Sql[A], Long] { case IntLiteral(v) => v } (IntLiteral(_))
  def floatLiteral[A] = Prism.partial[Sql[A], Double] { case FloatLiteral(v) => v } (FloatLiteral(_))
  def stringLiteral[A] = Prism.partial[Sql[A], String] { case StringLiteral(v) => v } (StringLiteral(_))
  def nullLiteral[A] = Prism.partial[Sql[A], Unit] { case NullLiteral() => () } (_ => NullLiteral())
  def boolLiteral[A] = Prism.partial[Sql[A], Boolean] { case BoolLiteral(v) => v } (BoolLiteral(_))

  // NB: we need to support relative paths, including `../foo`
  type FUPath = pathy.Path[_, pathy.Path.File, pathy.Path.Unsandboxed]

  def parser[T[_[_]]: BirecursiveT] = new SQLParser[T]()

  // TODO: Get rid of this one once we’ve parameterized everything on `T`.
  val fixParser = parser[Fix]

  // Note: This is hardcoded to `Fix` but cannot be made generic with version `1.0.1` of contextual,
  //       it results in a `ClassNotFound` exception being thrown at compile time
  //       https://github.com/propensive/contextual/issues/28
  // Note: This cannot be moved to tests as it will also result in a `ClassNotFound`
  //       exception being thrown at compile time as of contextual `1.0.1`
  //       https://github.com/propensive/contextual/issues/29
  // TODO: Write custom macro to do this ourselves in order to work around above issues
  implicit class SqlStringContext(sc: StringContext) {
    val sqlE = Prefix(SqlInterpolator.Expr,       sc)
    val sqlB = Prefix(SqlInterpolator.ScopedExpr, sc)
    val sqlM = Prefix(SqlInterpolator.Module,     sc)
  }

  def CrossRelation[T]
    (left: SqlRelation[T], right: SqlRelation[T])
    (implicit T: Corecursive.Aux[T, Sql])=
    JoinRelation(left, right, JoinType.Inner, boolLiteral[T](true).embed)

  /** Returns the name of the expression when viewed as a projection
    * (of the optional relation), if available.
    */
  def projectionName[T](
    expr: T,
    relationName: Option[String]
  )(implicit
    T: Recursive.Aux[T, Sql]
  ): Option[String] = {
    val loop: T => (Option[String] \/ (Option[String] \/ T)) =
      _.project match {
        case Ident(name) if !relationName.element(name)  => some(name).left
        case Binop(_, Embed(StringLiteral(v)), KeyDeref) => some(v).left
        case Unop(arg, FlattenMapValues)                 => arg.right.right
        case Unop(arg, FlattenArrayValues)               => arg.right.right
        case _                                           => None.left
      }

    \/.loopRight(expr.right, ι[Option[String]], loop)
  }

  def projectionNames[T]
    (projections: List[Proj[T]], relName: Option[String])
    (implicit T: Recursive.Aux[T, Sql])
      : SemanticError \/ List[(String, T)] = {
    val aliases = projections.flatMap(_.alias.toList)

    (aliases diff aliases.distinct).headOption.cata[SemanticError \/ List[(String, T)]](
      duplicateAlias => SemanticError.DuplicateAlias(duplicateAlias).left,
      projections.zipWithIndex.mapAccumLeftM(aliases.toSet) { case (used, (Proj(expr, alias), index)) =>
        alias.cata(
          a => (used, a -> expr).right,
          {
            val tentativeName = projectionName(expr, relName) getOrElse index.toString
            val alternatives = Stream.from(0).map(suffix => tentativeName + suffix.toString)
            (tentativeName #:: alternatives).dropWhile(used.contains).headOption.map { name =>
              // WartRemover seems to be confused by the `+` method on `Set`
              @SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
              val newUsed = used + name
              (newUsed, name -> expr)
            } \/> SemanticError.GenericError("Could not generate alias for a relation") // unlikely since we know it's an quasi-infinite stream
          })
      }.map(_._2))
  }

  def mapPathsMƒ[F[_]: Monad](f: FUPath => F[FUPath]): Sql ~> (F ∘ Sql)#λ =
    new (Sql ~> (F ∘ Sql)#λ) {
      def apply[A](e: Sql[A]) = e match {
        case Select(d, p, relations, filt, g, o) =>
          relations.traverse(_.mapPathsM(f)).map(Select(d, p, _, filt, g, o))
        case x => x.point[F]
      }
    }

  implicit class ExprOps[T[_[_]]: BirecursiveT](q: T[Sql]) {
    def mkPathsAbsolute(basePath: ADir): T[Sql] =
      q.transCata[T[Sql]](mapPathsMƒ[Id](refineTypeAbs(_).fold(ι, pathy.Path.unsandbox(basePath) </> _)))

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def makeTables(bindings: List[String]): T[Sql] = q.project match {
      case sel @ Select(_, _, _, _, _, _) => {
        // perform all the appropriate recursions
        // TODO remove asInstanceOf by providing a `SelectF` at compile time
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val sel2 = Functor[Sql].map(sel)(_.makeTables(bindings)).asInstanceOf[Select[T[Sql]]]

        def mkRel[A](rel: SqlRelation[A]): SqlRelation[A] = rel match {
          case ir @ IdentRelationAST(name, alias) if !bindings.contains(name) => {
            val filePath = posixCodec.parsePath(Some(_),Some(_),_ => None, _ => None)(name)
            filePath.map(p => TableRelationAST[A](p, alias)).getOrElse(ir)
          }

          case JoinRelation(left, right, tpe, clause) =>
            JoinRelation(mkRel(left), mkRel(right), tpe, clause)

          case other => other
        }

        Select.relation.modify((r: Option[SqlRelation[T[Sql]]]) => r.map(mkRel(_)))(sel2).embed
      }

      case Let(ident, bindTo, in) => {
        val form2 = bindTo.makeTables(bindings)
        val body2 = in.makeTables(ident.value :: bindings)
        let(ident, form2, body2).embed
      }

      case other => other.map(_.makeTables(bindings)).embed
    }

    /**
      * Inlines all function invocations with the bodies of functions in scope.
      * Leaves invocations to functions outside of scope untouched (as opposed to erroring out)
      * @param scope Returns the list of function definitions that match a given name and function arity along with a
      *              path specifying whether this function was found
      */
    def inlineInvokes[M[_]: Monad](scope: (CIName, Int) => M[List[(FunctionDecl[T[Sql]], ADir)]]): EitherT[M, SemanticError, T[Sql]] = {
      q.cataM[EitherT[M, SemanticError, ?], T[Sql]] {
        case invoke @ InvokeFunction(name, args) =>
          EitherT(scope(name, args.size).flatMap {
            case Nil                => invoke.embed.right.point[M]
            case List((funcDef, _)) => funcDef.applyArgs(args).point[M]
            case ambiguous          =>
              SemanticError.ambiguousFunctionInvoke(name, ambiguous.map { case(func, from) => (func.name, from)}).left.point[M]
          })
        case other => EitherT.rightT(other.embed.point[M])
      }
    }
  }

  def resolveImportsImpl[M[_]: Monad, T[_[_]]: BirecursiveT](scopedExpr: ScopedExpr[T[Sql]], baseDir: ADir, retrieve: ADir => M[List[Statement[T[Sql]]]])
  : EitherT[M, SemanticError, T[Sql]] = {

    def absImport(i: Import[T[Sql]], from: ADir): SemanticError \/ ADir =
      refineTypeAbs(i.path).fold(sandboxCurrent(_), r => sandboxCurrent(unsandbox(from) </> r))
        .toRightDisjunction {
          val invalidPathString = posixCodec.unsafePrintPath(i.path)
          val fromString = posixCodec.printPath(from)
          SemanticError.GenericError(s"$invalidPathString is invalid because it is located at $fromString")
        }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def scopeFromHere(imports: List[Import[T[Sql]]], funcsHere: List[FunctionDecl[T[Sql]]], here: ADir): (CIName, Int) => EitherT[M, SemanticError, List[(FunctionDecl[T[Sql]], ADir)]] = {
      case (name, arity) =>
        imports.traverse(absImport(_, here)).fold(
          err => EitherT(err.left.point[M]),
          absImportPaths => {
            // All functions coming from `imports` along with their respective import statements and where they are defined
            val funcsFromImports = absImportPaths.traverse(d => retrieve(d).map(stats => (stats.decls, stats.imports, d)))
            // All functions in "this" scope along with their own imports
            val allFuncs = funcsFromImports.map((funcsHere, imports, here) :: _)
            EitherT.rightT(allFuncs).flatMap(_.traverse { case (funcs, imports, from) =>
              def matchesSignature(func: FunctionDecl[T[Sql]]) = func.name === name && arity === func.args.size
              funcs.filter(matchesSignature).traverse { decl =>
                val others = funcs.filterNot(matchesSignature) // No recursice calls in SQL^2 so we don't include ourselves
                val currentScope = scopeFromHere(imports, others, from)
                decl.traverse(_.mkPathsAbsolute(from).inlineInvokes(currentScope)).flattenLeft.strengthR(from)
              }
            }).map(_.join)
          }
        )
    }

    scopedExpr.expr.inlineInvokes(scopeFromHere(scopedExpr.imports, scopedExpr.defs, baseDir)).flattenLeft
  }

  implicit class StatementsOps[A](a: List[Statement[A]]) {
    def decls: List[FunctionDecl[A]] =
      a.collect { case funcDec: FunctionDecl[_] => funcDec }

    def imports: List[Import[A]] =
      a.collect { case i: Import[_] => i }

    def pprint(implicit T: Recursive.Aux[A, Sql]): String =
      a.map(st => st.map(b => sql.pprint(b)).pprint).mkString(";\n")
  }

  def pprint[T](sql: T)(implicit T: Recursive.Aux[T, Sql]) = sql.para(pprintƒ)

  private val SimpleNamePattern = "[a-zA-Z][_a-zA-Z0-9]*".r

  private def _q(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private [sql] def escape(delimiter: String, s: String): String = s match {
    case SimpleNamePattern() => s
    case _                   => delimiter + s.replace("\\", "\\\\").replace(delimiter, "\\" + delimiter) + delimiter
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def pprintRelationƒ[T]
    (r: SqlRelation[(T, String)])
    (implicit T: Recursive.Aux[T, Sql])
      : String = {

    def ppalias(a: String): String = "as " + escape("`", a)

    def ppJoinType(tpe: JoinType): String = tpe match {
      case JoinType.Inner => "inner join"
      case JoinType.FullOuter => "full join"
      case JoinType.LeftOuter => "left join"
      case JoinType.RightOuter => "right join"
    }

    (r match {
      case IdentRelationAST(name, alias) =>
        escape("`", name) :: alias.map(ppalias).toList
      case VariRelationAST(vari, alias) =>
        pprintƒ.apply(vari) :: alias.map(ppalias).toList
      case TableRelationAST(path, alias) =>
        escape("`", prettyPrint(path)) :: alias.map(ppalias).toList
      case ExprRelationAST(expr, alias) =>
        expr._2 :: alias.map(ppalias).toList
      case JoinRelation(left, right, tpe, clause) =>
        (tpe, clause._1) match {
          case (JoinType.Inner, Embed(BoolLiteral(true))) =>
            List("(", pprintRelationƒ(left), "cross join", pprintRelationƒ(right), ")")
          case (_, _) =>
            List("(", pprintRelationƒ(left), ppJoinType(tpe), pprintRelationƒ(right), "on", clause._2, ")")
        }
    }).mkString(" ")
  }

  def pprintRelation[T](r: SqlRelation[T])(implicit T: Recursive.Aux[T, Sql]) =
    pprintRelationƒ(traverseRelation[Id, T, (T, String)](r, x => (x, pprint(x))))

  def pprintƒ[T](implicit T: Recursive.Aux[T, Sql])
      : Sql[(T, String)] => String = {
    def caseSql(c: Case[(T, String)]): String =
      List("when", c.cond._2, "then", c.expr._2) mkString " "

    {
      case Select(
        isDistinct,
        projections,
        relations,
        filter,
        groupBy,
        orderBy) =>
        "(" +
        List(
          Some("select"),
          isDistinct match { case `SelectDistinct` => Some("distinct"); case _ => None },
          Some(projections.map(p => p.alias.foldLeft(p.expr._2)(_ + " as " + escape("`", _))).mkString(", ")),
          relations.map(r => "from " + pprintRelationƒ(r)),
          filter.map("where " + _._2),
          groupBy.map(g =>
            ("group by" ::
              g.keys.map(_._2).mkString(", ") ::
              g.having.map("having " + _._2).toList).mkString(" ")),
          orderBy.map(o => List("order by", o.keys.map(x => x._2._2 + " " + x._1.shows) intercalate (", ")).mkString(" "))).foldMap(_.toList).mkString(" ") +
        ")"
      case Vari(symbol) => ":" + escape("`", symbol)
      case SetLiteral(exprs) => exprs.map(_._2).mkString("(", ", ", ")")
      case ArrayLiteral(exprs) => exprs.map(_._2).mkString("[", ", ", "]")
      case MapLiteral(exprs) => exprs.map {
        case (k, v) => k._2 + ": " + v._2
      }.mkString("{", ", ", "}")
      case Splice(expr) => expr.fold("*")("(" + _._2 + ").*")
      case Binop(lhs, rhs, op) => op match {
        case KeyDeref => rhs._1.project match {
          case StringLiteral(str) => "(" + lhs._2 + ")." + escape("`", str)
          case _                   => "(" + lhs._2 + "){" + rhs._2 + "}"
        }
        case IndexDeref => "(" + lhs._2 + ")[" + rhs._2 + "]"
        case UnshiftMap => "{" + lhs._2 + " : " + rhs._2 + " ...}"
        case _ => List("((" + lhs._2 + ")", op.sql, "(" + rhs._2 + "))").mkString(" ")
      }
      case Unop(expr, op) => op match {
        case FlattenMapKeys      => "(" + expr._2 + "){*:}"
        case FlattenMapValues    => "(" + expr._2 + "){:*}"
        case ShiftMapKeys        => "(" + expr._2 + "){_:}"
        case ShiftMapValues      => "(" + expr._2 + "){:_}"
        case FlattenArrayIndices => "(" + expr._2 + ")[*:]"
        case FlattenArrayValues  => "(" + expr._2 + ")[:*]"
        case ShiftArrayIndices   => "(" + expr._2 + ")[_:]"
        case ShiftArrayValues    => "(" + expr._2 + ")[:_]"
        case UnshiftArray        => "[" + expr._2 + " ...]"
        case _ =>
          val s = List(op.sql, "(", expr._2, ")") mkString " "
          // NB: dis-ambiguates the query in case this is the leading projection
          if (op ≟ Distinct) "(" + s + ")" else s
      }
      case Ident(name) => escape("`", name)
      case InvokeFunction(name, args) =>
        (name, args) match {
          case (CIName("like"), (_, value) :: (_, pattern) :: (Embed(StringLiteral("\\")), _) :: Nil) =>
            "(" + value + ") like (" + pattern + ")"
          case (CIName("like"), (_, value) :: (_, pattern) :: (_, esc) :: Nil) =>
            "(" + value + ") like (" + pattern + ") escape (" + esc + ")"
          case _ => name.value + "(" + args.map(_._2).mkString(", ") + ")"
        }
      case Match(expr, cases, default) =>
        ("case" ::
          expr._2 ::
          ((cases.map(caseSql) ++ default.map("else " + _._2).toList) :+
            "end")).mkString(" ")
      case Switch(cases, default) =>
        ("case" ::
          ((cases.map(caseSql) ++ default.map("else " + _._2).toList) :+
            "end")).mkString(" ")
      case Let(ident, bindTo, in) =>
        ident.shows ++ " := " ++ bindTo._2 ++ "; " ++ in._2
      case IntLiteral(v) => v.toString
      case FloatLiteral(v) => v.toString
      case StringLiteral(v) => _q(v)
      case NullLiteral() => "null"
      case BoolLiteral(v) => if (v) "true" else "false"
    }
  }

  def normalizeƒ[T](implicit T: Corecursive.Aux[T, Sql]):
      Sql[T] => Option[Sql[T]] = {
    case Binop(l, r, Union) =>
      Unop(Binop(l, r, UnionAll).embed, Distinct).some
    case Binop(l, r, Intersect) =>
      Unop(Binop(l, r, IntersectAll).embed, Distinct).some
    case _ => None
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def traverseRelation[G[_], A, B](r: SqlRelation[A], f: A => G[B])(
    implicit G: Applicative[G]):
      G[SqlRelation[B]] =
    r match {
      case IdentRelationAST(name, alias) =>
        G.point(IdentRelationAST(name, alias))
      case VariRelationAST(vari, alias) =>
        G.point(VariRelationAST(Vari(vari.symbol), alias))
      case TableRelationAST(name, alias) =>
        G.point(TableRelationAST(name, alias))
      case ExprRelationAST(expr, aliasName) =>
        G.apply(f(expr))(ExprRelationAST(_, aliasName))
      case JoinRelation(left, right, tpe, clause) =>
        G.apply3(traverseRelation(left, f), traverseRelation(right, f), f(clause))(
          JoinRelation(_, _, tpe, _))
    }
}
