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
import quasar.fp._
import quasar.recursionschemes._, FunctorT.ops._
import quasar.fs._, Path._
import quasar.std._

import scala.Any
import scala.util.matching.Regex
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.input.CharArrayReader.EofCh

import scalaz._, Scalaz._

sealed trait ParsingError { def message: String}
final case class GenericParsingError(message: String) extends ParsingError
final case class ParsingPathError(error: PathError) extends ParsingError {
  def message = error.message
}

object ParsingError {
  implicit val parsingErrorShow: Show[ParsingError] =
    Show.showFromToString
}

final case class Query(value: String)

class SQLParser extends StandardTokenParsers {
  class SqlLexical extends StdLexical {
    case class QuotedIdentifier(chars: String) extends Token {
      override def toString = chars
    }
    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }
    case class Variable(chars: String) extends Token {
      override def toString = ":" + chars
    }

    override def token: Parser[Token] = variParser | numLitParser | stringLitParser | quotedIdentParser | super.token

    override protected def processIdent(name: String) =
      if (reserved contains name.toLowerCase)
        Keyword(name.toLowerCase)
      else Identifier(name)

    def identifierString: Parser[String] =
      ((letter | elem('_')) ~ rep(digit | letter | elem('_'))) ^^ {
        case x ~ xs => x.toString + xs.mkString
      }

    def variParser: Parser[Token] = ':' ~> identifierString ^^ (Variable(_))

    def numLitParser: Parser[Token] = rep1(digit) ~ opt('.' ~> rep(digit)) ~ opt((elem('e') | 'E') ~> opt(elem('-') | '+') ~ rep(digit)) ^^ {
      case i ~ None ~ None     => NumericLit(i mkString "")
      case i ~ Some(d) ~ None  => FloatLit(i.mkString("") + "." + d.mkString(""))
      case i ~ d ~ Some(s ~ e) => FloatLit(i.mkString("") + "." + d.map(_.mkString("")).getOrElse("0") + "e" + s.getOrElse("") + e.mkString(""))
    }

    def stringLitParser: Parser[Token] =
      '\'' ~> rep(chrExcept('\'') | ('\'' ~ '\'') ^^ κ('\'')) <~ '\'' ^^ ( chars => StringLit(chars.mkString) )

    def quotedIdentParser: Parser[Token] =
      '"' ~> rep(chrExcept('"') | ('"' ~ '"') ^^ κ('"')) <~ '"' ^^ (chars => QuotedIdentifier(chars.mkString))

    override def whitespace: Parser[Any] = rep(
      whitespaceChar |
      '/' ~ '*' ~ comment |
      '-' ~ '-' ~ rep(chrExcept(EofCh, '\n')) |
      '/' ~ '*' ~ failure("unclosed comment"))

    override protected def comment: Parser[Any] = (
      '*' ~ '/'  ^^ κ(' ') |
      chrExcept(EofCh) ~ comment)
  }

  override val lexical = new SqlLexical

  override def ident: Parser[String] =
    super.ident | elem("quotedIdent", _.isInstanceOf[lexical.QuotedIdentifier]) ^^ (_.chars)

  def name(name: String) =
    elem("keyword", v => v.isInstanceOf[lexical.Identifier] && v.chars.toLowerCase == name) ^^ (κ(name))

  def floatLit: Parser[String] = elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  ignore(lexical.reserved += ("true", "false", "null"))

  ignore(lexical.delimiters += (
    "*", "+", "-", "%", "^", "~~", "!~~", "~", "~*", "!~", "!~*", "||", "<", "=",
    "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ":", ";", "...",
    "{", "}", "{*}", "{:*}", "{*:}", "{_}", "{:_}", "{_:}",
    "[", "]", "[*]", "[:*]", "[*:]", "[_]", "[:_]", "[_:]"))

  override def keyword(name: String): Parser[String] =
    if (lexical.reserved.contains(name))
      elem("keyword '" + name + "'", v => v.chars == name && v.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    else failure("You are trying to parse \""+name+"\" as a keyword, but it is not contained in the reserved keywords list")

  def op(op : String): Parser[String] =
    if (lexical.delimiters.contains(op)) elem("operator '" + op + "'", v => v.chars == op && v.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    else failure("You are trying to parse \""+op+"\" as an operator, but it is not contained in the operators list")

  def select: Parser[Expr] =
    name("select") ~> opt(name("distinct")) ~ projections ~
      opt(from) ~ opt(filter) ~
      opt(group_by) ~ opt(order_by) ^^ {
        case d ~ p ~ r ~ f ~ g ~ o =>
          Select(d.map(κ(SelectDistinct)).getOrElse(SelectAll), p, r.join, f, g, o)
      }

  def delete: Parser[Expr] =
    name("delete") ~> from ~ filter ^^ { case r ~ f =>
      Select(SelectAll, List(Proj(Splice(None), None)), r, Not(f).some, None, None)
    }

  def query = delete | select

  def projections: Parser[List[Proj[Expr]]] =
    repsep(projection, op(",")).map(_.toList)

  def projection: Parser[Proj[Expr]] =
    or_expr ~ opt(name("as") ~> ident) ^^ {
      case expr ~ ident => Proj(expr, ident)
    }

  def variable: Parser[Expr] =
    elem("variable", _.isInstanceOf[lexical.Variable]) ^^ (token => Vari(token.chars))

  def command: Parser[Expr] = expr <~ opt(op(";"))

  def or_expr: Parser[Expr] = and_expr * (name("or") ^^^ Or)

  def and_expr: Parser[Expr] = cmp_expr * (name("and") ^^^ And)

  def relationalOp: Parser[BinaryOperator] =
    op("=")  ^^^ Eq  |
    op("<>") ^^^ Neq |
    op("!=") ^^^ Neq |
    op("<")  ^^^ Lt  |
    op("<=") ^^^ Le  |
    op(">")  ^^^ Gt  |
    op(">=") ^^^ Ge

  def relational_suffix: Parser[Expr => Expr] =
    relationalOp ~ default_expr ^^ {
      case op ~ rhs => Binop(_, rhs, op)
    }

  def between_suffix: Parser[Expr => Expr] =
    name("between") ~ default_expr ~ name("and") ~ default_expr ^^ {
      case _ ~ lower ~ _ ~ upper =>
        lhs => InvokeFunction(StdLib.relations.Between.name,
          List(lhs, lower, upper))
    }

  def in_suffix: Parser[Expr => Expr] =
    name("in") ~ default_expr ^^ { case _ ~ a => In(_, a) }

  private def LIKE(l: Expr, r: Expr, esc: Option[Expr]) =
    InvokeFunction(StdLib.string.Like.name,
      List(l, r, esc.getOrElse(StringLiteral("\\"))))

  def like_suffix: Parser[Expr => Expr] =
    name("like") ~ default_expr ~ opt(name("escape") ~> default_expr) ^^ {
      case _ ~ a ~ esc => LIKE(_, a, esc)
    }

  def is_suffix: Parser[Expr => Expr] =
    (name("is") ~ opt(name("not")) ~ (
      keyword("null")    ^^^ (IsNull(_))
      | keyword("true")  ^^^ ((x: Expr) => Eq(x, BoolLiteral(true)))
      | keyword("false") ^^^ ((x: Expr) => Eq(x, BoolLiteral(false)))
    )) ^^ { case _ ~ n ~ f => (x: Expr) => val u = f(x); n.fold(u)(κ(Not(u))) }

  def negatable_suffix: Parser[Expr => Expr] = {
    opt(name("not")) ~ (between_suffix | in_suffix | like_suffix) ^^ {
      case inv ~ suffix =>
        inv.fold(suffix)(κ(lhs => Not(suffix(lhs))))
    }
  }

  def array_literal: Parser[Expr] =
    (op("[") ~> repsep(expr, op(",")) <~ op("]")) ^^ (ArrayLiteral(_))

  def pair: Parser[(Expr, Expr)] = expr ~ op(":") ~ expr ^^ {
    case l ~ _ ~ r => (l,r)
  }

  def map_literal: Parser[Expr] =
    (op("{") ~> repsep(pair, op(",")) <~ op("}")) ^^ (MapLiteral(_))

  def cmp_expr: Parser[Expr] =
    default_expr ~ rep(relational_suffix | negatable_suffix | is_suffix) ^^ {
      case lhs ~ suffixes => suffixes.foldLeft(lhs)((lhs, op) => op(lhs))
    }

  /** The default precedence level, for some built-ins, and all user-defined */
  def default_expr: Parser[Expr] =
    concat_expr * (
      op("~") ^^^ ((l: Expr, r: Expr) =>
        InvokeFunction(StdLib.string.Search.name,
          List(l, r, BoolLiteral(false)))) |
        op("~*") ^^^ ((l: Expr, r: Expr) =>
          InvokeFunction(StdLib.string.Search.name,
            List(l, r, BoolLiteral(true)))) |
        op("!~") ^^^ ((l: Expr, r: Expr) =>
          Not(InvokeFunction(StdLib.string.Search.name,
            List(l, r, BoolLiteral(false))))) |
        op("!~*") ^^^ ((l: Expr, r: Expr) =>
          Not(InvokeFunction(StdLib.string.Search.name,
            List(l, r, BoolLiteral(true))))) |
        op("~~") ^^^ ((l: Expr, r: Expr) => LIKE(l, r, None)) |
        op("!~~") ^^^ ((l: Expr, r: Expr) => Not(LIKE(l, r, None))))

  def concat_expr: Parser[Expr] =
    add_expr * (op("||") ^^^ Concat)

  def add_expr: Parser[Expr] =
    mult_expr * (op("+") ^^^ Plus | op("-") ^^^ Minus)

  def mult_expr: Parser[Expr] =
    pow_expr * (op("*") ^^^ Mult | op("/") ^^^ Div | op("%") ^^^ Mod)

  def pow_expr: Parser[Expr] =
    deref_expr * (op("^") ^^^ Pow)

  sealed trait DerefType
  case class ObjectDeref(expr: Expr) extends DerefType
  case class ArrayDeref(expr: Expr) extends DerefType
  case class DimChange(unop: UnaryOperator) extends DerefType

  def unshift_expr: Parser[Expr] =
    op("{") ~> expr <~ op("...") <~ op("}") ^^ UnshiftMap |
      op("[") ~> expr <~ op("...") <~ op("]") ^^ UnshiftArray

  def deref_expr: Parser[Expr] = primary_expr ~ (rep(
    (op(".") ~> (
      (ident ^^ (StringLiteral(_))) ^^ (ObjectDeref(_))))         |
      op("{*:}")               ^^^ DimChange(FlattenMapKeys)      |
      (op("{*}") | op("{:*}")) ^^^ DimChange(FlattenMapValues)    |
      op("{_:}")               ^^^ DimChange(ShiftMapKeys)        |
      (op("{_}") | op("{:_}")) ^^^ DimChange(ShiftMapValues)      |
      (op("{") ~> (expr ^^ (ObjectDeref(_))) <~ op("}"))          |
      op("[*:]")               ^^^ DimChange(FlattenArrayIndices) |
      (op("[*]") | op("[:*]")) ^^^ DimChange(FlattenArrayValues)  |
      op("[_:]")               ^^^ DimChange(ShiftArrayIndices)   |
      (op("[_]") | op("[:_]")) ^^^ DimChange(ShiftArrayValues)    |
      (op("[") ~> (expr ^^ (ArrayDeref(_))) <~ op("]"))
    ): Parser[List[DerefType]]) ~ opt(op(".") ~> wildcard) ^^ {
    case lhs ~ derefs ~ wild =>
      wild.foldLeft(derefs.foldLeft[Expr](lhs)((lhs, deref) => deref match {
        case DimChange(unop)           => Unop(lhs, unop)
        case ObjectDeref(Splice(None)) => FlattenMapValues(lhs)
        case ObjectDeref(rhs)          => FieldDeref(lhs, rhs)
        case ArrayDeref(Splice(None))  => FlattenArrayValues(lhs)
        case ArrayDeref(rhs)           => IndexDeref(lhs, rhs)
      }))((lhs, rhs) => Splice(Some(lhs)))
  }

  def unary_operator: Parser[UnaryOperator] =
    op("+")           ^^^ Positive |
    op("-")           ^^^ Negative |
    name("distinct") ^^^ Distinct

  def wildcard: Parser[Expr] = op("*") ^^^ Splice(None)

  def primary_expr: Parser[Expr] =
    case_expr |
    unshift_expr |
    op("(") ~> repsep(expr, op(",")) <~ op(")") ^^ {
      case Nil      => SetLiteral(Nil)
      case x :: Nil => x
      case xs       => SetLiteral(xs)
    } |
    unary_operator ~ primary_expr ^^ {
      case op ~ expr => op(expr)
    } |
    name("not")        ~> cmp_expr ^^ Not |
    name("exists")     ~> cmp_expr ^^ Exists |
    ident ~ (op("(") ~> repsep(expr, op(",")) <~ op(")")) ^^ {
      case a ~ xs => InvokeFunction(a, xs)
    } |
    variable |
    literal |
    wildcard |
    array_literal |
    map_literal |
    ident ^^ (Ident(_))

  def cases: Parser[List[Case[Expr]] ~ Option[Expr]] =
    rep1(name("when") ~> expr ~ name("then") ~ expr ^^ { case a ~ _ ~ b => Case(a, b) }) ~
      opt(name("else") ~> expr) <~ name("end")

  def case_expr: Parser[Expr] =
    name("case") ~> cases ^^ {
      case cases ~ default => Switch(cases, default)
    } |
      name("case") ~> expr ~ cases ^^ {
        case e ~ (cases ~ default) => Match(e, cases, default)
      }

  def literal: Parser[Expr] =
    numericLit ^^ { case i => IntLiteral(i.toLong) } |
    floatLit ^^ { case f => FloatLiteral(f.toDouble) } |
    stringLit ^^ { case s => StringLiteral(s) } |
    keyword("null") ^^^ NullLiteral() |
    keyword("true") ^^^ BoolLiteral(true) |
    keyword("false") ^^^ BoolLiteral(false)

  def from: Parser[Option[SqlRelation[Expr]]] =
    name("from") ~> relations

  def relations: Parser[Option[SqlRelation[Expr]]] =
    rep1sep(relation, op(",")).map(_.foldLeft[Option[SqlRelation[Expr]]](None) {
      case (None, traverse) => Some(traverse)
      case (Some(acc), traverse) => Some(CrossRelation(acc, traverse))
    })

  def std_join_relation: Parser[SqlRelation[Expr] => SqlRelation[Expr]] =
    opt(join_type) ~ name("join") ~ simple_relation ~ name("on") ~ expr ^^
      { case tpe ~ _ ~ r2 ~ _ ~ e => r1 => JoinRelation(r1, r2, tpe.getOrElse(InnerJoin), e) }

  def cross_join_relation: Parser[SqlRelation[Expr] => SqlRelation[Expr]] =
    name("cross") ~> name("join") ~> simple_relation ^^ {
      case r2 => r1 => CrossRelation(r1, r2)
    }

  def relation: Parser[SqlRelation[Expr]] =
    simple_relation ~ rep(std_join_relation | cross_join_relation) ^^ {
      case r ~ fs => fs.foldLeft(r) { case (r, f) => f(r) }
    }

  def join_type: Parser[JoinType] =
    (name("left") | name("right") | name("full")) ~ opt(name("outer")) ^^ {
      case "left" ~ o  => LeftJoin
      case "right" ~ o => RightJoin
      case "full" ~ o => FullJoin
    } | name("inner") ^^^ (InnerJoin)

  def simple_relation: Parser[SqlRelation[Expr]] =
    ident ~ opt(name("as") ~> ident) ^^ {
      case ident ~ alias => TableRelationAST[Expr](ident, alias)
    } |
    op("(") ~> (
      (expr ~ op(")") ~ name("as") ~ ident ^^ {
        case expr ~ _ ~ _ ~ alias => ExprRelationAST(expr, alias)
      }) |
      relation <~ op(")"))

  def filter: Parser[Expr] = name("where") ~> or_expr

  def group_by: Parser[GroupBy[Expr]] =
    name("group") ~> name("by") ~> rep1sep(or_expr, op(",")) ~ opt(name("having") ~> or_expr) ^^ {
      case k ~ h => GroupBy(k, h)
    }

  def order_by: Parser[OrderBy[Expr]] =
    name("order") ~> name("by") ~> rep1sep(or_expr ~ opt(name("asc") | name("desc")) ^^ {
      case i ~ (Some("asc") | None) => (ASC, i)
      case i ~ Some("desc") => (DESC, i)
    }, op(",")) ^^ (OrderBy(_))

  def expr: Parser[Expr] =
    (query | or_expr) * (
      name("limit")                     ^^^ Limit        |
        name("offset")                  ^^^ Offset       |
        name("union") ~ name("all")     ^^^ UnionAll     |
        name("union")                   ^^^ Union        |
        name("intersect") ~ name("all") ^^^ IntersectAll |
        name("intersect")               ^^^ Intersect    |
        name("except")                  ^^^ Except)

  private def stripQuotes(s:String) = s.substring(1, s.length-1)

  def parseExpr(exprSql: String): ParsingError \/ Expr =
    phrase(command)(new lexical.Scanner(exprSql)) match {
      case Success(r, q)        => \/.right(r)
      case Error(msg, input)    => \/.left(GenericParsingError(msg))
      case Failure(msg, input)  => \/.left(GenericParsingError(msg + "; " + input.first))
    }

  def parse(sql: Query): ParsingError \/ Expr = parseExpr(sql.value)
}

object SQLParser {
  def parseInContext(sql: Query, basePath: Path):
      ParsingError \/ Expr =
    new SQLParser().parse(sql)
      .flatMap(relativizePaths(_, basePath).bimap(
        ParsingPathError,
        _.transAna(repeatedly(normalizeƒ))))
}
