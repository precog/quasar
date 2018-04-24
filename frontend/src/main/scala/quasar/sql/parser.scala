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

package quasar.sql

import slamdata.Predef._
import quasar.common.JoinType
import quasar.fp.ski._
import quasar.fp._

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.input.CharArrayReader.EofCh
import matryoshka._
import matryoshka.implicits._
import pathy.Path
import pathy.Path._

import scalaz._
import Scalaz._

sealed abstract class DerefType[T[_[_]]] extends Product with Serializable
final case class MapDeref[T[_[_]]](expr: T[Sql])         extends DerefType[T]
final case class ArrayDeref[T[_[_]]](expr: T[Sql])       extends DerefType[T]
final case class DimChange[T[_[_]]](unop: UnaryOperator) extends DerefType[T]

private[sql] class SQLParser[T[_[_]]: BirecursiveT]
    extends StandardTokenParsers {
  class SqlLexical extends StdLexical with RegexParsers {
    override type Elem = super.Elem

    case class QuotedIdentifier(chars: String) extends Token {
      override def toString = chars
    }
    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }
    case class Variable(chars: String) extends Token {
      override def toString = ":" + chars
    }

    override def token: Parser[Token] =
      variParser |
      numLitParser |
      charLitParser |
      stringLitParser |
      identParser |
      EofCh ^^^ EOF |
      '\'' ~> failure("unclosed character literal") |
      '"'  ~> failure("unclosed string literal") |
      '`'  ~> failure("unclosed quoted identifier") |
      delim

    def identifierString: Parser[String] =
      letter ~ rep(digit | letter | elem('_')) ^^ {
        case x ~ xs => (x :: xs).mkString
      }

    def variParser: Parser[Token] = ':' ~> identParser ^^ (t => Variable(t.chars))

    def numLitParser: Parser[Token] = rep1(digit) ~ opt('.' ~> rep(digit)) ~ opt((elem('e') | 'E') ~> opt(elem('-') | '+') ~ rep(digit)) ^^ {
      case i ~ None ~ None     => NumericLit(i mkString "")
      case i ~ Some(d) ~ None  => FloatLit(i.mkString("") + "." + d.mkString(""))
      case i ~ d ~ Some(s ~ e) => FloatLit(i.mkString("") + "." + d.map(_.mkString("")).getOrElse("0") + "e" + s.map(_.toString).getOrElse("") + e.mkString(""))
    }

    val hexDigit: Parser[String] = """[0-9a-fA-F]""".r

    def char(delim: Char): Parser[Char] =
      chrExcept('\\', delim) |
        ('\\' ~>
          ('u' ~> repN(4, hexDigit) ^^
            (x => java.lang.Integer.parseInt(x.mkString, 16).toChar) |
            chrExcept(EofCh)))

    def charLitParser: Parser[Token] =
      '\'' ~> char('\'') <~ '\'' ^^ (c => StringLit(c.toString))

    def delimitedString(delim: Char): Parser[String] =
      delim ~> rep(char(delim)) <~ delim ^^ (_.mkString)

    def stringLitParser: Parser[Token] = delimitedString('"') ^^ (StringLit(_))

    def quotedIdentParser: Parser[Token] =
      delimitedString('`') ^^ (QuotedIdentifier(_))

    // NB: `Token`, which we do not control, causes this.
    @SuppressWarnings(Array(
      "org.wartremover.warts.Product",
      "org.wartremover.warts.Serializable"))
    def identParser: Parser[Token] =
      quotedIdentParser | identifierString ^^ processIdent

    override def whitespace: Parser[scala.Any] = rep(
      whitespaceChar |
      '/' ~ '*' ~ comment |
      '-' ~ '-' ~ rep(chrExcept(EofCh, '\n')) |
      '/' ~ '*' ~ failure("unclosed comment"))

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    override protected def comment: Parser[scala.Any] = (
      '*' ~ '/'  ^^ κ(' ') |
      chrExcept(EofCh) ~ comment)
  }

  override val lexical = new SqlLexical

  def floatLit: Parser[String] = elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  ignore(lexical.delimiters += (
    "*", "+", "-", "%", "^", "~~", "!~~", "~", "~*", "!~", "!~*", "||", "<", "=",
    "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ":", "??", ";", "..", "...",
    "{", "}", "{*}", "{:*}", "{*:}", "{_}", "{:_}", "{_:}",
    "[", "]", "[*]", "[:*]", "[*:]", "[_]", "[:_]", "[_:]", ":="))

  override def keyword(name: String): Parser[String] =
    elem(
      "keyword '" + name + "'",
      {
        case lexical.Identifier(chars) => chars.toLowerCase ≟ name
        case _                         => false
      }) ^^ (κ(name))

  override def ident: Parser[String] =
    super.ident |
      elem(
        "quotedIdent",
        { case lexical.QuotedIdentifier(_) => true; case _ => false }) ^^ (_.chars)

  def op(op : String): Parser[String] =
    if (lexical.delimiters.contains(op))
      elem(
        "operator '" + op + "'",
        { case lexical.Keyword(chars) => chars ≟ op; case _ => false }) ^^ (_.chars)
    else failure("You are trying to parse \""+op+"\" as an operator, but it is not contained in the operators list")

  def let_expr: Parser[T[Sql]] =
    ident ~ (op(":=") ~> expr) ~ (op(";") ~> expr) ^^ {
      case i ~ e ~ b => let(CIName(i), e, b).embed
    } | query_expr

  def func_def: Parser[FunctionDecl[T[Sql]]] =
    keyword("create") ~> keyword("function") ~> ident ~ (op("(") ~> repsep(variable, op(",")) <~ op(")")) ~
      (keyword("begin") ~> expr <~ keyword("end")) ^^ {
      case i ~ vars ~ expr => FunctionDecl[T[Sql]](CIName(i), vars.map(v => CIName(v.symbol)), expr)
    }

  def import_ : Parser[Import[T[Sql]]] =
    keyword("import") ~> ident >> {
      case i => posixCodec.parsePath[Option[Path[Any, Dir, Unsandboxed]]](κ(none), κ(none), Some(_), Some(_))(i).cata(
        path => success(Import(path)),
        failure("Import must identify a directory"))
    }

  def statements: Parser[List[Statement[T[Sql]]]] =
    repsep(func_def | import_, op(";")) <~ opt(op(";"))

  def scopedExpr: Parser[ScopedExpr[T[Sql]]] =
    statements ~ expr ^^ {
      case stats ~ expr => ScopedExpr(expr, stats)
    }

  def block: Parser[Block[T[Sql]]] =
    opt(repsep(func_def, op(";")) <~ op(";")) ~ expr ^^ {
      case defs ~ expr => Block(expr, defs.getOrElse(Nil))
    }

  def select_expr: Parser[T[Sql]] =
    keyword("select") ~> opt(keyword("distinct")) ~ projections ~
      opt(from) ~ opt(filter) ~
      opt(group_by) ~ opt(order_by) ^^ {
        case d ~ p ~ r ~ f ~ g ~ o =>
          select(d.map(κ(SelectDistinct)).getOrElse(SelectAll), p, r.join, f, g, o).embed
      }

  def delete: Parser[T[Sql]] =
    keyword("delete") ~> from ~ filter ^^ { case r ~ f =>
      select(
        SelectAll,
        List(Proj(splice[T[Sql]](None).embed, None)),
        r,
        Not(f).embed.some,
        None, None).embed
    }

  def insert: Parser[T[Sql]] =
    keyword("insert") ~> keyword("into") ~> relations ~ (
      keyword("values") ~> rep1sep(defined_expr, op(",")) ^^ (setLiteral(_).embed) |
        paren_list ~ keyword("values") ~ rep1sep(paren_list, op(",")) ^^ {
          case keys ~ _ ~ valueses =>
            setLiteral(valueses ∘ (vs => mapLiteral(keys zip vs).embed)).embed
        }) ^^ {
      case orig ~ inserted =>
        Union(
          inserted,
          select(
            SelectAll,
            List(Proj(splice[T[Sql]](None).embed, None)),
            orig,
            None, None, None).embed).embed
    }

  def query = delete | insert | select_expr

  def projections: Parser[List[Proj[T[Sql]]]] =
    repsep(projection, op(",")).map(_.toList)

  def projection: Parser[Proj[T[Sql]]] =
    defined_expr ~ opt(keyword("as") ~> ident) ^^ {
      case expr ~ ident => Proj(expr, ident)
    }

  def variable: Parser[Vari[T[Sql]]] =
    elem("variable", _.isInstanceOf[lexical.Variable]) ^^ (token => Vari[T[Sql]](token.chars))

  def defined_expr: Parser[T[Sql]] =
    range_expr * (op("??") ^^^ (IfUndefined(_: T[Sql], _: T[Sql]).embed))

  def range_expr: Parser[T[Sql]] =
    or_expr * (op("..") ^^^ (Range(_: T[Sql], _: T[Sql]).embed))

  def or_expr: Parser[T[Sql]] =
    and_expr * (keyword("or") ^^^ (Or(_: T[Sql], _: T[Sql]).embed))

  def and_expr: Parser[T[Sql]] =
    cmp_expr * (keyword("and") ^^^ (And(_: T[Sql], _: T[Sql]).embed))

  def relationalOp: Parser[BinaryOperator] =
    op("=")  ^^^ Eq  |
    op("<>") ^^^ Neq |
    op("!=") ^^^ Neq |
    op("<")  ^^^ Lt  |
    op("<=") ^^^ Le  |
    op(">")  ^^^ Gt  |
    op(">=") ^^^ Ge  |
    keyword("is") ~> opt(keyword("not")) ^^ (_.fold[BinaryOperator](Eq)(κ(Neq)))

  def relational_suffix: Parser[T[Sql] => T[Sql]] =
    relationalOp ~ default_expr ^^ {
      case op ~ rhs => binop(_, rhs, op).embed
    }

  def between_suffix: Parser[T[Sql] => T[Sql]] =
    keyword("between") ~ default_expr ~ keyword("and") ~ default_expr ^^ {
      case kw ~ lower ~ _ ~ upper =>
        lhs => invokeFunction(CIName(kw), List(lhs, lower, upper)).embed
    }

  def in_suffix: Parser[T[Sql] => T[Sql]] =
    keyword("in") ~ default_expr ^^ { case _ ~ a => In(_, a).embed }

  private def LIKE(l: T[Sql], r: T[Sql], esc: Option[T[Sql]]) =
    invokeFunction(CIName("like"),
      List(l, r, esc.getOrElse(stringLiteral[T[Sql]]("\\").embed))).embed

  def like_suffix: Parser[T[Sql] => T[Sql]] =
    keyword("like") ~ default_expr ~ opt(keyword("escape") ~> default_expr) ^^ {
      case _ ~ a ~ esc => LIKE(_, a, esc)
    }

  def negatable_suffix: Parser[T[Sql] => T[Sql]] = {
    opt(keyword("is")) ~> opt(keyword("not")) ~
      (between_suffix | in_suffix | like_suffix) ^^ {
        case inv ~ suffix => inv.fold(suffix)(κ(lhs => Not(suffix(lhs)).embed))
    }
  }

  def array_literal: Parser[T[Sql]] =
    (op("[") ~> repsep(expr, op(",")) <~ op("]")) ^^ (arrayLiteral(_).embed)

  def pair: Parser[(T[Sql], T[Sql])] = expr ~ (op(":") ~> expr) ^^ {
    case l ~ r => (l, r)
  }

  def map_literal: Parser[T[Sql]] =
    (op("{") ~> repsep(pair, op(",")) <~ op("}")) ^^ (mapLiteral(_).embed)

  def cmp_expr: Parser[T[Sql]] =
    default_expr ~ rep(negatable_suffix | relational_suffix) ^^ {
      case lhs ~ suffixes => suffixes.foldLeft(lhs)((lhs, op) => op(lhs))
    }

  /** The default precedence level, for some built-ins, and all user-defined */
  def default_expr: Parser[T[Sql]] =
    concat_expr * (
      op("~") ^^^ ((l: T[Sql], r: T[Sql]) =>
        invokeFunction(CIName("search"),
          List(l, r, boolLiteral[T[Sql]](false).embed)).embed) |
        op("~*") ^^^ ((l: T[Sql], r: T[Sql]) =>
          invokeFunction(CIName("search"),
            List(l, r, boolLiteral[T[Sql]](true).embed)).embed) |
        op("!~") ^^^ ((l: T[Sql], r: T[Sql]) =>
          Not(invokeFunction(CIName("search"),
            List(l, r, boolLiteral[T[Sql]](false).embed)).embed).embed) |
        op("!~*") ^^^ ((l: T[Sql], r: T[Sql]) =>
          Not(invokeFunction(CIName("search"),
            List(l, r, boolLiteral[T[Sql]](true).embed)).embed).embed) |
        op("~~") ^^^ ((l: T[Sql], r: T[Sql]) => LIKE(l, r, None)) |
        op("!~~") ^^^ ((l: T[Sql], r: T[Sql]) => Not(LIKE(l, r, None)).embed))

  def concat_expr: Parser[T[Sql]] =
    add_expr * (op("||") ^^^ (Concat(_: T[Sql], _: T[Sql]).embed))

  def add_expr: Parser[T[Sql]] =
    mult_expr * (
      op("+") ^^^ (Plus(_: T[Sql], _: T[Sql]).embed) |
      op("-") ^^^ (Minus(_: T[Sql], _: T[Sql]).embed))

  def mult_expr: Parser[T[Sql]] =
    pow_expr * (
      op("*") ^^^ (Mult(_: T[Sql], _: T[Sql]).embed) |
      op("/") ^^^ (Div(_: T[Sql], _: T[Sql]).embed) |
      op("%") ^^^ (Mod(_: T[Sql], _: T[Sql]).embed))

  def pow_expr: Parser[T[Sql]] =
    deref_expr * (op("^") ^^^ (Pow(_: T[Sql], _: T[Sql]).embed))

  def unshift_expr: Parser[T[Sql]] =
    op("[") ~> expr <~ op("...") <~ op("]") ^^ (UnshiftArray(_).embed) |
    op("{") ~ expr ~ op(":") ~ expr <~ op("...") <~ op("}") ^^ {
      case _ ~ k ~ _ ~ v => UnshiftMap(k, v).embed
    }

  def deref_expr: Parser[T[Sql]] = primary_expr ~ (rep(
    (op(".") ~> (
      (ident ^^ (stringLiteral[T[Sql]](_).embed)) ^^ (MapDeref(_)))) |
      op("{*:}")               ^^^ DimChange[T](FlattenMapKeys)      |
      (op("{*}") | op("{:*}")) ^^^ DimChange[T](FlattenMapValues)    |
      op("{_:}")               ^^^ DimChange[T](ShiftMapKeys)        |
      (op("{_}") | op("{:_}")) ^^^ DimChange[T](ShiftMapValues)      |
      (op("{") ~> (expr ^^ (MapDeref(_))) <~ op("}"))                |
      op("[*:]")               ^^^ DimChange[T](FlattenArrayIndices) |
      (op("[*]") | op("[:*]")) ^^^ DimChange[T](FlattenArrayValues)  |
      op("[_:]")               ^^^ DimChange[T](ShiftArrayIndices)   |
      (op("[_]") | op("[:_]")) ^^^ DimChange[T](ShiftArrayValues)    |
      (op("[") ~> (expr ^^ (ArrayDeref(_))) <~ op("]"))
    ): Parser[List[DerefType[T]]]) ~ opt(op(".") ~> wildcard) ^^ {
    case lhs ~ derefs ~ wild =>
      wild.foldLeft(derefs.foldLeft[T[Sql]](lhs)((lhs, deref) => (deref match {
        case DimChange(unop)  => Unop(lhs, unop)
        case MapDeref(rhs) => KeyDeref(lhs, rhs)
        case ArrayDeref(rhs)  => IndexDeref(lhs, rhs)
      }).embed))((lhs, rhs) => splice(lhs.some).embed)
  }

  def unary_operator: Parser[UnaryOperator] =
    op("+")             ^^^ Positive |
    op("-")             ^^^ Negative |
    keyword("distinct") ^^^ Distinct |
    keyword("not")      ^^^ Not |
    keyword("exists")   ^^^ Exists


  def wildcard: Parser[T[Sql]] = op("*") ^^^ splice[T[Sql]](None).embed

  def paren_list: Parser[List[T[Sql]]] = op("(") ~> repsep(expr, op(",")) <~ op(")")

  def function_expr: Parser[T[Sql]] =
    ident ~ paren_list ^^ { case a ~ xs => invokeFunction(CIName(a), xs).embed }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def primary_expr: Parser[T[Sql]] =
    case_expr |
    unshift_expr |
    paren_list ^^ {
      case Nil      => setLiteral[T[Sql]](Nil).embed
      case x :: Nil => x
      case xs       => setLiteral(xs).embed
    } |
    unary_operator ~ primary_expr ^^ {
      case op ~ expr => op(expr).embed
    } |
    function_expr |
    variable.map(_.embed) |
    literal |
    wildcard |
    array_literal |
    map_literal |
    ident ^^ (quasar.sql.ident[T[Sql]](_).embed)

  def cases: Parser[List[Case[T[Sql]]] ~ Option[T[Sql]]] =
    rep1(keyword("when") ~> expr ~ keyword("then") ~ expr ^^ { case a ~ _ ~ b => Case(a, b) }) ~
      opt(keyword("else") ~> expr) <~ keyword("end")

  def case_expr: Parser[T[Sql]] =
    keyword("case") ~> cases ^^ {
      case cases ~ default => switch(cases, default).embed
    } |
      keyword("case") ~> expr ~ cases ^^ {
        case e ~ (cases ~ default) => matc(e, cases, default).embed
      }

  def literal: Parser[T[Sql]] =
    numericLit        ^^ (i => intLiteral[T[Sql]](i.toLong).embed) |
    floatLit          ^^ (f => floatLiteral[T[Sql]](f.toDouble).embed) |
    stringLit         ^^ (stringLiteral[T[Sql]](_).embed) |
    keyword("null")  ^^^ nullLiteral[T[Sql]]().embed |
    keyword("true")  ^^^ boolLiteral[T[Sql]](true).embed |
    keyword("false") ^^^ boolLiteral[T[Sql]](false).embed

  def from: Parser[Option[SqlRelation[T[Sql]]]] =
    keyword("from") ~> relations

  def relations: Parser[Option[SqlRelation[T[Sql]]]] =
    rep1sep(relation, op(",")).map(_.foldLeft[Option[SqlRelation[T[Sql]]]](None) {
      case (None, traverse) => Some(traverse)
      case (Some(acc), traverse) => Some(CrossRelation[T[Sql]](acc, traverse))
    })

  def std_join_relation: Parser[SqlRelation[T[Sql]] => SqlRelation[T[Sql]]] =
    opt(join_type) ~ keyword("join") ~ simple_relation ~ keyword("on") ~ defined_expr ^^
      { case tpe ~ _ ~ r2 ~ _ ~ e => r1 => JoinRelation(r1, r2, tpe.getOrElse(JoinType.Inner), e) }

  def cross_join_relation: Parser[SqlRelation[T[Sql]] => SqlRelation[T[Sql]]] =
    keyword("cross") ~> keyword("join") ~> simple_relation ^^ {
      case r2 => r1 => CrossRelation[T[Sql]](r1, r2)
    }

  def relation: Parser[SqlRelation[T[Sql]]] =
    simple_relation ~ rep(std_join_relation | cross_join_relation) ^^ {
      case r ~ fs => fs.foldLeft(r) { case (r, f) => f(r) }
    }

  def join_type: Parser[JoinType] =
    (keyword("left") | keyword("right") | keyword("full")) <~ opt(keyword("outer")) ^^ {
      case "left"  => JoinType.LeftOuter
      case "right" => JoinType.RightOuter
      case "full"  => JoinType.FullOuter
    } | keyword("inner") ^^^ (JoinType.Inner)

  def simple_relation: Parser[SqlRelation[T[Sql]]] =
    ident ~ opt(keyword("as") ~> ident) ^^ {
      case ident ~ alias =>
        IdentRelationAST[T[Sql]](ident, alias)
    } |
    variable ~ opt(keyword("as") ~> ident) ^^ {
      case vari ~ alias =>
        VariRelationAST[T[Sql]](vari, alias)
    } |
    op("(") ~> (
      (expr ~ op(")") ~ opt(keyword("as") ~> ident) ^^ {
        case expr ~ _ ~ alias => ExprRelationAST[T[Sql]](expr, alias)
      }) |
      relation <~ op(")"))

  def filter: Parser[T[Sql]] = keyword("where") ~> defined_expr

  def group_by: Parser[GroupBy[T[Sql]]] =
    keyword("group") ~> keyword("by") ~> rep1sep(defined_expr, op(",")) ~ opt(keyword("having") ~> defined_expr) >> {
      case k ~ None => success(GroupBy(k, None))
      case _ ~ Some(_) => failure("Keyword `having` not supported.") // TODO remove failure when qz-3686 is fixed
    }

  def order_by: Parser[OrderBy[T[Sql]]] = {
    def asOrdering[I]: PartialFunction[I ~ Option[String], (OrderType, I)] = {
      case i ~ (Some("asc") | None) => (ASC, i)
      case i ~ Some("desc") => (DESC, i)
    }
    keyword("order") ~> keyword("by") ~> rep1sep(
      defined_expr ~ opt(keyword("asc") | keyword("desc")) ^^ asOrdering,
      op(",")) ^? { case o :: os => OrderBy(NonEmptyList(o, os: _*)) }
  }

  def expr: Parser[T[Sql]] = let_expr

  def query_expr: Parser[T[Sql]] =
    (query | defined_expr) * (
      keyword("limit")                        ^^^ (Limit(_: T[Sql], _: T[Sql]).embed)        |
        keyword("offset")                     ^^^ (Offset(_: T[Sql], _: T[Sql]).embed)       |
        keyword("sample")                     ^^^ (Sample(_: T[Sql], _: T[Sql]).embed)       |
        keyword("union") ~ keyword("all")     ^^^ (UnionAll(_: T[Sql], _: T[Sql]).embed)     |
        keyword("union")                      ^^^ (Union(_: T[Sql], _: T[Sql]).embed)        |
        keyword("intersect") ~ keyword("all") ^^^ (IntersectAll(_: T[Sql], _: T[Sql]).embed) |
        keyword("intersect")                  ^^^ (Intersect(_: T[Sql], _: T[Sql]).embed)    |
        keyword("except")                     ^^^ (Except(_: T[Sql], _: T[Sql]).embed))

  def parseWithParser[A](input: String, parser: Parser[A]): ParsingError \/ A =
    phrase(parser)(new lexical.Scanner(input)) match {
      case Success(r, q)        => \/.right(r)
      case Error(msg, input)    => \/.left(GenericParsingError(msg))
      case Failure(msg, input)  => \/.left(GenericParsingError(s"$msg; but found `${input.first.chars}'"))
    }

  val parse: String => ParsingError \/ ScopedExpr[T[Sql]] = query =>
    parseScopedExpr(query)

  def parseScopedExpr(scopedExprString: String): ParsingError \/ ScopedExpr[T[Sql]] =
    parseWithParser(scopedExprString, scopedExpr).map(_.map(normalize))

  def parseModule(moduleString: String): ParsingError \/ List[Statement[T[Sql]]] =
    parseWithParser(moduleString, statements).map(_.map(_.map(normalize)))

  def parseBlock(blockString: String): ParsingError \/ Block[T[Sql]] =
    parseWithParser(blockString, block).map(_.map(normalize))

  val parseExpr: String => ParsingError \/ T[Sql] = query =>
    parseWithParser(query, expr).map(normalize)

  private def normalize: T[Sql] => T[Sql] = _.transAna[T[Sql]](repeatedly(normalizeƒ)).makeTables(Nil)
}
