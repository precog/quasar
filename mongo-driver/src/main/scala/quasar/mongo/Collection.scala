package quasar.mongo

import quasar.Predef._

import scala.util.parsing.combinator.RegexParsers
import scalaz.{\/-, -\/, \/}

final case class Collection(databaseName: String, collectionName: String)

object Collection {
  trait BaseParser extends RegexParsers {
    override def skipWhitespace = false

    protected def substitute(pairs: List[(String, String)]): Parser[String] =
      pairs.foldLeft[Parser[String]](failure("no match")) {
        case (acc, (a, b)) => (a ^^ κ(b)) | acc
      }
  }

  def utf8length(str: String) = str.getBytes("UTF-8").length

  val DatabaseNameEscapes = List(
    " "  -> "+",
    "."  -> "~",
    "$"  -> "$$",
    "+"  -> "$add",
    "~"  -> "$tilde",
    "/"  -> "$div",
    "\\" -> "$esc",
    "\"" -> "$quot",
    "*"  -> "$mul",
    "<"  -> "$lt",
    ">"  -> "$gt",
    ":"  -> "$colon",
    "|"  -> "$bar",
    "?"  -> "$qmark")

  type ParseError = String

  object DatabaseNameParser extends BaseParser {
    def name: Parser[String] =
      char.* ^^ { _.mkString }

    def char: Parser[String] = substitute(DatabaseNameEscapes) | "(?s).".r

    def apply(input: String): ParseError \/ String = parseAll(name, input) match {
      case Success(name, _) =>
        if (utf8length(name) > 64)
          -\/("database name too long (> 64 bytes): " + name)
        else \/-(name)
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  object DatabaseNameUnparser extends BaseParser {
    def name = nameChar.* ^^ { _.mkString }

    def nameChar = substitute(DatabaseNameEscapes.map(_.swap)) | "(?s).".r

    def apply(input: String): String = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  val CollectionNameEscapes = List(
    "."  -> "\\.",
    "$"  -> "\\d",
    "\\" -> "\\\\")

  object CollectionSegmentParser extends BaseParser {
    def seg: Parser[String] =
      char.* ^^ { _.mkString }

    def char: Parser[String] = substitute(CollectionNameEscapes) | "(?s).".r

    def apply(input: String): String = parseAll(seg, input) match {
      case Success(seg, _) => seg
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  object CollectionNameUnparser extends BaseParser {
    def name = repsep(seg, ".")

    def seg = segChar.* ^^ { _.mkString }

    def segChar = substitute(CollectionNameEscapes.map(_.swap)) | "(?s)[^.]".r

    def apply(input: String): List[String] = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }
}
