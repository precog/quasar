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

package quasar.std

import quasar.Predef._
import quasar.{Data, Func, LogicalPlan, Type, Mapping, SemanticError}, LogicalPlan._, SemanticError._
import quasar.fp._

import matryoshka._
import scalaz._, Scalaz._, Validation.{success, failureNel}

trait StringLib extends Library {
  private def stringApply(f: (String, String) => String): Func.Typer =
    partialTyper {
      case Type.Const(Data.Str(a)) :: Type.Const(Data.Str(b)) :: Nil => Type.Const(Data.Str(f(a, b)))

      case Type.Str :: Type.Const(Data.Str(_)) :: Nil => Type.Str
      case Type.Const(Data.Str(_)) :: Type.Str :: Nil => Type.Str
      case Type.Str :: Type.Str :: Nil                => Type.Str
    }

  // TODO: variable arity
  val Concat = Func(Mapping, "concat", "Concatenates two (or more) string values",
    Type.Str, Type.Str :: Type.Str :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, List(Embed(ConstantF(Data.Str(""))), Embed(second))) =>
            second.some
          case InvokeF(_, List(Embed(first), Embed(ConstantF(Data.Str(""))))) =>
            first.some
          case _ => None
        }
    },
    stringApply(_ + _),
    basicUntyper)

  private def regexForLikePattern(pattern: String, escapeChar: Option[Char]):
      String = {
    def sansEscape(pat: List[Char]): List[Char] = pat match {
      case '_' :: t =>         '.' +: escape(t)
      case '%' :: t => ".*".toList ⊹ escape(t)
      case c   :: t =>
        if ("\\^$.|?*+()[{".contains(c))
          '\\' +: c +: escape(t)
        else c +: escape(t)
      case Nil      => Nil
    }

    def escape(pat: List[Char]): List[Char] =
      escapeChar match {
        case None => sansEscape(pat)
        case Some(esc) =>
          pat match {
            // NB: We only handle the escape char when it’s before a special
            //     char, otherwise you run into weird behavior when the escape
            //     char _is_ a special char. Will change if someone can find
            //     an actual definition of SQL’s semantics.
            case `esc` :: '%' :: t => '%' +: escape(t)
            case `esc` :: '_' :: t => '_' +: escape(t)
            case l                 => sansEscape(l)
          }
      }
    "^" + escape(pattern.toList).mkString + "$"
  }

  val Like = Func(Mapping, 
    "(like)",
    "Determines if a string value matches a pattern.",
    Type.Bool, Type.Str :: Type.Str :: Type.Str :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, List(Embed(str), Embed(ConstantF(Data.Str(pat))), Embed(ConstantF(Data.Str(esc))))) =>
            if (esc.length > 1)
              None
            else
              Search(str.embed,
                ConstantF[T[LogicalPlan]](Data.Str(regexForLikePattern(pat, esc.headOption))).embed,
                ConstantF[T[LogicalPlan]](Data.Bool(false)).embed).some
          case _ => None
        }
    },
    constTyper(Type.Bool),
    basicUntyper)

  def matchAnywhere(str: String, pattern: String, insen: Boolean) =
    java.util.regex.Pattern.compile(if (insen) "(?i)" ⊹ pattern else pattern).matcher(str).find()

  val Search = Func(Mapping, 
    "search",
    "Determines if a string value matches a regular expresssion. If the third argument is true, then it is a case-insensitive match.",
    Type.Bool, Type.Str :: Type.Str :: Type.Bool :: Nil,
    noSimplification,
    partialTyperV {
      case Type.Const(Data.Str(str)) :: Type.Const(Data.Str(pattern)) :: Type.Const(Data.Bool(insen)) :: Nil =>
        success(Type.Const(Data.Bool(matchAnywhere(str, pattern, insen))))
      case strT :: patternT :: insenT :: Nil =>
        (Type.typecheck(Type.Str, strT).leftMap(nel => nel.map(ι[SemanticError])) |@|
         Type.typecheck(Type.Str, patternT).leftMap(nel => nel.map(ι[SemanticError])) |@|
         Type.typecheck(Type.Bool, insenT).leftMap(nel => nel.map(ι[SemanticError])))((_, _, _) => Type.Bool)
    },
    basicUntyper)

  val Length = Func(Mapping, 
    "length",
    "Counts the number of characters in a string.",
    Type.Int, Type.Str :: Nil,
    noSimplification,
    partialTyper {
      case Type.Const(Data.Str(str)) :: Nil => Type.Const(Data.Int(str.length))
      case Type.Str :: Nil                  => Type.Int
    },
    basicUntyper)

  val Lower = Func(Mapping, 
    "lower",
    "Converts the string to lower case.",
    Type.Str, Type.Str :: Nil,
    noSimplification,
    partialTyper {
      case Type.Const(Data.Str(str)) :: Nil =>
        Type.Const(Data.Str(str.toLowerCase))
      case Type.Str :: Nil => Type.Str
    },
    basicUntyper)

  val Upper = Func(Mapping, 
    "upper",
    "Converts the string to upper case.",
    Type.Str, Type.Str :: Nil,
    noSimplification,
    partialTyper {
      case Type.Const(Data.Str(str)) :: Nil =>
        Type.Const (Data.Str(str.toUpperCase))
      case Type.Str :: Nil => Type.Str
    },
    basicUntyper)

  val Substring: Func = Func(Mapping, 
    "substring",
    "Extracts a portion of the string",
    Type.Str, Type.Str :: Type.Int :: Type.Int :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(f, List(
            Embed(ConstantF(Data.Str(str))),
            Embed(ConstantF(Data.Int(from))),
            for0))
              if 0 < from =>
            InvokeF(f, List(
              ConstantF[T[LogicalPlan]](Data.Str(str.substring(from.intValue))).embed,
              ConstantF[T[LogicalPlan]](Data.Int(0)).embed,
              for0)).some
          case _ => None
        }
    },
    partialTyperV {
      case List(
        Type.Const(Data.Str(str)),
        Type.Const(Data.Int(from)),
        Type.Const(Data.Int(for0))) => {
        success(Type.Const(Data.Str(str.substring(from.intValue, from.intValue + for0.intValue))))
      }
      case List(Type.Const(Data.Str(str)), Type.Const(Data.Int(from)), _)
          if str.length <= from =>
        success(Type.Const(Data.Str("")))
      case List(Type.Const(Data.Str(_)), Type.Const(Data.Int(_)), Type.Int) =>
        success(Type.Str)
      case List(Type.Const(Data.Str(_)), Type.Int, Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case List(Type.Const(Data.Str(_)), Type.Int,                Type.Int) =>
        success(Type.Str)
      case List(Type.Str, Type.Const(Data.Int(_)), Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case List(Type.Str, Type.Const(Data.Int(_)), Type.Int)                =>
        success(Type.Str)
      case List(Type.Str, Type.Int,                Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case List(Type.Str, Type.Int,                Type.Int)                =>
        success(Type.Str)
      case List(Type.Str, _,                       _)                       =>
        failureNel(GenericError("expected integer arguments for SUBSTRING"))
      case List(t, _, _) => failureNel(TypeError(Type.Str, t, None))
    },
    basicUntyper)

  val Boolean = Func(Mapping, 
    "boolean",
    "Converts the strings “true” and “false” into boolean values. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Bool, Type.Str :: Nil,
    noSimplification,
    partialTyperV {
      case Type.Const(Data.Str("true"))  :: Nil =>
        success(Type.Const(Data.Bool(true)))
      case Type.Const(Data.Str("false")) :: Nil =>
        success(Type.Const(Data.Bool(false)))
      case Type.Const(Data.Str(str))     :: Nil =>
        failureNel(InvalidStringCoercion(str, List("true", "false").right))
      case Type.Str                      :: Nil => success(Type.Bool)
    },
    untyper(x => ToString.tpe(List(x)).map(List(_))))

  val intRegex = "[+-]?\\d+"
  val floatRegex = intRegex + "(?:.\\d+)?(?:[eE]" + intRegex + ")?"
  val dateRegex = "(?:\\d{4}-\\d{2}-\\d{2}|\\d{8})"
  val timeRegex = "\\d{2}(?::?\\d{2}(?::?\\d{2}(?:\\.\\d{3})?)?)?Z?"
  val timestampRegex = dateRegex + "T" + timeRegex

  val Integer = Func(Mapping, 
    "integer",
    "Converts strings containing integers into integer values. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Int, Type.Str :: Nil,
    noSimplification,
    partialTyperV {
      case Type.Const(Data.Str(str))   :: Nil =>
        str.parseInt.fold(
          κ(failureNel(InvalidStringCoercion(str, "a string containing an integer".left))),
          i => success(Type.Const(Data.Int(i))))
      case Type.Str                    :: Nil => success(Type.Int)
    },
    untyper(x => ToString.tpe(List(x)).map(List(_))))

  val Decimal = Func(Mapping, 
    "decimal",
    "Converts strings containing decimals into decimal values. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Dec, Type.Str :: Nil,
    noSimplification,
    partialTyperV {
      case Type.Const(Data.Str(str))   :: Nil =>
        str.parseDouble.fold(
          κ(failureNel(InvalidStringCoercion(str, "a string containing an decimal number".left))),
          i => success(Type.Const(Data.Dec(i))))
      case Type.Str                    :: Nil => success(Type.Int)
    },
    untyper(x => ToString.tpe(List(x)).map(List(_))))

  val Null = Func(Mapping, 
    "null",
    "Converts strings containing “null” into the null value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Null, Type.Str :: Nil,
    noSimplification,
    partialTyperV {
      case Type.Const(Data.Str("null"))  :: Nil => success(Type.Const(Data.Null))
      case Type.Const(Data.Str(str))     :: Nil =>
        failureNel(InvalidStringCoercion(str, List("null").right))
      case Type.Str                      :: Nil => success(Type.Null)
    },
    untyper(x => ToString.tpe(List(x)).map(List(_))))

  val ToString: Func = Func(Mapping, 
    "to_string",
    "Converts any primitive type to a string.",
    Type.Str, Type.Syntaxed :: Nil,
    noSimplification,
    partialTyperV {
      case List(Type.Const(data)) => (data match {
        case Data.Str(str)     => success(str)
        case Data.Null         => success("null")
        case Data.Bool(b)      => success(b.shows)
        case Data.Int(i)       => success(i.shows)
        case Data.Dec(d)       => success(d.shows)
        case Data.Timestamp(t) => success(t.toString)
        case Data.Date(d)      => success(d.toString)
        case Data.Time(t)      => success(t.toString)
        case Data.Interval(i)  => success(i.toString)
        // NB: Should not be able to hit this case, because of the domain.
        case other             =>
          failureNel(
            TypeError(
              Type.Syntaxed,
              other.dataType,
              "can not convert aggregate types to String".some):SemanticError)
      }).map(s => Type.Const(Data.Str(s)))
      case List(_) => success(Type.Str)
    },
    partialUntyperV {
      case x @ Type.Const(_) =>
        (Null.tpe(List(x)) <+>
          Boolean.tpe(List(x)) <+>
          Integer.tpe(List(x)) <+>
          Decimal.tpe(List(x)) <+>
          DateLib.Date.tpe(List(x)) <+>
          DateLib.Time.tpe(List(x)) <+>
          DateLib.Timestamp.tpe(List(x)) <+>
          DateLib.Interval.tpe(List(x)))
          .map(List(_))
    })

  def functions = Concat :: Like :: Search :: Length :: Lower :: Upper :: Substring :: Boolean :: Integer :: Decimal :: Null :: ToString :: Nil
}
object StringLib extends StringLib
