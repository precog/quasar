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

package quasar.std

import slamdata.Predef._
import quasar._, SemanticError._
import quasar.fp._
import quasar.fp.ski._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import java.time.ZoneOffset.UTC

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._, Validation.{success, failureNel}
import shapeless.{Data => _, :: => _, _}

trait StringLib extends Library {
  private def stringApply(f: (String, String) => String): Func.Typer[nat._2] =
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Str(a)), Type.Const(Data.Str(b))) => Type.Const(Data.Str(f(a, b)))

      case Sized(Type.Str, Type.Const(Data.Str(_))) => Type.Str
      case Sized(Type.Const(Data.Str(_)), Type.Str) => Type.Str
      case Sized(Type.Str, Type.Str)                => Type.Str
    }

  // TODO: variable arity
  val Concat = BinaryFunc(
    Mapping,
    "Concatenates two (or more) string values",
    Type.Str,
    Func.Input2(Type.Str, Type.Str),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(Constant(Data.Str(""))), Embed(second))) =>
            second.some
          case InvokeUnapply(_, Sized(Embed(first), Embed(Constant(Data.Str(""))))) =>
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

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
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

  // TODO: This is here (rather than converted to `Search` in `sql.compile`)
  //       until we can constant-fold as we compile.
  val Like = TernaryFunc(
    Mapping,
    "Determines if a string value matches a pattern.",
    Type.Bool,
    Func.Input3(Type.Str, Type.Str, Type.Str),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(str), Embed(Constant(Data.Str(pat))), Embed(Constant(Data.Str(esc))))) =>
            if (esc.length > 1)
              None
            else
              Search(str.embed,
                constant[T](Data.Str(regexForLikePattern(pat, esc.headOption))).embed,
                constant[T](Data.Bool(false)).embed).some
          case _ => None
        }
    },
    constTyper(Type.Bool),
    basicUntyper)

  def matchAnywhere(str: String, pattern: String, insen: Boolean) =
    java.util.regex.Pattern.compile(if (insen) "(?i)" ⊹ pattern else pattern).matcher(str).find()

  val Search = TernaryFunc(
    Mapping,
    "Determines if a string value matches a regular expression. If the third argument is true, then it is a case-insensitive match.",
    Type.Bool,
    Func.Input3(Type.Str, Type.Str, Type.Bool),
    noSimplification,
    partialTyperV[nat._3] {
      case Sized(Type.Const(Data.Str(str)), Type.Const(Data.Str(pattern)), Type.Const(Data.Bool(insen))) =>
        success(Type.Const(Data.Bool(matchAnywhere(str, pattern, insen))))
      case Sized(strT, patternT, insenT) =>
        (Type.typecheck(Type.Str, strT).leftMap(nel => nel.map(ι[SemanticError])) |@|
         Type.typecheck(Type.Str, patternT).leftMap(nel => nel.map(ι[SemanticError])) |@|
         Type.typecheck(Type.Bool, insenT).leftMap(nel => nel.map(ι[SemanticError])))((_, _, _) => Type.Bool)
    },
    basicUntyper)

  val Length = UnaryFunc(
    Mapping,
    "Counts the number of characters in a string.",
    Type.Int,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => Type.Const(Data.Int(str.length))
      case Sized(Type.Str)                  => Type.Int
    },
    basicUntyper)

  val Lower = UnaryFunc(
    Mapping,
    "Converts the string to lower case.",
    Type.Str,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Str(str))) =>
        Type.Const(Data.Str(str.toLowerCase))
      case Sized(Type.Str) => Type.Str
    },
    basicUntyper)

  val Upper = UnaryFunc(
    Mapping,
    "Converts the string to upper case.",
    Type.Str,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Str(str))) =>
        Type.Const(Data.Str(str.toUpperCase))
      case Sized(Type.Str) => Type.Str
    },
    basicUntyper)

  /** Substring which always gives a result, no matter what offsets are provided.
    * Reverse-engineered from MongoDb's \$substr op, for lack of a better idea
    * of how this should work. Note: if `start` < 0, the result is `""`.
    * If `length` < 0, then result includes the rest of the string. Otherwise
    * the behavior is as you might expect.
    */
  def safeSubstring(str: String, start: Int, length: Int): String =
    if (start < 0 || start > str.length) ""
    else if (length < 0) str.substring(start, str.length)
    else str.substring(start, (start + length) min str.length)

  val Substring: TernaryFunc = TernaryFunc(
    Mapping,
    "Extracts a portion of the string",
    Type.Str,
    Func.Input3(Type.Str, Type.Int, Type.Int),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(f @ TernaryFunc(_, _, _, _, _, _, _), Sized(
            Embed(Constant(Data.Str(str))),
            Embed(Constant(Data.Int(from))),
            for0))
              if 0 < from =>
            Invoke(f, Func.Input3(
              Constant[T](Data.Str(str.substring(from.intValue))).embed,
              Constant[T](Data.Int(0)).embed,
              for0)).some
          case _ => None
        }
    },
    partialTyperV[nat._3] {
      case Sized(
        Type.Const(Data.Str(str)),
        Type.Const(Data.Int(from)),
        Type.Const(Data.Int(for0))) => {
        success(Type.Const(Data.Str(safeSubstring(str, from.intValue, for0.intValue))))
      }
      case Sized(Type.Const(Data.Str(str)), Type.Const(Data.Int(from)), _)
          if str.length <= from =>
        success(Type.Const(Data.Str("")))
      case Sized(_, Type.Const(Data.Int(from)), _)
          if from < 0 =>
        success(Type.Const(Data.Str("")))
      case Sized(_, _, Type.Const(Data.Int(for0)))
          if for0.intValue ≟ 0 =>
        success(Type.Const(Data.Str("")))
      case Sized(Type.Const(Data.Str(_)), Type.Const(Data.Int(_)), Type.Int) =>
        success(Type.Str)
      case Sized(Type.Const(Data.Str(_)), Type.Int, Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case Sized(Type.Const(Data.Str(_)), Type.Int,                Type.Int) =>
        success(Type.Str)
      case Sized(Type.Str, Type.Const(Data.Int(_)), Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case Sized(Type.Str, Type.Const(Data.Int(_)), Type.Int)                =>
        success(Type.Str)
      case Sized(Type.Str, Type.Int,                Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case Sized(Type.Str, Type.Int,                Type.Int)                =>
        success(Type.Str)
      case Sized(Type.Str, _,                       _)                       =>
        failureNel(GenericError("expected integer arguments for SUBSTRING"))
      case Sized(t, _, _) => failureNel(TypeError(Type.Str, t, None))
    },
    basicUntyper)

  val Boolean = UnaryFunc(
    Mapping,
    "Converts the strings “true” and “false” into boolean values. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Bool,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str("true")))  =>
        success(Type.Const(Data.Bool(true)))
      case Sized(Type.Const(Data.Str("false"))) =>
        success(Type.Const(Data.Bool(false)))
      case Sized(Type.Const(Data.Str(str)))     =>
        failureNel(InvalidStringCoercion(str, List("true", "false").right))
      case Sized(Type.Str)                      => success(Type.Bool)
    },
    untyper[nat._1](x => ToString.tpe(Func.Input1(x)).map(Func.Input1(_))))

  val intRegex = "[+-]?\\d+"
  val floatRegex = intRegex + "(?:.\\d+)?(?:[eE]" + intRegex + ")?"
  val dateRegex = "(?:\\d{4}-\\d{2}-\\d{2}|\\d{8})"
  val timeRegex = "\\d{2}(?::?\\d{2}(?::?\\d{2}(?:\\.\\d{3})?)?)?Z?"
  val timestampRegex = dateRegex + "T" + timeRegex

  val Integer = UnaryFunc(
    Mapping,
    "Converts strings containing integers into integer values. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Int,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) =>
        \/.fromTryCatchNonFatal(BigInt(str)).fold(
          κ(failureNel(InvalidStringCoercion(str, "a string containing an integer".left))),
          i => success(Type.Const(Data.Int(i))))

      case Sized(Type.Str) => success(Type.Int)
    },
    untyper[nat._1](x => ToString.tpe(Func.Input1(x)).map(Func.Input1(_))))

  val Decimal = UnaryFunc(
    Mapping,
    "Converts strings containing decimals into decimal values. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Dec,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) =>
        \/.fromTryCatchNonFatal(BigDecimal(str)).fold(
           κ(failureNel(InvalidStringCoercion(str, "a string containing an decimal number".left))),
          i => success(Type.Const(Data.Dec(i))))
      case Sized(Type.Str) => success(Type.Int)
    },
    untyper[nat._1](x => ToString.tpe(Func.Input1(x)).map(Func.Input1(_))))

  val Null = UnaryFunc(
    Mapping,
    "Converts strings containing “null” into the null value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Null,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str("null"))) => success(Type.Const(Data.Null))
      case Sized(Type.Const(Data.Str(str))) =>
        failureNel(InvalidStringCoercion(str, List("null").right))
      case Sized(Type.Str) => success(Type.Null)
    },
    untyper[nat._1](x => ToString.tpe(Func.Input1(x)).map(Func.Input1(_))))

  val ToString: UnaryFunc = UnaryFunc(
    Mapping,
    "Converts any primitive type to a string.",
    Type.Str,
    Func.Input1(Type.Syntaxed),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(data)) => (data match {
        case Data.Str(str)     => success(str)
        case Data.Null         => success("null")
        case Data.Bool(b)      => success(b.shows)
        case Data.Int(i)       => success(i.shows)
        case Data.Dec(d)       => success(d.shows)
        case Data.Timestamp(t) => success(t.atZone(UTC).format(DataCodec.dateTimeFormatter))
        case Data.Date(d)      => success(d.toString)
        case Data.Time(t)      => success(t.format(DataCodec.timeFormatter))
        case Data.Interval(i)  => success(i.toString)
        // NB: Should not be able to hit this case, because of the domain.
        case other             =>
          failureNel(
            TypeError(
              Type.Syntaxed,
              other.dataType,
              "can not convert aggregate types to String".some):SemanticError)
      }).map(s => Type.Const(Data.Str(s)))
      case Sized(_) => success(Type.Str)
    },
    partialUntyperV[nat._1] {
      case x @ Type.Const(_) =>
        (Null.tpe(Func.Input1(x)) <+>
          Boolean.tpe(Func.Input1(x)) <+>
          Integer.tpe(Func.Input1(x)) <+>
          Decimal.tpe(Func.Input1(x)) <+>
          DateLib.Date.tpe(Func.Input1(x)) <+>
          DateLib.Time.tpe(Func.Input1(x)) <+>
          DateLib.Timestamp.tpe(Func.Input1(x)) <+>
          DateLib.Interval.tpe(Func.Input1(x)))
          .map(Func.Input1(_))
    })
}

object StringLib extends StringLib
