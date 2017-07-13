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

import quasar._
import quasar.common.PrimaryType
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import matryoshka._
import scalaz._, Scalaz._
import shapeless.{Data => _, _}

trait IdentityLib extends Library {
  import Type._
  import Validation.success

  val Squash: UnaryFunc = UnaryFunc(
    Squashing,
    "Squashes all dimensional information",
    Top,
    Func.Input1(Top),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(Squash, Sized(x))))) =>
            Squash(x).some
          case _ => none
        }
    },
    partialTyper[nat._1] { case Sized(x) => x },
    untyper[nat._1](t => success(Func.Input1(t))))

  val ToId = UnaryFunc(
    Mapping,
    "Converts a string to a (backend-specific) object identifier.",
    Type.Id,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => Type.Const(Data.Id(str))
      case Sized(Type.Str)                  => Type.Id
    },
    basicUntyper)

  val TypeOf = UnaryFunc(
    Mapping,
    "Returns the simple type of a value.",
    Type.Str,
    Func.Input1(Type.Top),
    noSimplification,
    {
      case Sized(typ) =>
        success(typ.toPrimaryType.fold(
          if      (Type.Bottom.contains(typ))    Type.Bottom
          // NB: These cases should be identified via metadata
          else if (Type.Timestamp.contains(typ)) Type.Const(Data.Str("timestamp"))
          else if (Type.Date.contains(typ))      Type.Const(Data.Str("date"))
          else if (Type.Time.contains(typ))      Type.Const(Data.Str("time"))
          else if (Type.Interval.contains(typ))  Type.Const(Data.Str("interval"))
          else                                   Type.Str)(
          t => Type.Const(Data.Str(PrimaryType.name.reverseGet(t)))))
    }
                        ,
    partialUntyper[nat._1] {
      case Type.Bottom => Func.Input1(Type.Bottom)
      case Type.Const(Data.Str(name)) =>
        Func.Input1(PrimaryType.name.getOption(name).fold[Type](
          Type.Top)(
          Type.fromPrimaryType))
      case _ => Func.Input1(Type.Top)
    })
}

object IdentityLib extends IdentityLib
