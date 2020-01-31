/*
 * Copyright 2014–2020 SlamData Inc.
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
import quasar._
import quasar.common.data.Data
import quasar.fp._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import shapeless.{Data => _, :: => _, _}

trait StructuralLib extends Library {

  val MakeMap = BinaryFunc(
    Mapping,
    "Makes a singleton map containing a single key",
    noSimplification)

  val MakeArray = UnaryFunc(
    Mapping,
    "Makes a singleton array containing a single element",
    noSimplification)

  val Meta = UnaryFunc(
    Mapping,
    "Returns the metadata associated with a value.",
    noSimplification)

  val MapConcat: BinaryFunc = BinaryFunc(
    Mapping,
    "A right-biased merge of two maps into one map",
    noSimplification)

  val ArrayConcat: BinaryFunc = BinaryFunc(
    Mapping,
    "A merge of two arrays into one array",
    noSimplification)

  // TODO can we delete this now that we don't type-check?
  // NB: Used only during type-checking, and then compiled into either (string) Concat or ArrayConcat.
  val ConcatOp = BinaryFunc(
    Mapping,
    "A merge of two arrays/strings.",
    noSimplification)

  val MapProject = BinaryFunc(
    Mapping,
    "Extracts a specified key of an object",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(MakeMapN(m)), Embed(key))) =>
            m.map(_.leftMap(_.project)).toListMap.get(key).map(_.project)
          case _ => None
        }
    })

  val ArrayProject = BinaryFunc(
    Mapping,
    "Extracts a specified index of an array",
    noSimplification)

  val DeleteKey: BinaryFunc = BinaryFunc(
    Mapping,
    "Deletes a specified key from a map",
    noSimplification)

  val ContainsKey: BinaryFunc = BinaryFunc(
    Mapping,
    "Checks for the existence of the specified key in a map",
    noSimplification)

  val EnsureNumber: UnaryFunc = UnaryFunc(
    Mapping,
    "Ensures the value is a number",
    noSimplification)

  val EnsureString: UnaryFunc = UnaryFunc(
    Mapping,
    "Ensures the value is a string",
    noSimplification)

  val EnsureBoolean: UnaryFunc = UnaryFunc(
    Mapping,
    "Ensures the value is a boolean",
    noSimplification)

  val EnsureOffsetDateTime: UnaryFunc = UnaryFunc(
    Mapping,
    "Ensures the value is an OffsetDateTime",
    noSimplification)

  val EnsureNull: UnaryFunc = UnaryFunc(
    Mapping,
    "Ensures the value is null",
    noSimplification)

  val FlattenMap = UnaryFunc(
    Expansion,
    "Zooms in on the values of a map, extending the current dimension with the keys",
    noSimplification)

  val FlattenArray = UnaryFunc(
    Expansion,
    "Zooms in on the elements of an array, extending the current dimension with the indices",
    noSimplification)

  val FlattenMapKeys = UnaryFunc(
    Expansion,
    "Zooms in on the keys of a map, also extending the current dimension with the keys",
    noSimplification)

  val FlattenArrayIndices = UnaryFunc(
    Expansion,
    "Zooms in on the indices of an array, also extending the current dimension with the indices",
    noSimplification)

  val ShiftMap = UnaryFunc(
    Expansion,
    "Zooms in on the values of a map, adding the keys as a new dimension",
    noSimplification)

  val ShiftArray = UnaryFunc(
    Expansion,
    "Zooms in on the elements of an array, adding the indices as a new dimension",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(UnshiftArray, Sized(Embed(set)))))) => set.some
          case _                                                                                => None
        }
    })

  val ShiftMapKeys = UnaryFunc(
    Expansion,
    "Zooms in on the keys of a map, also adding the keys as a new dimension",
    noSimplification)

  val ShiftArrayIndices = UnaryFunc(
    Expansion,
    "Zooms in on the indices of an array, also adding the keys as a new dimension",
    noSimplification)

  val UnshiftMap: BinaryFunc = BinaryFunc(
    Reduction,
    "Puts the value into a map with the key.",
    noSimplification)

  val UnshiftArray: UnaryFunc = UnaryFunc(
    Reduction,
    "Puts the values into an array.",
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(ShiftArray, Sized(Embed(array)))))) => array.some
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(ShiftArrayIndices, Sized(Embed(Constant(Data.Arr(array)))))))) =>
            Constant(Data.Arr((0 until array.length).toList ∘ (Data.Int(_)))).some
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply( ShiftMap, Sized(Embed(Constant(Data.Obj(map)))))))) =>
            Constant(Data.Arr(map.values.toList)).some
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply( ShiftMapKeys, Sized(Embed(Constant(Data.Obj(map)))))))) =>
            Constant(Data.Arr(map.keys.toList.map(Data.Str(_)))).some
          case _ => None
        }
    })

  object MakeMapN {
    // Note: signature does not match VirtualFunc
    def apply[T](args: (T, T)*)(implicit T: Corecursive.Aux[T, LP]): LP[T] =
      args.toList match {
        case Nil      => Constant(Data.Obj())
        case x :: xs  =>
          xs.foldLeft(MakeMap(x._1, x._2))((a, b) =>
            MapConcat(a.embed, MakeMap(b._1, b._2).embed))
      }

    // Note: signature does not match VirtualFunc
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def unapply[T](t: LP[T])(implicit T: Recursive.Aux[T, LP]):
        Option[List[(T, T)]] =
      t match {
        case InvokeUnapply(MakeMap, Sized(name, expr)) => Some(List((name, expr)))
        case InvokeUnapply(MapConcat, Sized(a, b))     => (unapply(a.project) ⊛ unapply(b.project))(_ ::: _)
        case _                                             => None
      }
  }

  object MakeArrayN {
    def apply[T](args: T*)(implicit T: Corecursive.Aux[T, LP]): LP[T] =
      args.map(MakeArray(_))
        .reduceLeftOption((x, y) => ArrayConcat(x.embed, y.embed))
        .getOrElse(Constant(Data.Arr(Nil)))

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def unapply[T](t: T)(implicit T: Recursive.Aux[T, LP]): Option[List[T]] =
      t.project match {
        case InvokeUnapply(MakeArray, Sized(x))      => Some(x :: Nil)
        case InvokeUnapply(ArrayConcat, Sized(a, b)) => (unapply(a) ⊛ unapply(b))(_ ::: _)
        case _                                        => None
      }

    object Attr {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def unapply[A](t: Cofree[LP, A]): Option[List[Cofree[LP, A]]] =
        t.tail match {
          case InvokeUnapply(MakeArray, Sized(x))      => Some(x :: Nil)
          case InvokeUnapply(ArrayConcat, Sized(a, b)) => (unapply(a) ⊛ unapply(b))(_ ::: _)
          case _                                        => None
        }
    }
  }
}

object StructuralLib extends StructuralLib
