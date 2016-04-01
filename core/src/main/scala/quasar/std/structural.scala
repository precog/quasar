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
import quasar._, LogicalPlan._, SemanticError._
import quasar.fp._

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._, Validation.{success, failure}

trait StructuralLib extends Library {
  import Type._

  val MakeObject = Mapping(
    "MAKE_OBJECT",
    "Makes a singleton object containing a single field",
    AnyObject, Str :: Top :: Nil,
    noSimplification,
    partialTyper {
      case List(Const(Data.Str(name)), Const(Data.Set(data))) =>
        Const(Data.Set(data.map(d => Data.Obj(ListMap(name -> d)))))
      case List(Const(Data.Str(name)), Const(data)) => Const(Data.Obj(ListMap(name -> data)))
      case List(Const(Data.Str(name)), valueType)   => Obj(Map(name -> valueType), None)
      case List(_, valueType)   => Obj(Map(), Some(valueType))
    },
    partialUntyperV {
      case Const(Data.Obj(map)) => map.headOption match {
        case Some((key, value)) => success(List(Const(Data.Str(key)), Const(value)))
        case None => failure(NonEmptyList(GenericError("MAKE_OBJECT can’t result in an empty object")))
      }
      case Obj(map, uk) => map.headOption.fold(
        uk.fold[ValidationNel[SemanticError, List[Type]]](
          failure(NonEmptyList(GenericError("MAKE_OBJECT can’t result in an empty object"))))(
          t => success(List(Str, t)))) {
        case (key, value) => success(List(Const(Data.Str(key)), value))
      }
    })

  val MakeArray = Mapping(
    "MAKE_ARRAY",
    "Makes a singleton array containing a single element",
    AnyArray, Top :: Nil,
    noSimplification,
    partialTyper {
      case Const(Data.Set(data)) :: Nil =>
        Const(Data.Set(data.map(d => Data.Arr(d :: Nil))))
      case Const(data)           :: Nil => Const(Data.Arr(data :: Nil))
      case valueType             :: Nil => Arr(List(valueType))
    },
    partialUntyper {
      case Const(Data.Arr(List(elem))) => List(Const(elem))
      case Arr(List(elemType))         => List(elemType)
      case FlexArr(_, _, elemType)     => List(elemType)
    })

  val ObjectConcat: Mapping = Mapping(
    "OBJECT_CONCAT",
    "A right-biased merge of two objects into one object",
    AnyObject, AnyObject :: AnyObject :: Nil,
    noSimplification,
    partialTyperV {
      case List(Const(Data.Obj(map1)), Const(Data.Obj(map2))) =>
        success(Const(Data.Obj(
          if (map1.isEmpty)      map2
          else if (map2.isEmpty) map1
          else                   map1 ++ map2)))
      case List(Const(o1 @ Data.Obj(_)), o2) => ObjectConcat(o1.dataType, o2)
      case List(o1, Const(o2 @ Data.Obj(_))) => ObjectConcat(o1, o2.dataType)
      case List(Obj(map1, uk1), Obj(map2, None)) =>
        success(Obj(map1 ++ map2, uk1))
      case List(Obj(map1, uk1), Obj(map2, Some(uk2))) =>
        success(Obj(
          map1 ∘ (_ ⨿ uk2) ++ map2,
          Some(uk1.fold(uk2)(_ ⨿ uk2))))
    },
    partialUntyper {
      case x if x.objectLike =>
        val t = Obj(Map(), x.objectType)
        List(t, t)
    })

  val ArrayConcat: Mapping = Mapping(
    "ARRAY_CONCAT",
    "A merge of two arrays into one array",
    AnyArray, AnyArray :: AnyArray :: Nil,
    noSimplification,
    partialTyperV {
      case List(Const(Data.Arr(els1)), Const(Data.Arr(els2))) =>
        success(Const(Data.Arr(els1 ++ els2)))
      case List(Const(Data.Arr(els1)), a2) if els1.isEmpty => success(a2)
      case List(a1, Const(Data.Arr(els2))) if els2.isEmpty => success(a1)
      case List(Arr(els1), Arr(els2)) => success(Arr(els1 ++ els2))
      case List(Const(a1 @ Data.Arr(_)), a2) => ArrayConcat(a1.dataType, a2)
      case List(a1, Const(a2 @ Data.Arr(_))) => ArrayConcat(a1, a2.dataType)
      case List(a1, FlexArr(min2, max2, elem2)) =>
        (a1.arrayMinLength |@| a1.arrayType)((min1, typ1) =>
          success(FlexArr(
            min1 + min2,
            (a1.arrayMaxLength |@| max2)(_ + _),
            Type.lub(typ1, elem2))))
          .getOrElse(failure(NonEmptyList(GenericError(a1.shows + " is not an array."))))
      case List(FlexArr(min1, max1, elem1), a2) =>
        (a2.arrayMinLength |@| a2.arrayType)((min2, typ2) =>
          success(FlexArr(
            min1 + min2,
            (max1 |@| a2.arrayMaxLength)(_ + _),
            Type.lub(elem1, typ2))))
          .getOrElse(failure(NonEmptyList(GenericError(a2.shows + " is not an array."))))
    },
    partialUntyperV {
      case x if x.arrayLike =>
        x.arrayType.fold[ValidationNel[SemanticError, List[Type]]](
          failure(NonEmptyList(GenericError("internal error: " + x.shows + " is arrayLike, but no arrayType")))) {
          typ =>
            val t = FlexArr(0, x.arrayMaxLength, typ)
            success(List(t, t))
        }
    })

  // NB: Used only during type-checking, and then compiled into either (string) Concat or ArrayConcat.
  val ConcatOp = Mapping(
    "(||)",
    "A merge of two arrays/strings.",
    AnyArray ⨿ Str, AnyArray ⨿ Str :: AnyArray ⨿ Str :: Nil,
    noSimplification,
    partialTyperV {
      case t1 :: t2 :: Nil if t1.arrayLike && t2.contains(AnyArray ⨿ Str)    => ArrayConcat(t1, FlexArr(0, None, Top))
      case t1 :: t2 :: Nil if t1.contains(AnyArray ⨿ Str) && t2.arrayLike    => ArrayConcat(FlexArr(0, None, Top), t2)
      case t1 :: t2 :: Nil if t1.arrayLike && t2.arrayLike       => ArrayConcat(t1, t2)

      case Const(Data.Str(str1)) :: Const(Data.Str(str2)) :: Nil     => success(Const(Data.Str(str1 ++ str2)))
      case t1 :: t2 :: Nil if Str.contains(t1) && t2.contains(AnyArray ⨿ Str) => success(Type.Str)
      case t1 :: t2 :: Nil if t1.contains(AnyArray ⨿ Str) && Str.contains(t2) => success(Type.Str)
      case t1 :: t2 :: Nil if Str.contains(t1) && Str.contains(t2) => StringLib.Concat(t1, t2)

      case t1 :: t2 :: Nil if t1 == t2 => success(t1)

      case t1 :: t2 :: Nil if Str.contains(t1) && t2.arrayLike => failure(NonEmptyList(GenericError("cannot concat string with array")))
      case t1 :: t2 :: Nil if t1.arrayLike && Str.contains(t2) => failure(NonEmptyList(GenericError("cannot concat array with string")))
    },
    partialUntyperV {
      case x if x.contains(AnyArray ⨿ Str) => success(AnyArray ⨿ Str :: AnyArray ⨿ Str :: Nil)
      case x if x.arrayLike                 => ArrayConcat.untype(x)
      case x if x.contains(Type.Str)        => StringLib.Concat.untype(x)
    })

  val ObjectProject = Mapping(
    "({})",
    "Extracts a specified field of an object",
    Top, AnyObject :: Str :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case InvokeF(_, List(Embed(MakeObjectN(obj)), Embed(field))) =>
          obj.map(_.leftMap(_.project)).toListMap.get(field).map(_.project)
        case _ => None
      }
    },
    partialTyperV { case List(v1, v2) => v1.objectField(v2) },
    basicUntyper)

  val ArrayProject = Mapping(
    "([])",
    "Extracts a specified index of an array",
    Top, AnyArray :: Int :: Nil,
    noSimplification,
    partialTyperV { case List(v1, v2) => v1.arrayElem(v2) },
    basicUntyper)

  val DeleteField: Mapping = Mapping(
    "DELETE_FIELD",
    "Deletes a specified field from an object",
    AnyObject, AnyObject :: Str :: Nil,
    noSimplification,
    partialTyper {
      case List(Const(Data.Obj(map)), Const(Data.Str(key))) =>
        Const(Data.Obj(map - key))
      case List(Obj(map, uk), Const(Data.Str(key))) => Obj(map - key, uk)
      case List(v1, _) => Obj(Map(), v1.objectType)
    },
    partialUntyperV {
      case Const(o @ Data.Obj(map)) => DeleteField.untype(o.dataType)
      case Obj(map, _)              => success(List(Obj(map, Some(Top)), Str))
    })

  val FlattenMap = Expansion(
    "FLATTEN_MAP",
    "Zooms in on the values of a map, extending the current dimension with the keys",
    Top, AnyObject :: Nil,
    noSimplification,
    partialTyperV {
      case List(Const(Data.Obj(map))) =>
        success(Const(Data.Set(map.values.toList)))
      case List(x) if x.objectLike =>
        x.objectType.fold[ValidationNel[SemanticError, Type]](
          failure(NonEmptyList(GenericError("internal error: objectLike, but no objectType"))))(
          success)
    },
    untyper(tpe => success(List(Obj(Map(), Some(tpe))))))

  val FlattenArray = Expansion(
    "FLATTEN_ARRAY",
    "Zooms in on the elements of an array, extending the current dimension with the indices",
    Top, AnyArray :: Nil,
    noSimplification,
    partialTyperV {
      case List(Const(Data.Arr(elems))) => success(Const(Data.Set(elems)))
      case List(x) if x.arrayLike =>
        x.arrayType.fold[ValidationNel[SemanticError, Type]](
          failure(NonEmptyList(GenericError("internal error: arrayLike, but no arrayType"))))(
          success)
    },
    untyper(tpe => success(List(FlexArr(0, None, tpe)))))

  val FlattenMapKeys = Expansion(
    "{*:}",
    "Zooms in on the keys of a map, also extending the current dimension with the keys",
    Top, AnyObject :: Nil,
    noSimplification,
    partialTyper {
      case List(Const(Data.Obj(map))) =>
        Const(Data.Set(map.keys.toList ∘ Data.Str))
      case List(x) if x.objectLike => Str
    },
    untyper(tpe => success(List(Obj(Map(), Some(Top))))))

  val FlattenArrayIndices = Expansion(
    "[*:]",
    "Zooms in on the indices of an array, also extending the current dimension with the indices",
    Int, AnyArray :: Nil,
    noSimplification,
    partialTyper { case List(x) if x.arrayLike => Int },
    partialUntyper { case Int => List(FlexArr(0, None, Top)) })

  val ShiftMap = Expansion(
    "SHIFT_MAP",
    "Zooms in on the values of a map, adding the keys as a new dimension",
    Top, AnyObject :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case InvokeF(_, List(Embed(UnshiftMap(List(Embed(set)))))) => set.some
        case _                                                     => None
      }
    },
    partialTyperV {
      case List(x) if x.objectLike =>
        x.objectType.fold[ValidationNel[SemanticError, Type]](
          failure(NonEmptyList(GenericError("internal error: objectLike, but no objectType"))))(
          success)
    },
    untyper(tpe => success(List(Obj(Map(), Some(tpe))))))

  val ShiftArray = Expansion(
    "SHIFT_ARRAY",
    "Zooms in on the elements of an array, adding the indices as a new dimension",
    Top, AnyArray :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case InvokeF(_, List(Embed(UnshiftArray(List(Embed(set)))))) => set.some
        case _                                                       => None
      }
    },
    partialTyperV {
      case List(Const(Data.Arr(elems))) => success(Const(Data.Set(elems)))
      case List(x) if x.arrayLike =>
        x.arrayType.fold[ValidationNel[SemanticError, Type]](
          failure(NonEmptyList(GenericError("internal error: arrayLike, but no arrayType"))))(
          success)
    },
    untyper(tpe => success(List(FlexArr(0, None, tpe)))))

  val ShiftMapKeys = Expansion(
    "{_:}",
    "Zooms in on the keys of a map, also adding the keys as a new dimension",
    Top, AnyObject :: Nil,
    noSimplification,
    partialTyper { case List(x) if x.objectLike => Str },
    untyper(tpe => success(List(Obj(Map(), Some(Top))))))

  val ShiftArrayIndices = Expansion(
    "[_:]",
    "Zooms in on the indices of an array, also adding the keys as a new dimension",
    Int, AnyArray :: Nil,
    noSimplification,
    partialTyper { case List(x) if x.arrayLike => Int },
    partialUntyper { case Int => List(FlexArr(0, None, Top)) })

  val UnshiftMap: Func = Reduction(
    "{...}",
    "Unshifts a dimension from the set identity, creating a map with the dimensional values as the keys.",
    AnyObject, Top :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case InvokeF(_, List(Embed(ShiftMap(List(Embed(map)))))) => map.some
        case _                                                   => None
      }
    },
    partialTyper { case List(tpe) => Obj(Map(), Some(tpe)) },
    partialUntyperV { case tpe if tpe.objectLike =>
      tpe.objectType.fold[ValidationNel[SemanticError, List[Type]]](
        failure(NonEmptyList(GenericError("internal error: objectLike, but no objectType"))))(
        x => success(List(x)))
    })

  val UnshiftArray: Func = Reduction(
    "[...]",
    "Unshifts an integral dimension from the set identity, creating an array with the dimensional values as the indices.",
    AnyArray, Top :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case InvokeF(_, List(Embed(ShiftArray(List(Embed(array)))))) => array.some
        case InvokeF(_, List(Embed(ShiftArrayIndices(List(Embed(ConstantF(Data.Arr(array)))))))) =>
          ConstantF(Data.Arr((0 until array.length).toList ∘ (Data.Int(_)))).some
        case InvokeF(_, List(Embed(ShiftMap(List(Embed(ConstantF(Data.Obj(map)))))))) =>
          ConstantF(Data.Arr(map.values.toList)).some
        case InvokeF(_, List(Embed(ShiftMapKeys(List(Embed(ConstantF(Data.Obj(map)))))))) =>
          ConstantF(Data.Arr(map.keys.toList.map(Data.Str(_)))).some
        case _ => None
      }
    },
    partialTyper {
      // case List(Const(Data.Set(vs))) => Const(Data.Arr(vs))
      case List(Const(v))            => Const(Data.Arr(List(v)))
      case List(tpe)                 => FlexArr(0, None, tpe)
    },
    partialUntyperV { case tpe if tpe.arrayLike =>
      tpe.arrayType.fold[ValidationNel[SemanticError, List[Type]]](
        failure(NonEmptyList(GenericError("internal error: arrayLike, but no arrayType"))))(
        x => success(List(x)))
    })

  def functions = MakeObject :: MakeArray ::
                  ObjectConcat :: ArrayConcat :: ConcatOp ::
                  ObjectProject :: ArrayProject ::
                  FlattenMapKeys :: FlattenMap :: FlattenArrayIndices :: FlattenArray ::
                  ShiftMapKeys :: ShiftMap :: ShiftArrayIndices :: ShiftArray ::
                  UnshiftMap :: UnshiftArray ::
                  Nil

  // TODO: fix types and add the VirtualFuncs to the list of functions

  // val MakeObjectN = new VirtualFunc {
  object MakeObjectN {
    // Note: signature does not match VirtualFunc
    def apply[T[_[_]]: Corecursive](args: (T[LogicalPlan], T[LogicalPlan])*): LogicalPlan[T[LogicalPlan]] =
      args.toList match {
        case Nil      => ConstantF(Data.Obj(ListMap()))
        case x :: xs  =>
          xs.foldLeft(MakeObject(x._1, x._2))((a, b) =>
            ObjectConcat(a.embed, MakeObject(b._1, b._2).embed))
      }

    // Note: signature does not match VirtualFunc
    def unapply[T[_[_]]: Recursive](t: LogicalPlan[T[LogicalPlan]]):
        Option[List[(T[LogicalPlan], T[LogicalPlan])]] =
      t match {
        case MakeObject(List(name, expr)) => Some(List((name, expr)))
        case ObjectConcat(List(a, b))     => (unapply(a.project) ⊛ unapply(b.project))(_ ::: _)
        case _                            => None
      }
  }

  object MakeArrayN {
    def apply[T[_[_]]: Corecursive](args: T[LogicalPlan]*): LogicalPlan[T[LogicalPlan]] =
      args.map(MakeArray(_)) match {
        case Nil      => ConstantF(Data.Arr(Nil))
        case t :: Nil => t
        case mas      => mas.reduce((t, ma) => ArrayConcat(t.embed, ma.embed))
      }

    def unapply[T[_[_]]: Recursive](t: T[LogicalPlan]): Option[List[T[LogicalPlan]]] =
      t.project match {
        case MakeArray(x :: Nil)        => Some(x :: Nil)
        case ArrayConcat(a :: b :: Nil) => (unapply(a) ⊛ unapply(b))(_ ::: _)
        case _                          => None
      }

    object Attr {
      def unapply[A](t: Cofree[LogicalPlan, A]):
          Option[List[Cofree[LogicalPlan, A]]] =
        MakeArrayN.unapply[Cofree[?[_], A]](t)
    }
  }
}
object StructuralLib extends StructuralLib
