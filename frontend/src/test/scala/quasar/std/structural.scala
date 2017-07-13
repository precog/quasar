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

import scala.Predef.$conforms
import slamdata.Predef._
import quasar._

import org.specs2.scalaz._
import scalaz._, Scalaz._

class StructuralSpecs extends quasar.Qspec with ValidationMatchers {
  import quasar.Type._

  import StructuralLib._

  import DataArbitrary._
  import TypeArbitrary._

  "ConcatOp" should {
    // NB: the func's domain is the type assigned to any argument if nothing
    // else is known about it.
    val unknown = AnyArray ⨿ Str

    def stringToList(s: String): List[Data] =
      s.map(c => Data.Str(c.toString)).toList

    "type combination of arbitrary strs as str" >> prop { (st1: Type, st2: Type) =>
      ConcatOp.tpe(Func.Input2(st1, st2)).map(Str contains _) must beSuccessful(true)
      ConcatOp.tpe(Func.Input2(st2, st1)).map(Str contains _) must beSuccessful(true)
    }.setArbitrary1(arbStrType).setArbitrary2(arbStrType)

    "type arbitrary str || unknown as Str" >> prop { (st: Type) =>
      ConcatOp.tpe(Func.Input2(st, unknown)) must beSuccessful(Str)
      ConcatOp.tpe(Func.Input2(unknown, st)) must beSuccessful(Str)
    }.setArbitrary(arbStrType)

    "fold constant Strings" >> prop { (s1: String, s2: String) =>
      ConcatOp.tpe(Func.Input2(Const(Data.Str(s1)), Const(Data.Str(s2)))) must
        beSuccessful(Const(Data.Str(s1 + s2)))
    }

    "type combination of arbitrary arrays as array" >> prop { (at1: Type, at2: Type) =>
      ConcatOp.tpe(Func.Input2(at1, at2)).map(_.arrayLike) must beSuccessful(true)
      ConcatOp.tpe(Func.Input2(at2, at1)).map(_.arrayLike) must beSuccessful(true)
    }.setArbitrary1(arbArrayType).setArbitrary2(arbArrayType)

    "type arbitrary array || unknown as array" >> prop { (at: Type) =>
      ConcatOp.tpe(Func.Input2(at, unknown)).map(_.arrayLike) must beSuccessful(true)
      ConcatOp.tpe(Func.Input2(unknown, at)).map(_.arrayLike) must beSuccessful(true)
    }.setArbitrary(arbArrayType)

    "fold constant arrays" >> prop { (ds1: List[Data], ds2: List[Data]) =>
      ConcatOp.tpe(Func.Input2(Const(Data.Arr(ds1)), Const(Data.Arr(ds2)))) must
        beSuccessful(Const(Data.Arr(ds1 ++ ds2)))
    }

    "type mixed Str and array args as array" >> prop { (st: Type, at: Type) =>
      ConcatOp.tpe(Func.Input2(st, at)).map(_.arrayLike) must beSuccessful(true)
      ConcatOp.tpe(Func.Input2(at, st)).map(_.arrayLike) must beSuccessful(true)
    }.setArbitrary1(arbStrType).setArbitrary2(arbArrayType)

    "fold constant string || array" >> prop { (s: String, xs: List[Data]) =>
      ConcatOp.tpe(Func.Input2(Const(Data.Str(s)), Const(Data.Arr(xs)))) must
        beSuccessful(Const(Data.Arr(stringToList(s) ::: xs)))
    }

    "fold constant array || string" >> prop { (s: String, xs: List[Data]) =>
      ConcatOp.tpe(Func.Input2(Const(Data.Arr(xs)), Const(Data.Str(s)))) must
        beSuccessful(Const(Data.Arr(xs ::: stringToList(s))))
    }

    "propagate unknown types" in {
      ConcatOp.tpe(Func.Input2(unknown, unknown)) must beSuccessful(unknown)
    }
  }

  "FlattenMap" should {
    "only accept maps" >> prop { (nonMap: Type) =>
      FlattenMap.tpe(Func.Input1(nonMap)) must beFailing
    }.setArbitrary(arbArrayType)

    "convert from a map type to the type of its values" in {
      FlattenMap.tpe(Func.Input1(Obj(Map(), Str.some))) must beSuccessful(Str)
    }

    "untype to a map type from some value type" in {
      FlattenMap.untpe(Str).map(_.unsized) must beSuccessful(List(Obj(Map(), Str.some)))
    }
  }

  "FlattenMapKeys" should {
    "only accept maps" >> prop { (nonMap: Type) =>
      FlattenMapKeys.tpe(Func.Input1(nonMap)) must beFailing
    }.setArbitrary(arbArrayType)

    "convert from a map type to the type of its keys" in {
      FlattenMapKeys.tpe(Func.Input1(Obj(Map(), Int.some))) must beSuccessful(Str)
    }

    "untype to a map type from some key type" in {
      FlattenMapKeys.untpe(Str).map(_.unsized) must beSuccessful(List(Obj(Map(), Top.some)))
    }
  }

  "FlattenArray" should {
    "only accept arrays" >> prop { (nonArr: Type) =>
      FlattenArray.tpe(Func.Input1(nonArr)) must beFailing
    }.setArbitrary(arbStrType)

    "convert from an array type to the type of its values" in {
      FlattenArray.tpe(Func.Input1(FlexArr(0, None, Str))) must beSuccessful(Str)
    }

    "untype to an array type from some value type" in {
      FlattenArray.untpe(Str).map(_.unsized) must beSuccessful(List(FlexArr(0, None, Str)))
    }
  }

  "FlattenArrayIndices" should {
    "only accept arrays" >> prop { (nonArr: Type) =>
      FlattenArrayIndices.tpe(Func.Input1(nonArr)) must beFailing
    }.setArbitrary(arbStrType)

    "convert from an array type to int" in {
      FlattenArrayIndices.tpe(Func.Input1(FlexArr(0, None, Str))) must beSuccessful(Int)
    }

    "untype to an array type from int" in {
      FlattenArrayIndices.untpe(Int).map(_.unsized) must
        beSuccessful(List(FlexArr(0, None, Top)))
    }

    "only untype from ints" >> prop { (nonInt: Type) =>
      FlattenArrayIndices.untpe(nonInt).map(_.unsized) must beFailing
    }.setArbitrary(arbStrType)
  }

  "ShiftMap" should {
    "only accept maps" >> prop { (nonMap: Type) =>
      ShiftMap.tpe(Func.Input1(nonMap)) must beFailing
    }.setArbitrary(arbArrayType)

    "convert from a map type to the type of its values" in {
      ShiftMap.tpe(Func.Input1(Obj(Map(), Str.some))) must beSuccessful(Str)
    }

    "untype to a map type from some value type" in {
      ShiftMap.untpe(Str).map(_.unsized) must beSuccessful(List(Obj(Map(), Str.some)))
    }
  }

  "ShiftMapKeys" should {
    "only accept maps" >> prop { (nonMap: Type) =>
      ShiftMapKeys.tpe(Func.Input1(nonMap)) must beFailing
    }.setArbitrary(arbArrayType)

    "convert from a map type to the type of its keys" in {
      ShiftMapKeys.tpe(Func.Input1(Obj(Map(), Int.some))) must beSuccessful(Str)
    }

    "untype to a map type from some key type" in {
      ShiftMapKeys.untpe(Str).map(_.unsized) must beSuccessful(List(Obj(Map(), Top.some)))
    }
  }

  "ShiftArray" should {
    "only accept arrays" >> prop { (nonArr: Type) =>
      ShiftArray.tpe(Func.Input1(nonArr)) must beFailing
    }.setArbitrary(arbStrType)

    "convert from an array type to the type of its values" in {
      ShiftArray.tpe(Func.Input1(FlexArr(0, None, Str))) must beSuccessful(Str)
    }

    "untype to an array type from some value type" in {
      ShiftArray.untpe(Str).map(_.unsized) must beSuccessful(List(FlexArr(0, None, Str)))
    }
  }

  "ShiftArrayIndices" should {
    "only accept arrays" >> prop { (nonArr: Type) =>
      ShiftArrayIndices.tpe(Func.Input1(nonArr)) must beFailing
    }.setArbitrary(arbStrType)

    "convert from an array type to int" in {
      ShiftArrayIndices.tpe(Func.Input1(FlexArr(0, None, Str))) must beSuccessful(Int)
    }

    "untype to an array type from int" in {
      ShiftArrayIndices.untpe(Int).map(_.unsized) must
        beSuccessful(List(FlexArr(0, None, Top)))
    }

    "only untype from ints" >> prop { (nonInt: Type) =>
      ShiftArrayIndices.untpe(nonInt).map(_.unsized) must beFailing
    }.setArbitrary(arbStrType)
  }

  "UnshiftMap" should {
    "convert to a map type from some value type" in {
      UnshiftMap.tpe(Func.Input2(Top, Str)) must beSuccessful(Obj(Map(), Str.some))
    }

    "untype from a map type to the type of its values" in {
      UnshiftMap.untpe(Obj(Map(), Str.some)).map(_.unsized) must beSuccessful(List(Top, Str))
    }
  }

  "UnshiftArray" should {
    "convert to an array type from some value type" in {
      UnshiftArray.tpe(Func.Input1(Str)) must beSuccessful(FlexArr(0, None, Str))
    }

    "untype from an array type to the type of its values" in {
      UnshiftArray.untpe(FlexArr(0, None, Str)).map(_.unsized) must beSuccessful(List(Str))
    }
  }

  import org.scalacheck.Gen, Gen._
  import org.scalacheck.Arbitrary, Arbitrary._
  lazy val arbStrType: Arbitrary[Type] = Arbitrary(Gen.oneOf(
    const(Str),
    arbitrary[String].map(s => Const(Data.Str(s)))))

  lazy val arbArrayType: Arbitrary[Type] = Arbitrary(simpleArrayGen)
  lazy val simpleArrayGen = Gen.oneOf(
    for {
      i <- arbitrary[Int]
      n <- arbitrary[Option[Int]]
      t <- arbitrary[Type]
    } yield FlexArr(i.abs, n.map(i.abs max _.abs), t),
    for {
      t <- arbitrary[List[Type]]
    } yield Arr(t),
    for {
      ds <- resize(5, arbitrary[List[Data]])
    } yield Const(Data.Arr(ds)))
}
