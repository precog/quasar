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

package quasar.yggdrasil

import quasar.blueeyes._
import quasar.precog.common._
import quasar.blueeyes.json._

import scalaz._
import scalaz.Ordering._
import scalaz.Either3._
import scalaz.std.tuple._
import scalaz.syntax.comonad._

import org.specs2._
import org.scalacheck._, Gen._

import scala.annotation.tailrec

trait CogroupSpec[M[+_]] extends TableModuleTestSupport[M] with SpecificationLike with ScalaCheck {
  import SampleData._
  import trans._
  import trans.constants._

  implicit val cogroupData = Arbitrary(
    for {
      depth <- choose(1, 2)
      cschema <- Gen.oneOf(arraySchema(depth, 2), objectSchema(depth, 2))
      (idCount, data) <- genEventColumns(cschema)
    } yield {
      val (lschema, rschema) = Bifunctor[Tuple2].umap(cschema.splitAt(cschema.size / 2)) { _.map(_._1).toSet }
      val (l, r) =  data map {
                      case (ids, values) =>
                        val (d1, d2) = values.partition { case (cpath, _) => lschema.contains(cpath) }
                        (toRecord(ids, assemble(d1)), toRecord(ids, assemble(d2)))
                    } unzip

      (SampleData(l.sortBy(_ \ "key").toStream), SampleData(r.sortBy(_ \ "key").toStream))
    }
  )

  type CogroupResult[A] = Stream[Either3[A, (A, A), A]]

  @tailrec protected final def computeCogroup[A](l: Stream[A], r: Stream[A], acc: CogroupResult[A])(implicit ord: Order[A]): CogroupResult[A] = {
    (l, r) match {
      case (lh #:: lt, rh #:: rt) => ord.order(lh, rh) match {
        case EQ => {
          val (leftSpan, leftRemain) = l.partition(ord.order(_, lh) == EQ)
          val (rightSpan, rightRemain) = r.partition(ord.order(_, rh) == EQ)

          val cartesian = leftSpan.flatMap { lv => rightSpan.map { rv => middle3((lv, rv)) } }

          computeCogroup(leftRemain, rightRemain, acc ++ cartesian)
        }
        case LT => {
          val (leftRun, leftRemain) = l.partition(ord.order(_, rh) == LT)

          computeCogroup(leftRemain, r, acc ++ leftRun.map { case v => left3(v) })
        }
        case GT => {
          val (rightRun, rightRemain) = r.partition(ord.order(lh, _) == GT)

          computeCogroup(l, rightRemain, acc ++ rightRun.map { case v => right3(v) })
        }
      }
      case (Stream.Empty, _) => acc ++ r.map { case v => right3(v) }
      case (_, Stream.Empty) => acc ++ l.map { case v => left3(v) }
    }
  }

  def testCogroup(l: SampleData, r: SampleData) = {
    val ltable = fromSample(l)
    val rtable = fromSample(r)

    val keyOrder = Order[JValue].contramap((_: JValue) \ "key")

    val expected = computeCogroup(l.data, r.data, Stream())(keyOrder) map {
      case Left3(jv) => jv
      case Middle3((jv1, jv2)) =>
        JObject(
          JField("key", jv1 \ "key"),
          JField("valueLeft", jv1 \ "value"),
          JField("valueRight", jv2 \ "value"))
      case Right3(jv) => jv
    }

    val result: Table = ltable.cogroup(SourceKey.Single, SourceKey.Single, rtable)(
      Leaf(Source),
      Leaf(Source),
      OuterObjectConcat(WrapObject(SourceKey.Left, "key"), OuterObjectConcat(WrapObject(SourceValue.Left, "valueLeft"), WrapObject(SourceValue.Right, "valueRight"))))

    val jsonResult = toJson(result)

    jsonResult.copoint must_== expected
  }

  def testTrivialCogroup(f: Table => Table = identity[Table]) = {
    def recl = toRecord(Array(0), JArray(JNum(12) :: Nil))
    def recr = toRecord(Array(0), JArray(JUndefined :: JNum(13) :: Nil))

    val ltable = fromSample(SampleData(Stream(recl)))
    val rtable = fromSample(SampleData(Stream(recr)))

    val expected = Vector(toRecord(Array(0), JArray(JNum(12) :: JUndefined :: JNum(13) :: Nil)))

    val result: Table = ltable.cogroup(SourceKey.Single, SourceKey.Single, rtable)(
      Leaf(Source),
      Leaf(Source),
      OuterObjectConcat(WrapObject(SourceKey.Left, "key"), WrapObject(OuterArrayConcat(SourceValue.Left, SourceValue.Right), "value"))
    )

    val jsonResult = toJson(f(result))
    jsonResult.copoint must_== expected
  }

  def testTrivialNoRecordCogroup(f: Table => Table = identity[Table]) = {
    def recl = JNum(12)
    def recr = JNum(13)

    val ltable = fromSample(SampleData(Stream(recl)))
    val rtable = fromSample(SampleData(Stream(recr)))

    val expected = Vector(JNum(12), JNum(13))

    val result: Table = ltable.cogroup(Leaf(Source), Leaf(Source), rtable)(
      Leaf(Source),
      Leaf(Source),
      OuterArrayConcat(SourceValue.Left, SourceValue.Right))

    val jsonResult = toJson(f(result))
    jsonResult.copoint must_== expected
  }

  def testSimpleCogroup(f: Table => Table = identity[Table]) = {
    def recl(i: Int) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)))))
    def recr(i: Int) = toRecord(Array(i), JObject(List(JField("right", JString(i.toString)))))
    def recBoth(i: Int) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)), JField("right", JString(i.toString)))))

    val ltable  = fromSample(SampleData(Stream(recl(0), recl(1), recl(3), recl(3), recl(5), recl(7), recl(8), recl(8))))
    val rtable  = fromSample(SampleData(Stream(recr(0), recr(2), recr(3), recr(4), recr(5), recr(5), recr(6), recr(8), recr(8))))

    val expected = Vector(
      recBoth(0),
      recl(1),
      recr(2),
      recBoth(3),
      recBoth(3),
      recr(4),
      recBoth(5),
      recBoth(5),
      recr(6),
      recl(7),
      recBoth(8),
      recBoth(8),
      recBoth(8),
      recBoth(8)
    )

    val result: Table = ltable.cogroup(SourceKey.Single, SourceKey.Single, rtable)(
      Leaf(Source),
      Leaf(Source),
      OuterObjectConcat(WrapObject(SourceKey.Left, "key"), WrapObject(OuterObjectConcat(SourceValue.Left, SourceValue.Right), "value"))
    )

    val jsonResult = toJson(f(result))
    jsonResult.copoint must_== expected
  }

  def testUnionCogroup = {
    def recl(i: Int, j: Int) = toRecord(Array(i), JObject(List(JField("left", JNum(j)))))
    def recr(i: Int, j: Int) = toRecord(Array(i), JObject(List(JField("right", JNum(j)))))

    val ltable  = fromSample(SampleData(Stream(recl(0, 1), recl(1, 12), recl(3, 13), recl(4, 42), recl(5, 77))))
    val rtable  = fromSample(SampleData(Stream(recr(6, -1), recr(7, 0), recr(8, 14), recr(9, 42), recr(10, 77))))

    val expected = Vector(
      recl(0, 1), recl(1, 12), recl(3, 13), recl(4, 42), recl(5, 77),
      recr(6, -1), recr(7, 0), recr(8, 14), recr(9, 42), recr(10, 77)
    )

    val result: Table = ltable.cogroup(SourceKey.Single, SourceKey.Single, rtable)(
      Leaf(Source),
      Leaf(Source),
      OuterObjectConcat(WrapObject(SourceKey.Left, "key"), WrapObject(OuterObjectConcat(SourceValue.Left, SourceValue.Right), "value"))
    )

    val jsonResult = toJson(result)
    jsonResult.copoint must_== expected
  }

  def testAnotherSimpleCogroup = {
    def recl(i: Int) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)))))
    def recr(i: Int) = toRecord(Array(i), JObject(List(JField("right", JString(i.toString)))))
    def recBoth(i: Int) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)), JField("right", JString(i.toString)))))

    val ltable  = fromSample(SampleData(Stream(recl(2), recl(3), recl(4), recl(6), recl(7))))
    val rtable  = fromSample(SampleData(Stream(recr(0), recr(1), recr(5), recr(6), recr(7))))

    val expected = Vector(
      recr(0),
      recr(1),
      recl(2),
      recl(3),
      recl(4),
      recr(5),
      recBoth(6),
      recBoth(7)
    )

    val result: Table = ltable.cogroup(SourceKey.Single, SourceKey.Single, rtable)(
      Leaf(Source),
      Leaf(Source),
      OuterObjectConcat(WrapObject(SourceKey.Left, "key"), WrapObject(OuterObjectConcat(SourceValue.Left, SourceValue.Right), "value"))
    )

    val jsonResult = toJson(result)
    jsonResult.copoint must_== expected
  }

  def testAnotherSimpleCogroupSwitched = {
    def recl(i: Int) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)))))
    def recr(i: Int) = toRecord(Array(i), JObject(List(JField("right", JString(i.toString)))))
    def recBoth(i: Int) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)), JField("right", JString(i.toString)))))

    val rtable  = fromSample(SampleData(Stream(recr(2), recr(3), recr(4), recr(6), recr(7))))
    val ltable  = fromSample(SampleData(Stream(recl(0), recl(1), recl(5), recl(6), recl(7))))

    val expected = Vector(
      recl(0),
      recl(1),
      recr(2),
      recr(3),
      recr(4),
      recl(5),
      recBoth(6),
      recBoth(7)
    )

    val result: Table = ltable.cogroup(SourceKey.Single, SourceKey.Single, rtable)(
      Leaf(Source),
      Leaf(Source),
      OuterObjectConcat(WrapObject(SourceKey.Left, "key"), WrapObject(OuterObjectConcat(SourceValue.Left, SourceValue.Right), "value"))
    )

    val jsonResult = toJson(result)
    jsonResult.copoint must_== expected
  }

  def testUnsortedInputs = {
    def recl(i: Int) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)))))
    def recr(i: Int) = toRecord(Array(i), JObject(List(JField("right", JString(i.toString)))))

    val ltable  = fromSample(SampleData(Stream(recl(0), recl(1))))
    val rtable  = fromSample(SampleData(Stream(recr(1), recr(0))))

    toJson(ltable.cogroup(SourceKey.Single, SourceKey.Single, rtable)(
      Leaf(Source),
      Leaf(Source),
      OuterObjectConcat(WrapObject(SourceKey.Left, "key"), WrapObject(OuterObjectConcat(SourceValue.Left, SourceValue.Right), "value"))
    )).copoint must throwAn[Exception]
  }

  def testCogroupPathology1 = {
    import JParser.parseUnsafe
    val s1 = SampleData(Stream(toRecord(Array(1, 1, 1), parseUnsafe("""{ "a":[] }"""))))
    val s2 = SampleData(Stream(toRecord(Array(1, 1, 1), parseUnsafe("""{ "b":0 }"""))))

    testCogroup(s1, s2)
  }

  def testCogroupSliceBoundaries = {
    import JParser.parseUnsafe

    val s1 = SampleData(Stream(
      toRecord(Array(1), parseUnsafe("""{ "ruoh5A25Jaxa":-1.0 }""")),
      toRecord(Array(2), parseUnsafe("""{ "ruoh5A25Jaxa":-2.735023101944097E37 }""")),
      toRecord(Array(3), parseUnsafe("""{ "ruoh5A25Jaxa":2.12274644226519E38 }""")),
      toRecord(Array(4), parseUnsafe("""{ "ruoh5A25Jaxa":1.085656944502855E38 }""")),
      toRecord(Array(5), parseUnsafe("""{ "ruoh5A25Jaxa":-3.4028234663852886E38 }""")),
      toRecord(Array(6), parseUnsafe("""{ "ruoh5A25Jaxa":-1.0 }""")),
      toRecord(Array(7), parseUnsafe("""{ "ruoh5A25Jaxa":-3.4028234663852886E38 }""")),
      toRecord(Array(8), parseUnsafe("""{ "ruoh5A25Jaxa":2.4225587899613125E38 }""")),
      toRecord(Array(9), parseUnsafe("""{ "ruoh5A25Jaxa":-3.078101074510345E38 }""")),
      toRecord(Array(10), parseUnsafe("""{ "ruoh5A25Jaxa":0.0 }""")),
      toRecord(Array(11), parseUnsafe("""{ "ruoh5A25Jaxa":-2.049657967962047E38 }"""))
    ))

    val s2 = SampleData(Stream(
      toRecord(Array(1), parseUnsafe("""{ "mbsn8ya":-629648309198725501 }""")),
      toRecord(Array(2), parseUnsafe("""{ "mbsn8ya":-1642079669762657762 }""")),
      toRecord(Array(3), parseUnsafe("""{ "mbsn8ya":-75462980385303464 }""")),
      toRecord(Array(4), parseUnsafe("""{ "mbsn8ya":-4407493923710190330 }""")),
      toRecord(Array(5), parseUnsafe("""{ "mbsn8ya":4611686018427387903 }""")),
      toRecord(Array(6), parseUnsafe("""{ "mbsn8ya":-4374327062386862583 }""")),
      toRecord(Array(7), parseUnsafe("""{ "mbsn8ya":1920642186250198767 }""")),
      toRecord(Array(8), parseUnsafe("""{ "mbsn8ya":1 }""")),
      toRecord(Array(9), parseUnsafe("""{ "mbsn8ya":0 }""")),
      toRecord(Array(10), parseUnsafe("""{ "mbsn8ya":1 }""")),
      toRecord(Array(11), parseUnsafe("""{ "mbsn8ya":758880641626989193 }"""))
    ))

    testCogroup(s1, s2)
  }

  def testCogroupPathology2 = {
    val s1 = SampleData(Stream(
      toRecord(Array(19,49,71), JArray(JNum(-4611686018427387904l) :: Nil)),
      toRecord(Array(28,15,27), JArray(JNum(-4611686018427387904l) :: Nil)),
      toRecord(Array(33,11,79), JArray(JNum(-1330862996622233403l) :: Nil)),
      toRecord(Array(38,9,3),   JArray(JNum(483746605685223474l) :: Nil)),
      toRecord(Array(44,75,87), JArray(JNum(4611686018427387903l) :: Nil)),
      toRecord(Array(46,47,10), JArray(JNum(-4611686018427387904l) :: Nil)),
      toRecord(Array(47,17,78), JArray(JNum(3385965380985908250l) :: Nil)),
      toRecord(Array(47,89,84), JArray(JNum(-3713232335731560170l) :: Nil)),
      toRecord(Array(48,47,76), JArray(JNum(4611686018427387903l) :: Nil)),
      toRecord(Array(49,66,33), JArray(JNum(-1592288472435607010l) :: Nil)),
      toRecord(Array(50,9,89),  JArray(JNum(-3610518022153967388l) :: Nil)),
      toRecord(Array(59,54,72), JArray(JNum(4178019033671378504l) :: Nil)),
      toRecord(Array(59,80,38), JArray(JNum(0) :: Nil)),
      toRecord(Array(61,59,15), JArray(JNum(1056424478602208129l) :: Nil)),
      toRecord(Array(65,34,89), JArray(JNum(4611686018427387903l) :: Nil)),
      toRecord(Array(73,52,67), JArray(JNum(-4611686018427387904l) :: Nil)),
      toRecord(Array(74,60,85), JArray(JNum(-4477191148386604184l) :: Nil)),
      toRecord(Array(76,41,86), JArray(JNum(-2686421995147680512l) :: Nil)),
      toRecord(Array(77,46,75), JArray(JNum(-1) :: Nil)),
      toRecord(Array(77,65,58), JArray(JNum(-4032275398385636682l) :: Nil)),
      toRecord(Array(86,50,9),  JArray(JNum(4163435383002324073l) :: Nil))
    ))

    val s2 = SampleData(Stream(
      toRecord(Array(19,49,71), JArray(JUndefined :: JNum(2.2447601450142614E38) :: Nil)),
      toRecord(Array(28,15,27), JArray(JUndefined :: JNum(-1.0) :: Nil)),
      toRecord(Array(33,11,79), JArray(JUndefined :: JNum(-3.4028234663852886E38) :: Nil)),
      toRecord(Array(38,9,3),   JArray(JUndefined :: JNum(3.4028234663852886E38) :: Nil)),
      toRecord(Array(44,75,87), JArray(JUndefined :: JNum(3.4028234663852886E38) :: Nil)),
      toRecord(Array(46,47,10), JArray(JUndefined :: JNum(-7.090379511750481E37) :: Nil)),
      toRecord(Array(47,17,78), JArray(JUndefined :: JNum(2.646265046453461E38) :: Nil)),
      toRecord(Array(47,89,84), JArray(JUndefined :: JNum(0.0) :: Nil)),
      toRecord(Array(48,47,76), JArray(JUndefined :: JNum(1.3605700991092947E38) :: Nil)),
      toRecord(Array(49,66,33), JArray(JUndefined :: JNum(-1.4787158449349019E38) :: Nil)),
      toRecord(Array(50,9,89),  JArray(JUndefined :: JNum(-1.0) :: Nil)),
      toRecord(Array(59,54,72), JArray(JUndefined :: JNum(-3.4028234663852886E38) :: Nil)),
      toRecord(Array(59,80,38), JArray(JUndefined :: JNum(8.51654525599509E37) :: Nil)),
      toRecord(Array(61,59,15), JArray(JUndefined :: JNum(3.4028234663852886E38) :: Nil)),
      toRecord(Array(65,34,89), JArray(JUndefined :: JNum(-1.0) :: Nil)),
      toRecord(Array(73,52,67), JArray(JUndefined :: JNum(5.692401753312787E37) :: Nil)),
      toRecord(Array(74,60,85), JArray(JUndefined :: JNum(2.5390881291535566E38) :: Nil)),
      toRecord(Array(76,41,86), JArray(JUndefined :: JNum(-6.05866505535721E37) :: Nil)),
      toRecord(Array(77,46,75), JArray(JUndefined :: JNum(0.0) :: Nil)),
      toRecord(Array(77,65,58), JArray(JUndefined :: JNum(1.0) :: Nil)),
      toRecord(Array(86,50,9),  JArray(JUndefined :: JNum(-3.4028234663852886E38) :: Nil))
    ))

    testCogroup(s1, s2)
  }

  def testCogroupPathology3 = {
    import JParser.parseUnsafe
    val s1 = SampleData(Stream(
      parseUnsafe("""{ "value":{ "ugsrry":3.0961191760668197E+307 }, "key":[2.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":0.0 }, "key":[3.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":3.323617580854415E+307 }, "key":[5.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":-9.458984438931391E+306 }, "key":[6.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":1.0 }, "key":[10.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":0.0 }, "key":[13.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":-3.8439741460685273E+307 }, "key":[14.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":5.690895589711475E+307 }, "key":[15.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":0.0 }, "key":[16.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":-5.567237049482096E+307 }, "key":[17.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":-8.988465674311579E+307 }, "key":[18.0] }"""),
      parseUnsafe("""{ "value":{ "ugsrry":2.5882896341488965E+307 }, "key":[22.0] }""")
    ))

    val s2 = SampleData(Stream(
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[-1E-40146] }, "key":[2.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[-9.44770762864723688E-39073] }, "key":[3.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[2.894611552200768372E+19] }, "key":[5.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[-2.561276432629787073E-42575] }, "key":[6.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[-1E-10449] }, "key":[10.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[2110233717777347493] }, "key":[13.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[3.039020270015831847E+19] }, "key":[14.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[1E-50000] }, "key":[15.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[-1.296393752892965818E-49982] }, "key":[16.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[4.611686018427387903E+50018] }, "key":[17.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[0E+48881] }, "key":[18.0] }"""),
      parseUnsafe("""{ "value":{ "fzqJh5csbfsZqgkoi":[2.326724524858976798E-10633] }, "key":[22.0] }""")
    ))

    testCogroup(s1, s2)
  }

  def testPartialUndefinedCogroup = {
    import JParser.parseUnsafe

    val ltable = fromSample(SampleData(Stream(
      parseUnsafe("""{ "id" : "foo", "val" : 4 }"""))))

    val rtable = fromSample(SampleData(Stream(
      parseUnsafe("""{ "id" : "foo", "val" : 2 }"""),
      parseUnsafe("""{ "val" : 3 }"""),
      parseUnsafe("""{ "id" : "foo", "val" : 4 }"""))))

    val expected = Stream(
      parseUnsafe("""{ "id": "foo", "left": 4, "right": 2 }"""),
      parseUnsafe("""{ "id": "foo", "left": 4, "right": 4 }""")
    )

    val keySpec = DerefObjectStatic(Leaf(Source), CPathField("id"))
    val result = ltable.cogroup(keySpec, keySpec, rtable)(Leaf(Source),Leaf(Source),
      OuterObjectConcat(WrapObject(DerefObjectStatic(Leaf(SourceLeft), CPathField("id")), "id"),
                        WrapObject(DerefObjectStatic(Leaf(SourceLeft), CPathField("val")), "left"),
                        WrapObject(DerefObjectStatic(Leaf(SourceRight), CPathField("val")), "right")))

    toJson(result).copoint must_== expected
  }

  def testLongEqualSpansOnRight = {
    val record = JParser.parseUnsafe("""{"key":"Bob","value":42}""")
    val ltable = fromSample(SampleData(Stream(record)))
    val rtable = fromSample(SampleData(Stream.tabulate(22) { i =>
      JParser.parseUnsafe("""{"key":"Bob","value":%d}""" format i)
    }))

    val expected = Stream.tabulate(22)(JNum(_))

    val keySpec = DerefObjectStatic(Leaf(Source), CPathField("key"))
    val result: Table = ltable.cogroup(keySpec, keySpec, rtable)(
      WrapObject(Leaf(Source), "blah!"),
      WrapObject(Leaf(Source), "argh!"),
      DerefObjectStatic(Leaf(SourceRight), CPathField("value"))
    )

    val jsonResult = toJson(result).copoint
    jsonResult must_== expected
  }

  def testLongEqualSpansOnLeft = {
    val record = JParser.parseUnsafe("""{"key":"Bob","value":42}""")
    val ltable = fromSample(SampleData(Stream.tabulate(22) { i =>
      JParser.parseUnsafe("""{"key":"Bob","value":%d}""" format i)
    }))
    val rtable = fromSample(SampleData(Stream(record)))

    val expected = Stream.tabulate(22)(JNum(_))

    val keySpec = DerefObjectStatic(Leaf(Source), CPathField("key"))
    val result: Table = ltable.cogroup(keySpec, keySpec, rtable)(
      WrapObject(Leaf(Source), "blah!"),
      WrapObject(Leaf(Source), "argh!"),
      DerefObjectStatic(Leaf(SourceLeft), CPathField("value"))
    )

    val jsonResult = toJson(result).copoint
    jsonResult must_== expected
  }

  def testLongEqualSpansOnBoth = {
    val table = fromSample(SampleData(Stream.tabulate(22) { i =>
      JParser.parseUnsafe("""{"key":"Bob","value":%d}""" format i)
    }))

    val expected = (for {
      left <- 0 until 22
      right <- 0 until 22
    } yield {
      JParser.parseUnsafe("""{ "left": %d, "right": %d }""" format (left, right))
    }).toStream

    val keySpec = DerefObjectStatic(Leaf(Source), CPathField("key"))
    val result: Table = table.cogroup(keySpec, keySpec, table)(
      WrapObject(Leaf(Source), "blah!"),
      WrapObject(Leaf(Source), "argh!"),
      InnerObjectConcat(
        WrapObject(DerefObjectStatic(Leaf(SourceRight), CPathField("value")), "right"),
        WrapObject(DerefObjectStatic(Leaf(SourceLeft), CPathField("value")), "left")
      )
    )

    val jsonResult = toJson(result).copoint
    jsonResult must_== expected
  }

  def testLongLeftSpanWithIncreasingRight = {
    val ltable = fromSample(SampleData(Stream.tabulate(12) { i =>
      JParser.parseUnsafe("""{"key":"Bob","value":%d}""" format i)
    }))
    val rtable = fromSample(SampleData(Stream(
      JParser.parseUnsafe("""{"key":"Bob","value":50}"""),
      JParser.parseUnsafe("""{"key":"Charlie","value":60}""")
    )))


    val expected = Stream.tabulate(12) { i =>
      JParser.parseUnsafe("""{ "left": %d, "right": 50 }""" format i)
    } ++ Stream(JParser.parseUnsafe("""{ "right": 60 }"""))

    val keySpec = DerefObjectStatic(Leaf(Source), CPathField("key"))
    val result: Table = ltable.cogroup(keySpec, keySpec, rtable)(
      WrapObject(DerefObjectStatic(Leaf(Source), CPathField("value")), "left"),
      WrapObject(DerefObjectStatic(Leaf(Source), CPathField("value")), "right"),
      InnerObjectConcat(
        WrapObject(DerefObjectStatic(Leaf(SourceRight), CPathField("value")), "right"),
        WrapObject(DerefObjectStatic(Leaf(SourceLeft), CPathField("value")), "left")
      )
    )

    val jsonResult = toJson(result).copoint
    jsonResult must_== expected
  }
}
