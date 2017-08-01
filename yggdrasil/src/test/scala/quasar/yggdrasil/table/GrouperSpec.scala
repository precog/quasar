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
package table

import quasar.precog.common._
import quasar.blueeyes._, json._
import scalaz._, Scalaz._
import quasar.precog.TestSupport._

/*
Here are a number of motivating examples that are not reflected in the tests below, but are representative of solves that need to be
evaluated by the merge algorithm.

solve 'a = foo.a, 'b = foo.b
  bar' := bar where bar.b = 'b

  ['a, 'b, count(bar')]


solve 'a, 'b, 'c
  foo' := foo where foo.a = 'a & foo.b = 'b
  bar' := bar where bar.b = 'b & bar.c = 'c
  baz' := baz where baz.a = 'a & baz.c = 'c

  ['a, 'b, 'c, count(foo'), sum(bar'.d), stddev(baz'.d)]


solve 'a, 'b, 'c
  foo' := foo where foo.a = 'a & foo.b = 'b | foo.a = 'a & foo.c = 'c
  bar' := bar where bar.a = 'a | bar.b = 'b | bar.c = 'c

  [foo', bar', 'a, 'b, 'c]


solve 'a, 'b
  foo' := foo where foo.a = 'a | foo.b = 'b | foo.a = 'a & foo.b = 'b
  bar' := bar wehre bar.a = 'a & bar.b = 'b

  ...
*/

trait GrouperSpec extends SpecificationLike with ScalaCheck { self =>
  implicit def M = Need.need
  private def emptyTestModule = BlockStoreTestModule.empty[Need]

  def tic_a = CPathField("tic_a")
  def tic_b = CPathField("tic_b")

  def tic_aj = JPathField("tic_a")
  def tic_bj = JPathField("tic_b")

  implicit val fid = NaturalTransformation.refl[Need]

  val eq12F1 = CF1P("testing::eq12F1") {
    case c: DoubleColumn => new Map1Column(c) with BoolColumn {
      def apply(row: Int) = c(row) == 12.0d
    }
    case c: LongColumn => new Map1Column(c) with BoolColumn {
      def apply(row: Int) = c(row) == 12l
    }
    case c: NumColumn => new Map1Column(c) with BoolColumn {
      def apply(row: Int) = c(row.toInt) == 12
    }
  }

  def augmentWithIdentities(json: Stream[JValue]) = json.zipWithIndex map {
    case (v, i) => JObject(JField("key", JArray(JNum(i) :: Nil)) :: JField("value", v) :: Nil)
  }


  def testHistogramByValue(set: Stream[Int]) = {
    val module = emptyTestModule

    import module._
    import trans._
    import constants._

    val data = augmentWithIdentities(set.map(JNum(_)))
    val groupId = module.newGroupId

    val spec = GroupingSource(
      fromJson(data),
      SourceKey.Single, Some(TransSpec1.Id), groupId,
      GroupKeySpecSource(tic_a, SourceValue.Single))

    val result = Table.merge(spec) { (key: RValue, map: GroupId => Need[Table]) =>
      for {
        gs1  <- map(groupId)
        gs1Json <- gs1.toJson
      } yield {
        key.toJValue must beLike {
          case jo: JObject => (jo \ "tic_a") match {
            case JNum(i) => set must contain(i)
          }
        }

        val histoKey = key.toJValue(tic_aj)
        val JNum(histoKey0) = histoKey
        val histoKeyInt = histoKey0.toInt

        gs1Json must not(beEmpty)
        forall(gs1Json) { record =>
          (record \ "value") mustEqual histoKey
        }

        gs1Json.size must_== set.count(_ == histoKeyInt)

        fromJson(JNum(gs1Json.size) #:: Stream.empty)
      }
    }

    val resultIter = result.flatMap(_.toJson).copoint

    resultIter must haveSize(set.distinct.size)

    val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JNum(_) }

    forall(resultIter) { i => expectedSet must contain(i) }
  }


  def testHistogramByValueMapped(set: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data = augmentWithIdentities(set.map(JNum(_)))

    val doubleF1 = CF1P("testing::doubleF1") {
      case c: LongColumn => new Map1Column(c) with LongColumn {
        def apply(row: Int) = c(row) * 2
      }
    }

    val groupId = module.newGroupId

    val valueTrans = InnerObjectConcat(
      WrapObject(SourceKey.Single, TableModule.paths.Key.name),
      WrapObject(Map1(SourceValue.Single, doubleF1), TableModule.paths.Value.name))

    val spec = GroupingSource(
      fromJson(data),
      SourceKey.Single, Some(valueTrans), groupId,
      GroupKeySpecSource(tic_a, SourceValue.Single))

    val result = Table.merge(spec) { (key: RValue, map: GroupId => Need[Table]) =>
      for {
        gs1  <- map(groupId)
        gs1Json <- gs1.toJson
      } yield {
        key.toJValue must beLike {
          case jo: JObject => (jo \ "tic_a") match {
            case JNum(i) => set must contain(i)
          }
        }

        val histoKey = key.toJValue(tic_aj)
        val JNum(histoKey0) = histoKey
        val histoKeyInt = histoKey0.toInt

        gs1Json must not(beEmpty)
        forall(gs1Json) { record =>
          val JNum(v2) = (record \ "value")
          v2 must_== (histoKey0 * 2)
        }

        fromJson(JNum(gs1Json.size) #:: Stream.empty)
      }
    }

    val resultIter = result.flatMap(_.toJson).copoint

    resultIter must haveSize(set.distinct.size)

    val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JNum(_) }

    forall(resultIter) { i => expectedSet must contain(i) }
  }


  def testHistogramEvenOdd(set: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data = augmentWithIdentities(set.map(JNum(_)))

    val mod2 = CF1P("testing::mod2") {
      case c: LongColumn => new Map1Column(c) with LongColumn {
        def apply(row: Int) = c(row) % 2
      }
    }

    val groupId = module.newGroupId

    val spec = GroupingSource(
      fromJson(data),
      SourceKey.Single, Some(TransSpec1.Id), groupId,
      GroupKeySpecSource(tic_a, Map1(SourceValue.Single, mod2)))

    val result = Table.merge(spec) { (key: RValue, map: GroupId => Need[Table]) =>
      for {
        gs1  <- map(groupId)
        gs1Json <- gs1.toJson
      } yield {
        key.toJValue must beLike {
          case jo: JObject => (jo \ "tic_a") match {
            case JNum(i) => set.map(_ % 2) must contain(i)
          }
        }

        val histoKey = key.toJValue(tic_aj)
        val JNum(histoKey0) = histoKey
        val histoKeyInt = histoKey0.toInt

        gs1Json must not(beEmpty)
        forall(gs1Json) { record =>
          val JNum(v0) = (record \ "value")
          histoKeyInt must_== (v0.toInt % 2)
        }

        fromJson(JNum(gs1Json.size) #:: Stream.empty)
      }
    }

    val resultIter = result.flatMap(_.toJson).copoint

    resultIter must haveSize((set map { _ % 2 } distinct) size)

    val expectedSet = (set.toSeq groupBy { _ % 2 } values) map { _.length } map { JNum(_) }

    forall(resultIter) { i => expectedSet must contain(i) }
  }


  def simpleMultiKeyData = {
    val JArray(elements) = JParser.parseUnsafe("""[
      { "key": [0], "value": {"a": 12, "b": 7} },
      { "key": [1], "value": {"a": 42} },
      { "key": [2], "value": {"a": 11, "c": true} },
      { "key": [3], "value": {"a": 12} },
      { "key": [4], "value": {"b": 15} },
      { "key": [5], "value": {"b": -1, "c": false} },
      { "key": [6], "value": {"b": 7} },
      { "key": [7], "value": {"a": -7, "b": 3, "d": "testing"} }
    ]""")

    elements.toStream
  }


  def testHistogramTwoKeysAnd = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val table = fromJson(simpleMultiKeyData)

    val groupId = newGroupId

    val spec = GroupingSource(
      table,
      SourceKey.Single, Some(TransSpec1.Id), groupId,
      GroupKeySpecAnd(
        GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, CPathField("a"))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, CPathField("b")))))

    val result = Table.merge(spec) { (key, map) =>
      for {
        gs1  <- map(groupId)
        gs1Json <- gs1.toJson
      } yield {
        key.toJValue must beLike {
          case obj: JObject => {
            val a = obj(tic_aj)
            val b = obj(tic_bj)

            a must beLike {
              case JNum(i) if i == 12 => {
                b must beLike {
                  case JNum(i) if i == 7 => ok
                }
              }

              case JNum(i) if i == -7 => {
                b must beLike {
                  case JNum(i) if i == 3 => ok
                }
              }
            }
          }
        }

        gs1Json must haveSize(1)
        fromJson(Stream(JNum(gs1Json.size)))
      }
    }

    val resultJson = result.flatMap(_.toJson).copoint

    resultJson must haveSize(2)

    forall(resultJson) { v =>
      v must beLike {
        case JNum(i) if i == 1 => ok
      }
    }
  }


  def testHistogramTwoKeysOr = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val table = fromJson(simpleMultiKeyData)

    val groupId = newGroupId

    val spec = GroupingSource(
      table,
      SourceKey.Single, Some(TransSpec1.Id), groupId,
      GroupKeySpecOr(
        GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, CPathField("a"))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, CPathField("b")))))

    val result = Table.merge(spec) { (key, map) =>
      for {
        gs1  <- map(groupId)
        gs1Json <- gs1.toJson
      } yield {
        key.toJValue must beLike {
          case obj: JObject => {
            val a = obj(tic_aj)
            val b = obj(tic_bj)

            if (a == JUndefined) {
              b must beLike {
                case JNum(i) if i == 7 => gs1Json must haveSize(2)
                case JNum(i) if i == 15 => gs1Json must haveSize(1)
                case JNum(i) if i == -1 => gs1Json must haveSize(1)
                case JNum(i) if i == 3 => gs1Json must haveSize(1)
              }
            } else if (b == JUndefined) {
              a must beLike {
                case JNum(i) if i == 12 => gs1Json must haveSize(2)
                case JNum(i) if i == 42 => gs1Json must haveSize(1)
                case JNum(i) if i == 11 => gs1Json must haveSize(1)
                case JNum(i) if i == -7 => gs1Json must haveSize(1)
              }
            } else {
              a must beLike {
                case JNum(i) if i == 12 => {
                  b must beLike {
                    case JNum(i) if i == 7 => ok
                  }
                }

                case JNum(i) if i == -7 => {
                  b must beLike {
                    case JNum(i) if i == 3 => ok
                  }
                }
              }

              gs1Json must haveSize(1)
            }
          }
        }

        fromJson(Stream(JNum(gs1Json.size)))
      }
    }

    val resultJson = result.flatMap(_.toJson).copoint
    resultJson must haveSize(8)

    forall(resultJson) { v =>
      v must beLike {
        case JNum(i) if i == 2 || i == 1 => ok
      }
    }
  }


  def testHistogramExtraAnd = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val table = fromJson(simpleMultiKeyData)
    val groupId = newGroupId

    val spec = GroupingSource(
      table,
      SourceKey.Single, Some(TransSpec1.Id), groupId,
      GroupKeySpecAnd(
        GroupKeySpecSource(CPathField("extra"),
          Filter(Map1(DerefObjectStatic(SourceValue.Single, CPathField("a")), eq12F1), Map1(DerefObjectStatic(SourceValue.Single, CPathField("a")), eq12F1))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, CPathField("b")))))

    val result = Table.merge(spec) { (key, map) =>
      for {
        gs1  <- map(groupId)
        gs1Json <- gs1.toJson
      } yield {
        (key.toJValue(tic_bj)) must beLike {
          case JNum(i) => i must_== 7
        }

        gs1Json must haveSize(1)
        fromJson(Stream(JNum(gs1Json.size)))
      }
    }

    val resultJson = result.flatMap(_.toJson).copoint

    resultJson must haveSize(1)

    forall(resultJson) { v =>
      v must beLike {
        case JNum(i) if i == 1 => ok
      }
    }
  }


  def testHistogramExtraOr = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val table = fromJson(simpleMultiKeyData)
    val groupId = newGroupId

    // data where data.b = 'b | data.a = 12
    val spec = GroupingSource(
      table,
      SourceKey.Single, Some(SourceValue.Single), groupId,
      GroupKeySpecOr(
        GroupKeySpecSource(CPathField("extra"),
          Filter(Map1(DerefObjectStatic(SourceValue.Single, CPathField("a")), eq12F1), Map1(DerefObjectStatic(SourceValue.Single, CPathField("a")), eq12F1))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, CPathField("b")))))

    val result = Table.merge(spec) { (key, map) =>
      for {
        gs1  <- map(groupId)
        gs1Json <- gs1.toJson
      } yield {
        (key.toJValue(tic_bj)) must beLike {
          case JUndefined =>
            (gs1Json.head \ "a") must beLike {
              case JNum(i) if i == 12 => ok
            }

          case JNum(i) if i == 7 => gs1Json must haveSize(2)
          case JNum(i) if i == 15 => gs1Json must haveSize(1)
          case JNum(i) if i == -1 => gs1Json must haveSize(1)
          case JNum(i) if i == 3 => gs1Json must haveSize(1)
        }

        fromJson(Stream(JArray(key.toJValue :: JNum(gs1Json.size) :: Nil)))
      }
    }

    val resultJson = result.flatMap(_.toJson).copoint

    resultJson must haveSize(5)

    val JArray(expected) = JParser.parseUnsafe("""[
      [{"tic_b": 7}, 2],
      [{"tic_b": 15}, 1],
      [{"tic_b": -1}, 1],
      [{"tic_b": 3}, 1],
      [{"extra": true}, 2]
    ]""")

    resultJson.toSet must_== expected.toSet
  }


  def testCtr(rawData1: Stream[Int], rawData2: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data1 = augmentWithIdentities(rawData1.map(JNum(_)))
    val data2 = augmentWithIdentities(rawData2.map(JNum(_)))

    val table1 = fromJson(data1)
    val table2 = fromJson(data2)

    val groupId1 = newGroupId
    val groupId2 = newGroupId

    // t1 where t1 = 'a
    val spec1 = GroupingSource(
      table1,
      SourceKey.Single, Some(SourceValue.Single), groupId1,
      GroupKeySpecSource(tic_a, SourceValue.Single))

    // t2 where t2 = 'a
    val spec2 = GroupingSource(
      table2,
      SourceKey.Single, Some(SourceValue.Single), groupId2,
      GroupKeySpecSource(tic_a, SourceValue.Single))

    val intersect = GroupingAlignment(
      DerefObjectStatic(Leaf(Source), tic_a),
      DerefObjectStatic(Leaf(Source), tic_a),
      spec1,
      spec2, GroupingSpec.Intersection)

    val result = Table.merge(intersect) { (key, map) =>
      for {
        gs1  <- map(groupId1)
        gs2  <- map(groupId2)
        gs1Json <- gs1.toJson
        gs2Json <- gs2.toJson
      } yield {
        val JNum(keyBigInt) = key.toJValue(tic_aj)

        forall(gs1Json) { row =>
          row must beLike {
            case JNum(i) => i mustEqual keyBigInt
          }
        }

        gs1Json must haveSize(rawData1.count(_ == keyBigInt.toInt))
        gs2Json must haveSize(rawData2.count(_ == keyBigInt.toInt))

        fromJson(Stream(
          JObject(
            JField("key", key.toJValue(tic_aj)) ::
            JField("value", JNum(gs1Json.size + gs2Json.size)) :: Nil)))
      }
    }

    val resultJson = result.flatMap(_.toJson).copoint

    resultJson must haveSize((rawData1.toSet intersect rawData2.toSet).size)

    forall(resultJson) { record =>
      val JNum(k) = record \ "key"
      val JNum(v) = record \ "value"

      v mustEqual ((rawData1 ++ rawData2) filter { k == _ } length)
    }
  }


  def testCtrPartialJoinAnd(rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data1 = augmentWithIdentities(rawData1 map {
      case (a, b0) => JObject(JField("a", JNum(a)) :: b0.map(b => JField("b", JNum(b))).toList)
    })

    val data2 = augmentWithIdentities(rawData2 map { v => JObject(JField("a", JNum(v)) :: Nil) })

    val table1 = fromJson(data1)
    val table2 = fromJson(data2)

    val groupId1 = newGroupId
    val groupId2 = newGroupId

    val spec1 = GroupingSource(
      table1,
      SourceKey.Single, Some(SourceValue.Single), groupId1,
      GroupKeySpecAnd(
        GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, CPathField("a"))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, CPathField("b")))))

    val spec2 = GroupingSource(
      table2,
      SourceKey.Single, Some(SourceValue.Single), groupId2,
      GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, CPathField("a"))))

    val intersection = GroupingAlignment(
      DerefObjectStatic(Leaf(Source), tic_a),
      DerefObjectStatic(Leaf(Source), tic_a),
      spec1,
      spec2, GroupingSpec.Intersection)

    val result = Table.merge(intersection) { (key, map) =>
      for {
        gs1  <- map(groupId1)
        gs2  <- map(groupId2)
        gs1Json <- gs1.toJson
        gs2Json <- gs2.toJson
      } yield {
        val JNum(keyBigInt) = key.toJValue(tic_aj)
        key.toJValue(tic_bj) must beLike {
          case JNum(_) => ok
        }

        gs1Json must not(beEmpty)
        gs2Json must not(beEmpty)

        forall(gs1Json) { row =>
          (row \ "a") must beLike {
            case JNum(i) => i mustEqual keyBigInt
          }
        }

        forall(gs2Json) { row =>
          (row \ "a") must beLike {
            case JNum(i) => i mustEqual keyBigInt
          }
        }

        fromJson(Stream(
          JObject(
            JField("key", JArray(key.toJValue(tic_aj) :: key.toJValue(tic_bj) :: Nil)) ::
            JField("value", JNum(gs1Json.size + gs2Json.size)) :: Nil)))
      }
    }

    val resultJson = result.flatMap(_.toJson).copoint

    val joinKeys = (rawData1.map(_._1).toSet intersect rawData2.toSet)

    val grouped1 = rawData1.filter({ case (a, b) => joinKeys(a) }).groupBy(_._1)
    val grouped2 = rawData2.groupBy(identity[Int]).filterKeys(joinKeys)

    // in order to get a binding for both 'a and 'b, values must come from
    // rawData1 and 'a must be in the join keys.
    resultJson must haveSize(rawData1.filter({ case (a, b) => joinKeys(a) }).distinct.size)

    val grouped1ab = grouped1.mapValues(_.groupBy(_._2.get))
    forall(resultJson) { v =>
      v must beLike {
        case obj: JObject => {
          val JArray(JNum(ka) :: JNum(kb) :: Nil) = obj \ "key"
          val JNum(v) = obj \ "value"

          v must_== (grouped1ab(ka.toInt)(kb.toInt).size + grouped2(ka.toInt).size)
        }
      }
    }
  }


  def testCtrPartialJoinOr(rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data1 = augmentWithIdentities(rawData1 map {
      case (a, b0) => JObject(JField("a", JNum(a)) :: b0.map(b => JField("b", JNum(b))).toList)
    })

    val data2 = augmentWithIdentities(rawData2 map { v => JObject(JField("a", JNum(v)) :: Nil) })

    val table1 = fromJson(data1)
    val table2 = fromJson(data2)

    val groupId1 = newGroupId
    val groupId2 = newGroupId

    // spec1' := spec1 where spec1.a = 'a | spec1.b = 'b
    // spec2' := spec2 where spec2.a = 'a
    val spec1 = GroupingSource(
      table1,
      SourceKey.Single, Some(SourceValue.Single), groupId1,
      GroupKeySpecOr(
        GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, CPathField("a"))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, CPathField("b")))))

    val spec2 = GroupingSource(
      table2,
      SourceKey.Single, Some(SourceValue.Single), groupId2,
      GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, CPathField("a"))))

    val intersection = GroupingAlignment(
      DerefObjectStatic(Leaf(Source), tic_a),
      DerefObjectStatic(Leaf(Source), tic_a),
      spec1,
      spec2, GroupingSpec.Intersection)

    var elapsed = 0L
    var firstMerge = 0L

    val result = Table.merge(intersection) { (key, map) =>
      if (firstMerge == 0) firstMerge = System.currentTimeMillis
      val start = System.currentTimeMillis
      for {
        gs1  <- map(groupId1)
        gs2  <- map(groupId2)
        gs1Json <- gs1.toJson
        gs2Json <- gs2.toJson
      } yield {
        val ka @ JNum(kaValue) = key.toJValue(tic_aj)
        val kb = key.toJValue(tic_bj)

        gs1Json must not(beEmpty)
        gs2Json must not(beEmpty)

        forall(gs1Json) { row =>
         ((row \ "a") must_== ka) or ((row \ "b") must_== kb)
        }

        forall(gs2Json) { row =>
          (row \ "a") must beLike {
            case JNum(i) => i mustEqual kaValue
          }
        }

        val result = fromJson(Stream(
          JObject(
            JField("key", key.toJValue(tic_aj)) ::
            JField("value", JNum(gs1Json.size + gs2Json.size)) :: Nil)))

        elapsed += (System.currentTimeMillis - start)
        result
      }
    }

    val resultJson = result.flatMap(_.toJson).copoint
    val elapsedOverMerge = System.currentTimeMillis - firstMerge
    //println("total elapsed in merge: " + elapsedOverMerge)
    //println("total elapsed outside of body: " + (elapsedOverMerge - elapsed))

    val joinKeys = (rawData1.map(_._1).toSet intersect rawData2.toSet)
    val joinRows = rawData1.filter({ case (a, b) => joinKeys(a) })

    val crossRows = for {
      (row1a, Some(row1b)) <- rawData1
      row2 <- rawData2
    } yield {
      JObject(
        JField("a", JNum(row2)) ::
        JField("b", JNum(row1b)) :: Nil)
    }

    resultJson must haveSize(joinRows.map(_._1).distinct.size + crossRows.distinct.size)
  }


  def testNonTrivial = {
    //
    // forall 'a forall 'b
    //   foo where foo.a = 'a & foo.b = 'b
    //   bar where bar.a = 'a
    //   baz where baz.b = 'b
    //
    //   { a: 'a, b: 'b, foo: count(foo'), bar: count(bar'), baz: count(baz') }
    //

    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val JArray(foo0) = JParser.parseUnsafe("""[
      { "a":42.0, "b":12.0 },
      { "a":42.0 },
      { "a":77.0 },
      { "c":-3.0 },
      { "b":7.0 },
      { "a":42.0, "b":12.0 },
      { "a":7.0, "b":42.0 },
      { "a":17.0, "b":6.0 },
      { "b":1.0 },
      { "a":21.0, "b":12.0 },
      { "a":42.0, "b":-2.0 },
      { "c":-3.0 },
      { "a":7.0, "b":42.0 },
      { "a":42.0, "b":12.0 }
    ]""")

    val JArray(bar0) = JParser.parseUnsafe("""[
      { "a":42.0 },
      { "a":42.0 },
      { "a":77.0 },
      { "c":-3.0 },
      { "b":7.0 },
      { "b":12.0 },
      { "a":7.0, "b":42.0 },
      { "a":17.0, "c":77.0 },
      { "b":1.0 },
      { "b":12.0 },
      { "b":-2.0 },
      { "c":-3.0 },
      { "a":7.0 },
      { "a":42.0 }
    ]""")

    val JArray(baz0) = JParser.parseUnsafe("""[
      { "b":12.0 },
      { "b":6.0 },
      { "a":42.0 },
      { "b":1.0 },
      { "b":12.0 },
      { "c":-3.0 },
      { "b":42.0 },
      { "d":0.0 }
    ]""")

    val foo = augmentWithIdentities(foo0.toStream)
    val bar = augmentWithIdentities(bar0.toStream)
    val baz = augmentWithIdentities(baz0.toStream)

    val fooGroup = newGroupId
    val barGroup = newGroupId
    val bazGroup = newGroupId

    val fooSpec = GroupingSource(
      fromJson(foo.toStream),
      SourceKey.Single, Some(SourceValue.Single), fooGroup,
      GroupKeySpecAnd(
        GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, CPathField("a"))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, CPathField("b")))))

    val barSpec = GroupingSource(
      fromJson(bar.toStream),
      SourceKey.Single, Some(SourceValue.Single), barGroup,
      GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, CPathField("a"))))

    val bazSpec = GroupingSource(
      fromJson(baz.toStream),
      SourceKey.Single, Some(SourceValue.Single), bazGroup,
      GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, CPathField("b"))))

    val spec = GroupingAlignment(
      DerefObjectStatic(Leaf(Source), tic_b),
      DerefObjectStatic(Leaf(Source), tic_b),
      GroupingAlignment(
        DerefObjectStatic(Leaf(Source), tic_a),
        DerefObjectStatic(Leaf(Source), tic_a),
        fooSpec,
        barSpec, GroupingSpec.Intersection),
      bazSpec, GroupingSpec.Intersection)

    val forallResult = Table.merge(spec) { (key, map) =>
      val a = key.toJValue(tic_aj)
      val b = key.toJValue(tic_bj)

      a mustNotEqual JUndefined
      b mustNotEqual JUndefined

      for {
        fooP <- map(fooGroup)
        barP <- map(barGroup)
        bazP <- map(bazGroup)
        fooPJson <- fooP.toJson
        barPJson <- barP.toJson
        bazPJson <- bazP.toJson
      } yield {
        fooPJson must not(beEmpty)
        barPJson must not(beEmpty)
        bazPJson must not(beEmpty)

        val result = Stream(
          JObject(
            JField("a", a) ::
            JField("b", b) ::
            JField("foo", JNum(fooPJson.size)) ::
            JField("bar", JNum(barPJson.size)) ::
            JField("baz", JNum(bazPJson.size)) :: Nil))

        fromJson(result)
      }
    }

    val forallJson = forallResult flatMap { _.toJson } copoint

    forallJson must not(beEmpty)
    forallJson must haveSize(3)

    forall(forallJson) { row =>
      val JNum(ai) = row \ "a"
      val JNum(bi) = row \ "b"

      val JNum(fooi) = row \ "foo"
      val JNum(bari) = row \ "bar"
      val JNum(bazi) = row \ "baz"

      (ai.toInt, bi.toInt) must beLike {
        case (42, 12) =>
          fooi mustEqual 3
          bari mustEqual 3
          bazi mustEqual 2

        case (7, 42) =>
          fooi mustEqual 2
          bari mustEqual 2
          bazi mustEqual 1

        case (17, 6) =>
          fooi mustEqual 1
          bari mustEqual 1
          bazi mustEqual 1
      }
    }
  }

  "simple single-key grouping" should {
    "scalacheck a histogram by value" in Prop.forAllNoShrink(testHistogramByValue _)
    "histogram for two of the same value" in testHistogramByValue(Stream(2147483647, 2147483647))
    "histogram when observing spans of equal values" in testHistogramByValue(Stream(24, -10, 0, -1, -1, 0, 24, 0, 0, 24, -1, 0, 0, 24))
    "compute a histogram by value (mapping target)" in prop (testHistogramByValueMapped _)
    "compute a histogram by value (mapping target) trivial example" in testHistogramByValueMapped(Stream(0))
    "compute a histogram by even/odd" in prop (testHistogramEvenOdd _)
    "compute a histogram by even/odd trivial example" in testHistogramEvenOdd(Stream(0))
  }

  "simple multi-key grouping" should {
    "compute a histogram on two keys" >> {
      "and" >> testHistogramTwoKeysAnd
      "or" >> testHistogramTwoKeysOr
    }
    "compute a histogram on one key with an extra" >> {
      "and" >> testHistogramExtraAnd
      "or" >> testHistogramExtraOr
    }
  }

  "multi-set grouping" should {
    "compute ctr on value" in Prop.forAllNoShrink (testCtr _)
    "compute ctr with an empty dataset" in testCtr(Stream(), Stream(1))
    "compute ctr with singleton datasets" in testCtr(Stream(1), Stream(1))
    "compute ctr with simple datasets with repeats" in testCtr(Stream(1, 1, 1), Stream(1))
    "compute ctr with simple datasets" in testCtr(Stream(-565998477, 1911906594, 1), Stream(1948335811, -528723320, 1))
    "compute ctr with simple datasets" in testCtr(Stream(1, 2147483647, 2126441435, -1, 0, 0), Stream(2006322377, -2147483648, -1456034303, 2147483647, 0, 2147483647, -1904025337))

    "compute ctr on one field of a composite value" >> {
      "and" >> Prop.forAllNoShrink(testCtrPartialJoinAnd _).pendingUntilFixed
      "and with un-joinable datasets" >> testCtrPartialJoinAnd(
        Stream((0,Some(1)), (1123021019,Some(-2147483648))),
        Stream(-1675865668, 889796884, 2147483647, -1099860336, -2147483648, -2147483648, 1, 1496400141)
      )
      "and with joinable datasets" >> testCtrPartialJoinAnd(
        Stream((-1,Some(-1771882715)), (-2091150211,Some(1)), (1,Some(-1161386492)), (0,Some(-1)), (-1,Some(-1)), (-2147483648,Some(-2147483648)), (-1,Some(1)), (0,Some(391541906)), (-2147483648,Some(725820706)), (0,Some(-2147483648)), (1286585203,Some(560695941))),
        Stream(0, -297579588, -1, 2147483647, -1, -1536865491, 1049246142, -2147483648, -2147483648, 766980226, -1047565460)
      )
      "and with repeated group keys in joinable datasets" >> testCtrPartialJoinAnd(
        Stream((1,Some(-421523375)), (1381663801,Some(2145939312)), (975603510,Some(-456843566)), (-260964705,Some(-811947401)), (-1643830562,Some(0)), (382901678,Some(-2147483648)), (-1770905652,Some(-1)), (1172197808,Some(1)), (-206421051,Some(307500840)), (2147483647,Some(-1)), (2147483647,Some(-1)), (-1775980054,Some(2147483647))),
        Stream(1, -1, -2005746103, 720318134, 852618110, 1813748094, -1, -1676020815, -627348537, 2147483647, -2147483648)
      )

      // TODO: the performance of the following is too awful to run under scalacheck, even with a minimal
      // number of examples.
      "or" >> Prop.forAllNoShrink (testCtrPartialJoinOr _).set(minTestsOk = 10)
      "or with empty 1st dataset" >> testCtrPartialJoinOr(Stream(), Stream(1))
      "or with empty 2nd dataset" >> testCtrPartialJoinOr(Stream((1, Some(2))), Stream())
      "or with un-joinable datasets" >> testCtrPartialJoinOr(
        Stream((-2,Some(1))),
        Stream(-1)
      )
      "or with a join in datasets" >> testCtrPartialJoinOr(
        Stream((2,Some(-1)), (1,Some(-1)), (3,Some(4)), (1,Some(-1))),
        Stream(-2, 1, 1, 5, 0, 6)
      )

      // runs a bit long
      "or with a pathological example" >> {
        val s1 = Stream((-954410459,Some(0)), (-1,Some(2007696701)), (2105675940,Some(-1245674830)), (-1582587372,Some(1940093023)), (63198658,Some(2068956190)), (0,Some(-189150978)), (2000592976,Some(-222301652)), (523154377,Some(0)), (-2147483648,Some(1632775270)), (1092038023,Some(1819439617)), (-2147483648,Some(2147483647)), (0,Some(0)), (2147483647,Some(0)), (-1143657189,Some(-2147483648)), (-1958852329,Some(2147483647)), (-2147483648,Some(608866931)), (-273338630,Some(-2147483648)), (-2147483648,Some(-1841559997)), (-2147483648,Some(1601378038)), (0,Some(-1)), (1,Some(1)), (-670756012,Some(-106440741)), (-2147483648,Some(-431649434)), (0,Some(585196920)), (0,Some(143242157)), (2147483647,Some(0)), (-1002181171,Some(2147483647)), (260767290,Some(2147483647)), (2147483647,Some(0)), (1502519219,Some(-80993454)), (-2147483648,Some(1)), (26401216,Some(1737006538)), (459053133,Some(1)), (1,Some(222440292)), (2147483647,Some(-1)), (-785490772,Some(2147483647)), (-1519510933,Some(1)), (1064945303,Some(2015037890)), (2147483647,Some(-1888515244)), (-2147483648,Some(0)), (-1782288738,Some(-2147483648)), (-1243866137,Some(-2036899743)), (2147483647,Some(-2147483648)), (152217775,Some(1)), (-1,Some(1822038570)), (-557295510,Some(-2147483648)), (0,Some(0)), (-1389729666,Some(407111520)), (0,Some(1110392883)), (-2042103283,Some(-1366550515)), (-1309507483,Some(-2147483648)), (2147483647,Some(0)), (1322668865,Some(1)), (1,Some(1)), (1296673327,Some(341152609)), (1040120825,Some(-1731488506)), (-951605740,Some(1)), (690140640,Some(-1783450717)), (1395849695,Some(768982688)), (-1,Some(-894395447)), (2147483647,Some(2147483647)), (-1,Some(-2016297234)), (-1416825502,Some(-2147483648)), (1727813995,Some(1)), (-1178284872,Some(-2147483648)), (2147483647,Some(-1468556846)), (-361436734,Some(0)), (960146451,Some(-2147483648)), (-2147483648,Some(-2147483648)), (973715803,Some(603648248)), (2147483647,Some(0)), (-2147483648,Some(-36955603)), (2005706222,Some(-242403982)), (-1274227445,Some(1156421302)), (-2147483648,Some(385347685)), (-2147483648,Some(926114223)), (1690927871,Some(1)), (-330611474,Some(-2147483648)), (-1801526113,Some(922619077)), (-2147483648,Some(-1903319530)), (2147483647,Some(0)))

        val s2 = Stream(0, 0, 0, 1, 1, 434608913, 193294286, 0, -1921860406, 2147483647, -2147483648, 1, -1, 0, -2147483648, 0, -113276442, -1564947365, 2147483647, -54676151, -1, 49986682, -391210112, 1, -1, 2147483647, 0, -1, 0, 0, 2147483647, -225140804, 1245119802, 1, -548778232, -1138847365, 1, 73483948, 0, -1, -996046474, -695581403, 2147483647, -2147483648, -1, 1563916971, -2147483648, 0, 1, 607908889, -2009071663, -1382431435, 778550183, 2147483647, -2147483648, 0, -1)

        //println("s1.size = %d, s2.size = %d".format(s1.size, s2.size))
        //println("distinct s1.size = %d, s2.size = %d".format(s1.map(_._1).toSet.size, s2.toSet.size))
        testCtrPartialJoinOr(s1, s2)
      }

      "or with a simple join in datasets" >> testCtrPartialJoinOr(
        Stream((436413513,Some(-477784155)),
               (1693516917,Some(1537597532)),
               (-33300192,Some(1)),
               (-1,Some(417911606)),
               (941828761,Some(-1)),
               (-116426729,Some(0)),
               (0,Some(1)),
               (-1,Some(175860194)),
               (-2147483648,Some(-2014951990)),
               (2147483647,Some(293027634)),
               (-1964286008,Some(132426726))),
        Stream(-1)
      )
    }
  }

  "handle non-trivial group alignment with composite key" in testNonTrivial
}

object GrouperSpec extends TableModuleSpec[Need] with GrouperSpec
