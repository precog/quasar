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
import org.scalacheck.Gen._
// import org.specs2.ScalaCheck
// import org.scalacheck._, Gen._, Arbitrary._
import SampleData._

trait BlockAlignSpec extends SpecificationLike with ScalaCheck {
  implicit def M = Need.need
  private def emptyTestModule = BlockStoreTestModule.empty[Need]

  def testAlign(sample: SampleData) = {
    val module = emptyTestModule

    import module._
    import module.trans.constants._

    val lstream = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 => v }
    val rstream = sample.data.zipWithIndex collect { case (v, i) if i % 3 == 0 => v }

    val expected = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 && i % 3 == 0 => v }

    val finalResults = for {
      results <- Table.align(fromJson(lstream), SourceKey.Single, fromJson(rstream), SourceKey.Single)
      leftResult  <- results._1.toJson
      rightResult <- results._2.toJson
      leftResult2 <- results._1.toJson
    } yield {
      (leftResult, rightResult, leftResult2)
    }

    val (leftResult, rightResult, leftResult2) = finalResults.copoint

    leftResult must_== expected
    rightResult must_== expected
    leftResult must_== leftResult2
  }

  def checkAlign = {
    implicit val gen = sample(objectSchema(_, 3))
    prop { (sample: SampleData) => testAlign(sample.sortBy(_ \ "key")) }
  }

  def alignSimple = {
    val JArray(elements) = JParser.parseUnsafe("""[
        {
          "value":{ "fr8y":-2.761198250953116839E+14037, "hw":[], "q":2.429467767811669098E+50018 },
          "key":[1.0,2.0]
        },
        {
          "value":{ "fr8y":8862932465119160.0, "hw":[], "q":-7.06989214308545856E+34226 },
          "key":[2.0,1.0]
        },
        {
          "value":{ "fr8y":3.754645750547307163E-38452, "hw":[], "q":-2.097582685805979759E+29344 },
          "key":[2.0,4.0]
        },
        {
          "value":{ "fr8y":0.0, "hw":[], "q":2.839669248714535100E+14955 },
          "key":[3.0,4.0]
        },
        {
          "value":{ "fr8y":-1E+8908, "hw":[], "q":6.56825624988914593E-49983 },
          "key":[4.0,2.0]
        },
        {
          "value":{ "fr8y":123473018907070400.0, "hw":[], "q":0E+35485 },
          "key":[4.0,4.0]
        }]
    """)

    val sample = SampleData(elements.toStream, Some((2,List((JPath(".q"),CNum), (JPath(".hw"),CEmptyArray), (JPath(".fr8y"),CNum)))))

    testAlign(sample.sortBy(_ \ "key"))
  }

  def alignAcrossBoundaries = {
    val JArray(elements) = JParser.parseUnsafe("""[
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.0
        },
        "key":[4.0,26.0,21.0]
      },
      {
        "value":8.715632723857159E+307,
        "key":[5.0,39.0,59.0]
      },
      {
        "value":-1.104890528035041E+307,
        "key":[6.0,50.0,5.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.9570651303391037E+307
        },
        "key":[6.0,59.0,9.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.645581770904817E+307
        },
        "key":[9.0,28.0,29.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.0
        },
        "key":[10.0,21.0,30.0]
      },
      {
        "value":7.556003912577644E+307,
        "key":[11.0,51.0,15.0]
      },
      {
        "value":3.4877741123656093E+307,
        "key":[11.0,52.0,28.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":8.263625900450069E+307
        },
        "key":[12.0,25.0,58.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[16.0,31.0,42.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-7.976677386824275E+307
        },
        "key":[21.0,7.0,10.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":5.359853255240687E+307
        },
        "key":[23.0,12.0,26.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":8.988465674311579E+307
        },
        "key":[25.0,39.0,48.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[26.0,13.0,32.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":6.740770480418812E+307
        },
        "key":[27.0,17.0,7.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[27.0,36.0,16.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[32.0,13.0,11.0]
      },
      {
        "value":-1.0,
        "key":[32.0,50.0,21.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.0
        },
        "key":[37.0,5.0,43.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":2.911458319070367E+307
        },
        "key":[41.0,17.0,32.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":6.878966108624357E+307
        },
        "key":[48.0,14.0,45.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.024686561894592E+307
        },
        "key":[51.0,26.0,46.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.8366746041516641E+307
        },
        "key":[52.0,50.0,54.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.0
        },
        "key":[53.0,54.0,27.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.87789254395668E+307
        },
        "key":[55.0,46.0,57.0]
      }]
    """)

    val sample = SampleData(elements.toStream, Some((3,List((JPath(".xb5hs2ckjajs0k44x"),CDouble), (JPath(".zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump"),CEmptyArray), (JPath(".sp7hpv"),CEmptyObject)))))
    testAlign(sample.sortBy(_ \ "key"))
  }


  def testAlignSymmetry(i: Int) = {
    val module = emptyTestModule

    import module._
    import module.trans._

    def test(ltable: Table, alignOnL: TransSpec1, rtable: Table, alignOnR: TransSpec1) = {
      val (ljsondirect, rjsondirect) = (for {
        aligned <- Table.align(ltable, alignOnL, rtable, alignOnR)
        ljson <- aligned._1.toJson
        rjson <- aligned._2.toJson
      } yield {
        (ljson, rjson)
      }).copoint

      val (ljsonreversed, rjsonreversed) = (for {
        aligned <- Table.align(rtable, alignOnR, ltable, alignOnL)
        ljson <- aligned._1.toJson
        rjson <- aligned._2.toJson
      } yield {
        (ljson, rjson)
      }).copoint

      (ljsonreversed.toList must_== rjsondirect.toList) and
      (rjsonreversed.toList must_== ljsondirect.toList)
    }

    def test0 = {
      val lsortedOn = DerefArrayStatic(Leaf(Source), CPathIndex(1))
      val rsortedOn = DerefArrayStatic(Leaf(Source), CPathIndex(1))
      val JArray(ljson) = JParser.parseUnsafe("""[
        [[3],{ "000000":-1 },-1],
        [[4],{ "000000":0 },0],
        [[5],{ "000000":0 },0],
        [[0],{ "000000":1 },1],
        [[2],{ "000000":2126441435 },2126441435],
        [[1],{ "000000":2147483647 },2147483647]
      ]""")

      val JArray(rjson) = JParser.parseUnsafe("""[
        [[1],{ "000000":-2147483648 },-2147483648],
        [[6],{ "000000":-1904025337 },-1904025337],
        [[2],{ "000000":-1456034303 },-1456034303],
        [[4],{ "000000":0 },0],
        [[0],{ "000000":2006322377 },2006322377],
        [[3],{ "000000":2147483647 },2147483647],
        [[5],{ "000000":2147483647 },2147483647]
      ]""")

      test(fromJson(ljson.toStream), lsortedOn, fromJson(rjson.toStream), rsortedOn)
    }

    def test1 = {
      val JArray(ljson) = JParser.parseUnsafe("""[
        [[10],{ "000001":-2, "000000":42 },{ "a":42, "b":-2 }],
        [[7],{ "000001":6, "000000":17 },{ "a":17, "b":6 }],
        [[0],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[5],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[9],{ "000001":12, "000000":21 },{ "a":21, "b":12 }],
        [[13],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[6],{ "000001":42, "000000":7 },{ "a":7, "b":42 }],
        [[12],{ "000001":42, "000000":7 },{ "a":7, "b":42 }]
      ]""")

      val lsortedOn = OuterObjectConcat(WrapObject(
        DerefObjectStatic(
          OuterObjectConcat(
            WrapObject(DerefObjectStatic(DerefArrayStatic(Leaf(Source),CPathIndex(1)),
                                         CPathField("000001")),"000000"),
            WrapObject(DerefObjectStatic(DerefArrayStatic(Leaf(Source),CPathIndex(1)),
                                         CPathField("000000")),"000001")
          ),
          CPathField("000000")
        ),
        "000000"
      ))

      val JArray(rjson) = JParser.parseUnsafe("""[
        [[3],{ "000000":1 },{ "b":1 }],
        [[1],{ "000000":6 },{ "b":6 }],
        [[0],{ "000000":12 },{ "b":12 }],
        [[4],{ "000000":12 },{ "b":12 }],
        [[6],{ "000000":42 },{ "b":42 }]
      ]""")

      val rsortedOn = DerefArrayStatic(Leaf(Source),CPathIndex(1))

      test(fromJson(ljson.toStream), lsortedOn, fromJson(rjson.toStream), rsortedOn)
    }

    def test2 = {
      val JArray(ljson) = JParser.parseUnsafe("""[
        [[6],{ "000001":42, "000000":7 },{ "a":7, "b":42 }],
        [[12],{ "000001":42, "000000":7 },{ "a":7, "b":42 }],
        [[7],{ "000001":6, "000000":17 },{ "a":17, "b":6 }],
        [[9],{ "000001":12, "000000":21 },{ "a":21, "b":12 }]
      ]""")
      val JArray(ljson2) = JParser.parseUnsafe("""[
        [[0],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[5],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[10],{ "000001":-2, "000000":42 },{ "a":42, "b":-2 }],
        [[13],{ "000001":12, "000000":42 },{ "a":42, "b":12 }]
      ]""")

      val lsortedOn = OuterObjectConcat(WrapObject(
        DerefObjectStatic(
          OuterObjectConcat(
            WrapObject(DerefObjectStatic(DerefArrayStatic(Leaf(Source),CPathIndex(1)),
                                         CPathField("000000")),"000000"),
            WrapObject(DerefObjectStatic(DerefArrayStatic(Leaf(Source),CPathIndex(1)),
                                         CPathField("000001")),"000001")
          ),
          CPathField("000000")
        ),
        "000000"
      ))

      val JArray(rjson) = JParser.parseUnsafe("""[
        [[6],{ "000000":7 },{ "a":7, "b":42 }],
        [[12],{ "000000":7 },{ "a":7 }],
        [[7],{ "000000":17 },{ "a":17, "c":77 }]
      ]""")
      val JArray(rjson2) = JParser.parseUnsafe("""[
        [[0],{ "000000":42 },{ "a":42 }],
        [[1],{ "000000":42 },{ "a":42 }],
        [[13],{ "000000":42 },{ "a":42 }],
        [[2],{ "000000":77 },{ "a":77 }]
      ]""")

      val rsortedOn = DerefArrayStatic(Leaf(Source),CPathIndex(1))

      val ltable = Table(fromJson(ljson.toStream).slices ++ fromJson(ljson2.toStream).slices, UnknownSize)
      val rtable = Table(fromJson(rjson.toStream).slices ++ fromJson(rjson2.toStream).slices, UnknownSize)

      test(ltable, lsortedOn, rtable, rsortedOn)
    }

    i match {
      case 0 => test0
      case 1 => test1
      case 2 => test2
    }
  }
}

object BlockAlignSpec extends TableModuleSpec[Need] with BlockAlignSpec {
  "align" should {
    "a simple example" in alignSimple
    "across slice boundaries" in alignAcrossBoundaries
    "survive a trivial scalacheck" in checkAlign
    "produce the same results irrespective of input order" in testAlignSymmetry(0)
    "produce the same results irrespective of input order" in testAlignSymmetry(1)
    "produce the same results irrespective of input order" in testAlignSymmetry(2)
  }
}
