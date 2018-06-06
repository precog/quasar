/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.algebra._
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.ejson, ejson.{EJsonArbitrary, TypeTag, z85}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.numeric.{Natural, Positive}
import quasar.tpe._

import scala.Predef.$conforms
import scala.Byte

import eu.timepit.refined.auto._
import matryoshka.{project => _, _}
import matryoshka.data._
import matryoshka.implicits._
import monocle.syntax.fields._
import org.scalacheck._, Arbitrary.arbitrary
import org.specs2.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazArbitrary._
import scodec.bits.ByteVector
import spire.math.Real

final class CompressionSpec extends quasar.Qspec
  with StructuralTypeArbitrary
  with TypeStatArbitrary
  with EJsonArbitrary
  with SimpleTypeArbitrary {

  implicit val params = Parameters(maxSize = 10)

  import StructuralType.{ConstST, TagST, TypeST}

  type J = TypedEJson[Fix]
  type S = SST[J, Real]

  val J = ejson.Fixed[J]

  case class LeafEjs(ejs: J) {
    def toSst: S = SST.fromEJson(Real(1), ejs)
  }

  implicit val arbitraryLeafEjs: Arbitrary[LeafEjs] =
    Arbitrary(Gen.oneOf(
      Gen.const(J.nul()),
      arbitrary[Boolean] map (b => J.bool(b)),
      arbitrary[String] map (s => J.str(s)),
      arbitrary[BigDecimal] map (d => J.dec(d)),
      arbitrary[Byte] map (b => J.byte(b)),
      arbitrary[Char] map (c => J.char(c)),
      arbitrary[BigInt] map (i => J.int(i))
    ) map (LeafEjs(_)))

  implicit val orderLeafEjs: Order[LeafEjs] =
    Order.orderBy(_.ejs)

  implicit val realShow: Show[Real] = Show.showFromToString

  val cnt1 = TypeStat.count(Real(1))
  val envTType = envTIso[TypeStat[Real], StructuralType.ST[J, ?], S] composeLens _2
  def sstConst(s: S) = ConstST unapply (s.project) map (_._3)

  def attemptCompression(s: S, f: SSTF[J, Real, S] => Option[SSTF[J, Real, S]]): S =
    s.transAna[S](orOriginal(f))

  "coalesceKeys" >> {
    def test(kind: String, f: Char => J) =
      s"compresses largest group of keys having same primary ${kind}" >> prop {
        (cs: ISet[Char], n: BigInt, b: Byte, unk0: Option[(LeafEjs, LeafEjs)]) => (cs.size > 1) ==> {

        val chars = cs.toIList map f
        val int = J.int(n)
        val byte = J.byte(b)
        val nul = SST.fromEJson(Real(1), J.nul())
        val m0 = IMap.fromFoldable((int :: byte :: chars) strengthR nul)
        val unk  = unk0.map(_.umap(_.toSst))
        val msst = envT(cnt1, TypeST(TypeF.map[J, S](m0, unk))).embed

        val uval = SST.fromEJson(Real(cs.size), J.nul())
        val ukey = chars.foldMap1Opt(c => SST.fromEJson(Real(1), c))
        val m1   = IMap.fromFoldable(IList(byte, int) strengthR nul)
        val unk1 = ukey.strengthR(uval) |+| unk
        val exp  = envT(cnt1, TypeST(TypeF.map[J, S](m1, unk1))).embed

        attemptCompression(msst, compression.coalesceKeys(2L)) must_= exp
      }}

    test("type", J.char(_))

    test("tag", c => J.meta(J.char(c), J.tpe(TypeTag("cp"))))

    "ignores map where size of keys does not exceed maxSize" >> prop {
      (xs: IList[(LeafEjs, LeafEjs)], unk0: Option[(LeafEjs, LeafEjs)]) =>

      val m   = IMap.fromFoldable(xs.map(_.bimap(_.ejs, _.toSst)))
      val unk = unk0.map(_.umap(_.toSst))
      val sst = envT(cnt1, TypeST(TypeF.map[J, S](m, unk))).embed

      Natural(m.size.toLong).cata(
        l => attemptCompression(sst, compression.coalesceKeys(l)),
        sst
      ) must_= sst
    }
  }

  "coalescePrimary" >> {
    "combines consts with their primary SST in unions" >> prop { (sj: LeafEjs, sjs: ISet[LeafEjs]) =>
      val pt = primaryTagOf(sj.ejs)
      val primarySst = compression.primarySst(Real(1), sj.ejs)

      val leafs = sjs.insert(sj).toIList
      val ssts = leafs.map(_.toSst)

      val (matching, nonmatching) =
        leafs.partition(l => primaryTagOf(l.ejs) ≟ pt)

      val simplified = matching.map(l => compression.primarySst(Real(1), l.ejs))
      val coalesced = NonEmptyList.nel(primarySst, simplified).suml1

      val compressed =
        attemptCompression(
          NonEmptyList.nel(primarySst, ssts).suml1,
          compression.coalescePrimary)

      compressed must_= NonEmptyList.nel(coalesced, nonmatching map (_.toSst)).suml1
    }

    "combines multiple instances of a primary SST in unions" >> prop {
      (x: SimpleType, xs: NonEmptyList[SimpleType]) =>

      val y   = envT(cnt1, TypeST(TypeF.simple[J, S](x))).embed
      val ys  = xs.map(st => envT(cnt1, TypeST(TypeF.simple[J, S](st))).embed)
      val cnt = TypeStat.count(Real(ys.length + 1))

      val union = envT(cnt, TypeST(TypeF.union[J, S](y, ys.head, ys.tail))).embed
      val sum   = (y <:: ys).suml1

      attemptCompression(union, compression.coalescePrimary) must_= sum
    }

    "no effect when a const's primary tag not in the union" >> prop { ljs: NonEmptyList[LeafEjs] =>
      val sum = ljs.foldMap1(_.toSst)
      attemptCompression(sum, compression.coalescePrimary) must_= sum
    }
  }

  "coalesceWithUnknown" >> {
    def testUnk(kind: String, f: Char => J, g: (TypeStat[Real], SimpleType) => SSTF[J, Real, S]) =
      s"merges known map entry with unknown entry when same primary $kind appears in unknown" >> prop {
        (head: (Char, LeafEjs), xs0: NonEmptyList[(Char, LeafEjs)], kv: (BigInt, LeafEjs)) =>

        val xs: IMap[Char, LeafEjs] = IMap.fromFoldable(xs0) + head

        val u1 = head.bimap(_ => SST.fromEJson(Real(1), f('x')), _.toSst)
        val u2 = head.bimap(
          c => g(TypeStat.fromEJson(Real(1), J.char(c)), SimpleType.Char).embed,
          _.toSst)
        val kv1 = kv.bimap(J.int(_), _.toSst)
        val cs = xs.toList.map(_.bimap(f, _.toSst))
        val m = IMap.fromFoldable(kv1 :: cs)
        val sst1 = envT(cnt1, TypeST(TypeF.map(m, u1.some))).embed
        val sst2 = envT(cnt1, TypeST(TypeF.map(m, u2.some))).embed

        val a = cs.foldMap1Opt { case (j, s) => (SST.fromEJson(Real(1), j), s) }
        val b = IMap.singleton(kv1._1, kv1._2)
        val exp1 = envT(cnt1, TypeST(TypeF.map(b, a map (_ |+| u1)))).embed
        val exp2 = envT(cnt1, TypeST(TypeF.map(b, a map (_ |+| u2)))).embed

        (attemptCompression(sst1, compression.coalesceWithUnknown) must_= exp1) and
        (attemptCompression(sst2, compression.coalesceWithUnknown) must_= exp2)
      }

    def testUnkUnion(kind: String, f: Char => J, g: (TypeStat[Real], SimpleType) => SSTF[J, Real, S]) =
      s"merges known map entry with unknown when primary $kind appears in unknown union" >> prop {
        (head: (Char, LeafEjs), xs0: NonEmptyList[(Char, LeafEjs)], kv: (BigInt, LeafEjs)) =>

        val xs: IMap[Char, LeafEjs] = IMap.fromFoldable(xs0) + head

        val u1 = head.bimap(_ => SST.fromEJson(Real(1), f('x')), _.toSst)
        val u2 = head.bimap(
          c => g(TypeStat.fromEJson(Real(1), J.char(c)), SimpleType.Char).embed,
          _.toSst)
        val st = envT(cnt1, TypeST(TypeF.simple[J, S](SimpleType.Dec))).embed
        val tp = envT(cnt1, TypeST(TypeF.top[J, S]())).embed
        val u1u = u1.leftMap(s => envT(cnt1, TypeST(TypeF.union[J, S](tp, s, IList(st)))).embed)
        val u2u = u2.leftMap(s => envT(cnt1, TypeST(TypeF.union[J, S](s, st, IList(tp)))).embed)
        val kv1 = kv.bimap(J.int(_), _.toSst)
        val cs = xs.toList.map(_.bimap(f, _.toSst))
        val m = IMap.fromFoldable(kv1 :: cs)
        val sst1 = envT(cnt1, TypeST(TypeF.map(m, u1u.some))).embed
        val sst2 = envT(cnt1, TypeST(TypeF.map(m, u2u.some))).embed

        val a = cs.foldMap1Opt { case (j, s) => (SST.fromEJson(Real(1), j), s) }
        val b = IMap.singleton(kv1._1, kv1._2)
        val exp1 = envT(cnt1, TypeST(TypeF.map(b, a map (_ |+| u1u)))).embed
        val exp2 = envT(cnt1, TypeST(TypeF.map(b, a map (_ |+| u2u)))).embed

        (attemptCompression(sst1, compression.coalesceWithUnknown) must_= exp1) and
        (attemptCompression(sst2, compression.coalesceWithUnknown) must_= exp2)
      }

    def test(kind: String, f: Char => J, g: (TypeStat[Real], SimpleType) => SSTF[J, Real, S]) = {
      testUnk(kind, f, g)
      testUnkUnion(kind, f, g)
    }

    test("type",
      J.char(_),
      (ts, st) => envT(ts, TypeST(TypeF.simple[J, S](st))))

    test("tag",
      c => J.meta(J.char(c), J.tpe(TypeTag("codepoint"))),
      (ts, st) => envT(TypeStat.count(ts.size), TagST[J](Tagged(
        TypeTag("codepoint"),
        envT(ts, TypeST(TypeF.simple[J, S](st))).embed))))

    "has no effect on maps when all keys are known" >> prop { xs: IList[(LeafEjs, LeafEjs)] =>
      val m   = IMap.fromFoldable(xs.map(_.bimap(_.ejs, _.toSst)))
      val sst = envT(cnt1, TypeST(TypeF.map[J, S](m, None))).embed

      attemptCompression(sst, compression.coalesceWithUnknown) must_= sst
    }

    "has no effect on maps when primary type not in unknown" >> prop { xs: IList[(LeafEjs, LeafEjs)] =>
      val m   = IMap.fromFoldable(xs.map(_.bimap(_.ejs, _.toSst)))
      val T   = envT(cnt1, TypeST(TypeF.top[J, S]())).embed
      val sst = envT(cnt1, TypeST(TypeF.map[J, S](m, Some((T, T))))).embed

      attemptCompression(sst, compression.coalesceWithUnknown) must_= sst
    }

    "has no effect on maps when primary tag not in unknown" >> prop { xs: IList[(LeafEjs, LeafEjs)] =>
      val foo = TypeTag("foo")
      val bar = TypeTag("bar")
      val m = IMap.fromFoldable(xs.map(_.bimap(l => J.meta(l.ejs, J.tpe(foo)), _.toSst)))
      val T = envT(cnt1, TagST[J](Tagged(bar, envT(cnt1, TypeST(TypeF.top[J, S]())).embed))).embed
      val sst = envT(cnt1, TypeST(TypeF.map[J, S](m, Some((T, T))))).embed

      attemptCompression(sst, compression.coalesceWithUnknown) must_= sst
    }
  }

  "limitArrays" >> {
    "compresses arrays longer than maxLen to the union of the members" >> prop {
      xs: NonEmptyList[BigInt] => (xs.length > 1) ==> {

      val alen: Natural = Natural(xs.length.toLong) getOrElse 0L
      val lt: Natural = Natural((xs.length - 1).toLong) getOrElse 0L
      val rlen = Real(xs.length).some
      val ints = xs.map(J.int(_))
      val xsst = SST.fromEJson(Real(1), J.arr(ints.toList))

      val sum = ints.foldMap1(x => SST.fromEJson(Real(1), x))
      val coll = TypeStat.coll(Real(1), rlen, rlen)
      val lubarr = envT(coll, TypeST(TypeF.arr[J, S](sum.right))).embed

      val req = xsst.elgotApo[S](compression.limitArrays(alen))
      val rlt = xsst.elgotApo[S](compression.limitArrays(lt))

      (req must_= xsst) and (rlt must_= lubarr)
    }}

    "does not limit structural string arrays" >> prop {
      s: String => (s.length > 1) ==> {

      val lim: Natural = Natural((s.length - 1).toLong) getOrElse 0L
      val stringSst = strings.widen[J, Real](Real(1), s).embed

      stringSst.elgotApo[S](compression.limitArrays(lim)) must_= stringSst
    }}
  }

  "limitStrings" >> {
    "compresses strings longer than maxLen" >> prop { s: String => (s.length > 1) ==> {
      val plen: Natural = Natural(s.length.toLong) getOrElse 0L
      val lt: Natural = Natural((s.length - 1).toLong) getOrElse 0L

      val str = SST.fromEJson(Real(1), J.str(s))
      val arr = strings.compress[S, J, Real](str.copoint, s).embed

      val req = attemptCompression(str, compression.limitStrings(plen))
      val rlt = attemptCompression(str, compression.limitStrings(lt))

      (req must_= str) and (rlt must_= arr)
    }}
  }

  "narrowUnion" >> {
    "reduces the largest group of values having the same primary type" >> prop {
      (bs: ISet[Byte], c1: Char, c2: Char, c3: Char, d1: BigDecimal) =>
      ((bs.size > 3) && (ISet.fromFoldable(IList(c1, c2, c3)).size ≟ 3)) ==> {

      val bytes = bs.toIList.map(b => SST.fromEJson(Real(1), J.byte(b)))
      val chars = IList(c1, c2, c3).map(c => SST.fromEJson(Real(1), J.char(c)))
      val dec = SST.fromEJson(Real(1), J.dec(d1))

      val compByte = envT(
        bytes.foldMap1Opt(_.copoint) | TypeStat.count(Real(0)),
        TypeST(TypeF.simple[J, S](SimpleType.Byte))
      ).embed

      val union0 = NonEmptyList.nel(dec, chars ::: bytes).suml1
      val union1 = envT(union0.copoint, TypeST(TypeF.union[J, S](compByte, dec, chars))).embed

      attemptCompression(union0, compression.narrowUnion(3L)) must_= union1
    }}

    "no effect on unions smaller or equal to maxSize" >> prop {
      (x: LeafEjs, y: LeafEjs, xs: IList[LeafEjs]) =>

      val union = envT(cnt1, TypeST(TypeF.union[J, S](x.toSst, y.toSst, xs map (_.toSst)))).embed

      Positive((xs.length + 2).toLong).cata(
        l => attemptCompression(union, compression.narrowUnion(l)),
        union
      ) must_= union
    }
  }

  "z85EncodedBinary" >> {
    "compresses all encoded binary strings" >> prop { bs: Vector[Byte] =>
      val bytes   = ByteVector(bs)
      val encoded = z85.encode(bytes)
      val ejs     = J.meta(J.str(encoded), J.sizedTpe(TypeTag.Binary, BigInt(bytes.size)))
      val sst     = SST.fromEJson(Real(1), ejs)

      val bstat   = TypeStat.byte(Real(1), Byte.MinValue, Byte.MaxValue)
      val byte    = envT(bstat, TypeST(TypeF.simple[J, S](SimpleType.Byte))).embed
      val rsize   = Real(bytes.size).some
      val coll    = TypeStat.coll(Real(1), rsize, rsize)
      val barr    = envT(coll, TypeST(TypeF.arr[J, S](byte.right))).embed

      attemptCompression(sst, compression.z85EncodedBinary) must_= barr
    }
  }
}
