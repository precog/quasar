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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.algebra._
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.ejson, ejson.{CommonEJson => C, EJson, EJsonArbitrary, ExtEJson => E, z85}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.numeric.Positive
import quasar.tpe._

import scala.Predef.$conforms

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

  type J = Fix[EJson]
  type S = SST[J, Real]

  case class LeafEjs(ejs: J) {
    def toSST: S = SST.fromEJson(Real(1), ejs)
  }

  implicit val arbitraryLeafEjs: Arbitrary[LeafEjs] =
    Arbitrary(Gen.oneOf(
      Gen.const(C(ejson.nul[J]()).embed),
      arbitrary[Boolean] map (b => C(ejson.bool[J](b)).embed),
      arbitrary[String] map (s => C(ejson.str[J](s)).embed),
      arbitrary[BigDecimal] map (d => C(ejson.dec[J](d)).embed),
      arbitrary[Byte] map (b => E(ejson.byte[J](b)).embed),
      arbitrary[Char] map (c => E(ejson.char[J](c)).embed),
      arbitrary[BigInt] map (i => E(ejson.int[J](i)).embed)
    ) map (LeafEjs(_)))

  implicit val orderLeafEjs: Order[LeafEjs] =
    Order.orderBy(_.ejs)

  implicit val realShow: Show[Real] = Show.showFromToString

  val cnt1 = TypeStat.count(Real(1)).some
  val envTType = envTIso[Option[TypeStat[Real]], TypeF[J, ?], S] composeLens _2
  val sstConst = project[S, SSTF[J, Real, ?]] composeLens envTType composePrism TypeF.const

  "coalesceKeys" >> {
    "compresses largest group of keys having same primary type" >> prop {
      (cs: ISet[Char], n: BigInt, b: Byte, unk0: Option[(LeafEjs, LeafEjs)]) => (cs.size > 1) ==> {

      val chars = cs.toIList.map(c => E(ejson.char[J](c)).embed)
      val int = E(ejson.int[J](n)).embed
      val byte = E(ejson.byte[J](b)).embed
      val nul = SST.fromEJson(Real(1), C(ejson.nul[J]()).embed)
      val m0 = IMap.fromFoldable((int :: byte :: chars) strengthR nul)

      val unk  = unk0.map(_.umap(_.toSST))
      val msst = envT(cnt1, TypeF.map[J, S](m0, unk)).embed
      val uval = SST.fromEJson(Real(cs.size), C(ejson.nul[J]()).embed)
      val ukey = chars.foldMap(c => SST.fromEJson(Real(1), c))
      val m1   = IMap.fromFoldable(IList(byte, int) strengthR nul)
      val unk1 = (ukey, uval).some |+| unk
      val exp  = envT(cnt1, TypeF.map[J, S](m1, unk1)).embed

      msst.transCata[S](compression.coalesceKeys[J, Real](2L)) must_= exp
    }}

    "ignores map where size of keys does not exceed maxSize" >> prop {
      (xs: IList[(LeafEjs, LeafEjs)], unk0: Option[(LeafEjs, LeafEjs)]) =>

      val m   = IMap.fromFoldable(xs.map(_.bimap(_.ejs, _.toSST)))
      val unk = unk0.map(_.umap(l => SST.fromEJson(Real(1), l.ejs)))
      val sst = envT(cnt1, TypeF.map[J, S](m, unk)).embed

      Positive(m.size.toLong).cata(
        l => sst.transCata[S](compression.coalesceKeys[J, Real](l)),
        sst
      ) must_= sst
    }
  }

  "coalescePrimary" >> {
    "combines consts with their primary type in unions" >> prop { (st0: SimpleType, sjs: ISet[LeafEjs]) =>
      val st = sjs.findMax.flatMap(x => simpleTypeOf(x.ejs)) | st0
      val simpleSst = envT(cnt1, TypeF.simple[J, S](st)).embed
      val ssts = sjs.toIList.map(_.toSST)
      val (matching, nonmatching) = ssts.partition(sstConst.exist(j => simpleTypeOf(j) exists (_ ≟ st)))
      val simplified = matching.map(x => envTType.set(TypeF.simple(st))(x.project).embed)
      val coalesced = (simpleSst :: simplified).suml

      val compressed = (simpleSst :: ssts).suml.transCata[S](compression.coalescePrimary[J, Real])

      compressed must_= (coalesced :: nonmatching).suml
    }

    "combines multiple instances of a primary type in unions" >> prop {
      (sts: NonEmptyList[SimpleType], a1: NonEmptyList[J], a2: NonEmptyList[J]) =>
      val as1 = SST.fromEJson(Real(1), C(ejson.arr[J](a1.toList)).embed)
      val as2 = SST.fromEJson(Real(1), C(ejson.arr[J](a2.toList)).embed)
      val xs  = sts.list.map(st => envT(cnt1, TypeF.simple[J, S](st)).embed)
      val cnt = TypeStat.count(Real(xs.length + 2)).some

      val union = envT(cnt, TypeF.union[J, S](as1, as2, xs)).embed
      val sum   = (as1 :: as2 :: xs).suml

      union.transCata[S](compression.coalescePrimary[J, Real]) must_= sum
    }

    "no effect when a const's primary type not in the union" >> prop { ljs: IList[LeafEjs] =>
      val sum = ljs.foldMap(_.toSST)
      sum.transCata[S](compression.coalescePrimary[J, Real]) must_= sum
    }
  }

  "coalesceWithUnknown" >> {
    "merges known map entry with unknown entry when same primary type appears in unknown" >> prop {
      (head: (Char, LeafEjs), xs0: NonEmptyList[(Char, LeafEjs)], kv: (BigInt, LeafEjs)) =>

      val xs: IMap[Char, LeafEjs] = IMap.fromFoldable(xs0) + head

      val u1 = head.leftAs(SST.fromEJson(Real(1), E(ejson.char[J]('x')).embed)).map(_.toSST)
      val u2 = head.bimap(c =>
        envT(
          TypeStat.fromEJson(Real(1), E(ejson.char[J](c)).embed).some,
          TypeF.simple[J, S](SimpleType.Char)).embed,
        _.toSST)
      val kv1 = kv.bimap(i => E(ejson.int[J](i)).embed, _.toSST)
      val cs = xs.toList.map(_.bimap(c => E(ejson.char[J](c)).embed, _.toSST))
      val m = IMap.fromFoldable(kv1 :: cs)
      val sst1 = envT(cnt1, TypeF.map(m, u1.some)).embed
      val sst2 = envT(cnt1, TypeF.map(m, u2.some)).embed

      val a = cs.foldMap { case (j, s) => (SST.fromEJson(Real(1), j), s) }
      val b = IMap.singleton(kv1._1, kv1._2)
      val exp1 = envT(cnt1, TypeF.map(b, (a |+| u1).some)).embed
      val exp2 = envT(cnt1, TypeF.map(b, (a |+| u2).some)).embed

      (sst1.transCata[S](compression.coalesceWithUnknown[J, Real]) must_= exp1) and
      (sst2.transCata[S](compression.coalesceWithUnknown[J, Real]) must_= exp2)
    }

    "merges known map entry with unknown when primary type appears in unknown union" >> prop {
      (head: (Char, LeafEjs), xs0: NonEmptyList[(Char, LeafEjs)], kv: (BigInt, LeafEjs)) =>

      val xs: IMap[Char, LeafEjs] = IMap.fromFoldable(xs0) + head

      val u1 = head.leftAs(SST.fromEJson(Real(1), E(ejson.char[J]('x')).embed)).map(_.toSST)
      val u2 = head.bimap(c =>
        envT(
          TypeStat.fromEJson(Real(1), E(ejson.char[J](c)).embed).some,
          TypeF.simple[J, S](SimpleType.Char)).embed,
        _.toSST)
      val st = envT(cnt1, TypeF.simple[J, S](SimpleType.Dec)).embed
      val tp = envT(cnt1, TypeF.top[J, S]()).embed
      val u1u = u1.leftMap(s => envT(cnt1, TypeF.union[J, S](tp, s, IList(st))).embed)
      val u2u = u2.leftMap(s => envT(cnt1, TypeF.union[J, S](s, st, IList(tp))).embed)
      val kv1 = kv.bimap(i => E(ejson.int[J](i)).embed, _.toSST)
      val cs = xs.toList.map(_.bimap(c => E(ejson.char[J](c)).embed, _.toSST))
      val m = IMap.fromFoldable(kv1 :: cs)
      val sst1 = envT(cnt1, TypeF.map(m, u1u.some)).embed
      val sst2 = envT(cnt1, TypeF.map(m, u2u.some)).embed

      val a = cs.foldMap { case (j, s) => (SST.fromEJson(Real(1), j), s) }
      val b = IMap.singleton(kv1._1, kv1._2)
      val exp1 = envT(cnt1, TypeF.map(b, (a |+| u1u).some)).embed
      val exp2 = envT(cnt1, TypeF.map(b, (a |+| u2u).some)).embed

      (sst1.transCata[S](compression.coalesceWithUnknown[J, Real]) must_= exp1) and
      (sst2.transCata[S](compression.coalesceWithUnknown[J, Real]) must_= exp2)
    }

    "has no effect on maps when all keys are known" >> prop { xs: IList[(LeafEjs, LeafEjs)] =>
      val m   = IMap.fromFoldable(xs.map(_.bimap(_.ejs, _.toSST)))
      val sst = envT(cnt1, TypeF.map[J, S](m, None)).embed

      sst.transCata[S](compression.coalesceWithUnknown[J, Real]) must_= sst
    }

    "has no effect on maps when primary type not in unknown" >> prop { xs: IList[(LeafEjs, LeafEjs)] =>
      val m   = IMap.fromFoldable(xs.map(_.bimap(_.ejs, _.toSST)))
      val T   = envT(cnt1, TypeF.top[J, S]()).embed
      val sst = envT(cnt1, TypeF.map[J, S](m, Some((T, T)))).embed

      sst.transCata[S](compression.coalesceWithUnknown[J, Real]) must_= sst
    }
  }

  "limitArrays" >> {
    "compresses arrays longer than maxLen to the union of the members" >> prop {
      xs: List[BigInt] => (xs.length > 1) ==> {

      val alen: Positive = Positive(xs.length.toLong) getOrElse 1L
      val lt: Positive = Positive((xs.length - 1).toLong) getOrElse 1L
      val rlen = Real(xs.length).some
      val ints = xs.map(i => E(ejson.int[J](i)).embed)
      val xsst = SST.fromEJson(Real(1), C(ejson.arr[J](ints)).embed)

      val sum = ints.foldMap(x => SST.fromEJson(Real(1), x))
      val coll = TypeStat.coll(Real(1), rlen, rlen).some
      val lubarr = envT(coll, TypeF.arr[J, S](sum.right)).embed

      val req = xsst.transCata[S](compression.limitArrays[J, Real](alen))
      val rlt = xsst.transCata[S](compression.limitArrays[J, Real](lt))

      (req must_= xsst) and (rlt must_= lubarr)
    }}
  }

  "limitStrings" >> {
    "compresses strings longer than maxLen" >> prop { s: String => (s.length > 1) ==> {
      val plen: Positive = Positive(s.length.toLong) getOrElse 1L
      val lt: Positive = Positive((s.length - 1).toLong) getOrElse 1L
      val rlen = Real(s.length).some
      val str  = SST.fromEJson(Real(1), C(ejson.str[J](s)).embed)

      val char = envT(cnt1, TypeF.simple[J, S](SimpleType.Char)).embed
      val coll = TypeStat.coll(Real(1), rlen, rlen).some
      val arr  = envT(coll, TypeF.arr[J, S](char.right)).embed

      val req = str.transCata[S](compression.limitStrings[J, Real](plen))
      val rlt = str.transCata[S](compression.limitStrings[J, Real](lt))

      (req must_= str) and (rlt must_= arr)
    }}
  }

  "narrowUnion" >> {
    "reduces the largest group of values having the same primary type" >> prop {
      (bs: ISet[Byte], c1: Char, c2: Char, c3: Char, d1: BigDecimal) =>
      ((bs.size > 3) && (ISet.fromFoldable(IList(c1, c2, c3)).size ≟ 3)) ==> {

      val bytes = bs.toIList.map(b => SST.fromEJson(Real(1), E(ejson.byte[J](b)).embed))
      val chars = IList(c1, c2, c3).map(c => SST.fromEJson(Real(1), E(ejson.char[J](c)).embed))
      val dec = SST.fromEJson(Real(1), C(ejson.dec[J](d1)).embed)

      val compByte = envT(
        bytes.foldMap(_.copoint),
        TypeF.simple[J, S](SimpleType.Byte)
      ).embed

      val union0 = (dec :: chars ::: bytes).suml
      val union1 = envT(union0.copoint, TypeF.union[J, S](compByte, dec, chars)).embed

      union0.transCata[S](compression.narrowUnion(3L)) must_= union1
    }}

    "no effect on unions smaller or equal to maxSize" >> prop {
      (x: LeafEjs, y: LeafEjs, xs: IList[LeafEjs]) =>

      val union = envT(cnt1, TypeF.union[J, S](x.toSST, y.toSST, xs map (_.toSST))).embed

      Positive((xs.length + 2).toLong).cata(
        l => union.transCata[S](compression.narrowUnion(l)),
        union
      ) must_= union
    }
  }

  "z85EncodedBinary" >> {
    "compresses all encoded binary strings" >> prop { bs: Vector[Byte] =>
      val bytes   = ByteVector(bs)
      val encoded = z85.encode(bytes)
      val ejs     = E(ejson.meta[J](
                      C(ejson.str[J](encoded)).embed,
                      ejson.SizedTypeTag[J](ejson.BinaryTag, BigInt(bytes.size))
                    )).embed
      val sst     = SST.fromEJson(Real(1), ejs)

      val byte    = envT(cnt1, TypeF.simple[J, S](SimpleType.Byte)).embed
      val rsize   = Real(bytes.size).some
      val coll    = TypeStat.coll(Real(1), rsize, rsize).some
      val barr    = envT(coll, TypeF.arr[J, S](byte.right)).embed

      sst.transCata[S](compression.z85EncodedBinary[J, Real]) must_= barr
    }
  }
}
