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

package quasar.datagen

import slamdata.Predef._

import quasar.contrib.matryoshka._
import quasar.contrib.spire.random.dist._
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.numeric.SampleStats
import quasar.sst.{strings, SST, StructuralType, TypeStat}
import quasar.tpe.{SimpleType, TypeF}

import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.matcher.MatchResult
import scalaz.{IList, IMap, ISet, NonEmptyList, Scalaz}, Scalaz._
import spire.random.rng.{Cmwc5, Utils}
import spire.std.double._

final class GenerateSpec extends quasar.Qspec {
  type J = Fix[EJson]
  type S = SST[J, Double]

  val J = Fixed[J]
  val prng = Cmwc5.fromSeed(Utils.seedFromLong(5))
  val maxL = 5.0

  def sst(ts: TypeStat[Double], tf: TypeF[J, S]): S =
    envT(ts, StructuralType.TypeST(tf)).embed

  def sstL(ts: TypeStat[Double], st: SimpleType): S =
    sst(ts, TypeF.simple(st))

  def sstS(n: Int, s: String): S =
    strings.widen[J, Double](n.toDouble, s).embed

  def tsJ(n: Int, j: J): TypeStat[Double] =
    TypeStat.fromEJson(n.toDouble, j)

  val tsJ1 = tsJ(1, _: J)

  def valuesShould(s: S)(f: J => Boolean): MatchResult[Any] =
    dist.sample(maxL, s).cata(
      _.replicateM(100).apply(prng).all(f) must beTrue,
      ko(s"Expected a Dist for ${s.shows}, found None."))

  "generating values from SSTs" should {
    "none for bottom" >> {
      dist.sample(maxL, sst(TypeStat.count(1.0), TypeF.bottom())) must beNone
    }

    "arbitrary leaf value for top" >> {
      val topSst = sst(TypeStat.count(1.0), TypeF.top())

      valuesShould(topSst) { j =>
        J.arr.isEmpty(j) && J.map.isEmpty(j) && J.meta.isEmpty(j)
      }
    }

    "weighted selection from a union" >> {
      val x = sstL(TypeStat.char(SampleStats.freq(0.0, 'a'.toDouble), 'a', 'a'), SimpleType.Char)
      val y = sstL(TypeStat.byte(100.0, 7.toByte, 7.toByte), SimpleType.Char)
      val z = sstL(TypeStat.bool(0.0, 0.0), SimpleType.Bool)
      val u = sst(TypeStat.count(100.0), TypeF.union(x, y, IList(z)))

      valuesShould(u)(_ ≟ J.byte(7.toByte))
    }

    "array" >> {
      "constant for known empty" >> {
        val asst = SST.fromEJson(1.0, J.arr(Nil))
        valuesShould(asst)(_ ≟ J.arr(Nil))
      }

      "fixed length of known values" >> {
        val a = sstL(tsJ1(J.char('a')), SimpleType.Char)
        val b = sstL(tsJ1(J.char('b')), SimpleType.Char)
        val c = sstL(tsJ1(J.char('c')), SimpleType.Char)

        val asst =
          sst(
            TypeStat.coll(1.0, Some(3.0), Some(3.0)),
            TypeF.arr(IList(a, b, c).left))

        valuesShould(asst)(_ ≟ J.arr(List(J.char('a'), J.char('b'), J.char('c'))))
      }

      "variable length of known values according to probabilities" >> {
        val a = sstL(tsJ(100, J.char('a')), SimpleType.Char)
        val b = sstL(tsJ(100, J.char('b')), SimpleType.Char)
        val c = sstL(tsJ( 70, J.char('c')), SimpleType.Char)
        val d = sstL(tsJ( 70, J.char('d')), SimpleType.Char)
        val e = sstL(tsJ( 30, J.char('e')), SimpleType.Char)

        val vsst =
          sst(
            TypeStat.coll(100.0, Some(2.0), Some(5.0)),
            TypeF.arr(IList(a, b, c, d, e).left))

        val ab = J.arr(List('a', 'b') map (J.char(_)))
        val abcd = J.arr(List('a', 'b', 'c', 'd') map (J.char(_)))
        val abcde = J.arr(List('a', 'b', 'c', 'd', 'e') map (J.char(_)))

        valuesShould(vsst)(List(ab, abcd, abcde) element _)
      }

      "uniform between bounds for lub array" >> {
        val k = sstL(tsJ1(J.char('k')), SimpleType.Char)
        val t = sstL(tsJ1(J.char('t')), SimpleType.Char)

        val kta =
          sst(
            TypeStat.coll(2.0, Some(3.0), Some(7.0)),
            TypeF.arr((k ⊹ t).right))

        valuesShould(kta)(J.arr.exist { js =>
          js.length >= 3 &&
          js.length <= 7 &&
          js.all(j => j >= J.char('k') && j <= J.char('t'))
        })
      }

      "uniform between empty and max len for unbounded lub array" >> {
        val w = sstL(tsJ1(J.char('w')), SimpleType.Char)
        val z = sstL(tsJ1(J.char('z')), SimpleType.Char)

        val wza =
          sst(
            TypeStat.coll(2.0, None, None),
            TypeF.arr((w ⊹ z).right))

        valuesShould(wza)(J.arr.exist { js =>
          js.length <= maxL &&
          js.all(j => j >= J.char('w') && j <= J.char('z'))
        })
      }
    }

    "map" >> {
      "known keys according to their independent probabilities" >> {
        val foo = J.str("foo")
        val bar = J.str("bar")
        val baz = J.str("baz")

        val msst =
          sst(
            TypeStat.coll(10.0, Some(1.0), Some(3.0)),
            TypeF.map(
              IMap(
                foo -> sst(TypeStat.count(10.0), TypeF.top()),
                bar -> sst(TypeStat.count(5.0), TypeF.top()),
                baz -> sst(TypeStat.count(3.0), TypeF.top())),
              None))

        val validKeys =
          ISet.fromList(List(
            ISet.fromList(List(foo)),
            ISet.fromList(List(foo, bar)),
            ISet.fromList(List(foo, baz)),
            ISet.fromList(List(foo, bar, baz))))

        valuesShould(msst)(J.imap.exist(m => validKeys.member(m.keySet)))
      }

      "unknown keys up to max size according to probability" >> {
        val name =
          J.str("name")

        val atof =
          SampleStats.fromFoldable(IList('a', 'b', 'c', 'd', 'e', 'f') map (_.toDouble))

        val unkk =
          sstL(
            TypeStat.char(atof, 'a', 'f'),
            SimpleType.Char)

        val msst =
          sst(
            TypeStat.coll(10.0, Some(1.0), Some(3.0)),
            TypeF.map(
              IMap(name -> sst(TypeStat.count(10.0), TypeF.top())),
              Some((unkk, sst(TypeStat.count(5.0), TypeF.top())))))

        valuesShould(msst)(J.imap.exist { m =>
          m.size <= 3 &&
          m.member(J.str("name")) &&
          m.delete(J.str("name")).keySet.all(J.char.exist(c => c >= 'a' && c <= 'f'))
        })
      }
    }

    "constant value for const" >> {
      val j = J.char('g')
      valuesShould(sst(tsJ1(j), TypeF.const(j)))(_ ≟ j)
    }

    "bool according to bernoulli dist" >> {
      val t = sstL(TypeStat.bool(100.0, 0.0), SimpleType.Bool)
      val f = sstL(TypeStat.bool(0.0, 10.0), SimpleType.Bool)
      valuesShould(t ⊹ f)(J.bool.nonEmpty)
    }

    "byte within range" >> {
      val bsst =
        NonEmptyList(3, 4, 5, 6, 7, 8, 9, 10, 11) foldMap1 { i =>
          sstL(tsJ1(J.byte(i.toByte)), SimpleType.Byte)
        }

      valuesShould(bsst)(J.byte.exist(b => (b >= 3.toByte) && (b <= 17.toByte)))
    }

    "char within range" >> {
      val csst =
        NonEmptyList('e', 'f', 'g', 'h', 'i', 'j', 'k', 'l') foldMap1 { c =>
          sstL(tsJ1(J.char(c)), SimpleType.Char)
        }

      valuesShould(csst)(J.char.exist(c => (c >= 'e') && (c <= 'l')))
    }

    "str within length range and char range" >> {
      val ssst =
        NonEmptyList("foo", "quux", "xex", "orp") foldMap1 { s =>
          val strStat = TypeStat.fromEJson(1.0, J.str(s))
          strings.compress[S, J, Double](strStat, s).embed
        }

      valuesShould(ssst)(J.str.exist { s =>
        (s.length ≟ 4 || s.length ≟ 3) &&
        s.toList.all(c => c >= 'e' && c <= 'x')
      })
    }

    "str within length range and positional char range" >> {
      val ssst =
        NonEmptyList("foo", "quux", "xex", "orp") foldMap1 (sstS(1, _))

      valuesShould(ssst)(J.str.exist { s =>
        (s.length ≟ 4 || s.length ≟ 3) &&
        (IList('f', 'q', 'x', 'o') element s(0)) &&
        (IList('o', 'u', 'e', 'r') element s(1)) &&
        (IList('o', 'u', 'x', 'p') element s(2)) &&
        (s.length ≟ 4).fold(s(3) ≟ 'x', true)
      })
    }

    "int according to dist, clamped by min/max" >> {
      val isst =
        NonEmptyList(3, 23534, 32, -34, 23, 234, 1123, 93749, 2738) foldMap1 { i =>
          sstL(tsJ1(J.int(BigInt(i))), SimpleType.Int)
        }

      valuesShould(isst)(J.int.exist(i => (i >= -34) && (i <= 93749)))
    }

    "dec according to dist, clamped by min/max" >> {
      val dsst =
        NonEmptyList(3.34, 4.24, 1.24984, 83.2334, 3494, 234.3453, 243.129983) foldMap1 { d =>
          sstL(tsJ1(J.dec(BigDecimal(d))), SimpleType.Dec)
        }

      valuesShould(dsst)(J.dec.exist(d => (d >= 1.24984) && (d <= 3494)))
    }
  }
}
