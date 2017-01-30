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

package quasar.physical.couchbase

import quasar.Predef._
import quasar.{Data => QData, TestConfig}
import quasar.common.PhaseResultT
import quasar.effect.{MonotonicSeq, Read}
import quasar.fp.free._
import quasar.fp.reflNT
import quasar.fp.ski.κ
import quasar.fp.tree.{UnaryArg, BinaryArg, TernaryArg}
import quasar.fs.FileSystemError
import quasar.physical.couchbase.common.{CBDataCodec, Context}
import quasar.physical.couchbase.fs.{context, FsType}
import quasar.physical.couchbase.fs.queryfile.n1qlResults
import quasar.physical.couchbase.planner.CBPhaseLog
import quasar.physical.couchbase.planner.Planner.mapFuncPlanner
import quasar.qscript.{MapFunc, MapFuncStdLibTestRunner, FreeMapA}
import quasar.std.StdLibSpec

import java.time.LocalDate

import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka.patterns._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.execute.Result
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class CouchbaseStdLibSpec extends StdLibSpec {
  import N1QL._, Select._

  implicit val codec = CBDataCodec

  type Eff0[A] = Coproduct[MonotonicSeq, Read[Context, ?], A]
  type Eff[A]  = Coproduct[Task, Eff0, A]

  type F[A] = Free[Eff, A]
  type M[A] = CBPhaseLog[F, A]

  def run[A](
    fm: Free[MapFunc[Fix, ?], A],
    args: A => QData,
    expected: QData,
    ctx: Context
  ): Result = {

    def argN1ql(d: QData): M[Fix[N1QL]] = Data[Fix[N1QL]](d).embed.η[M]

    val q: M[Fix[N1QL]] =
      fm.cataM(interpretM(a => argN1ql(args(a)), mapFuncPlanner[Fix, F].plan))

    val r: FileSystemError \/ (String, Vector[QData]) =
      (for {
        qq <- q.leftMap(FileSystemError.qscriptPlanningFailed(_))
        s  =  Select[Fix[N1QL]](
                Value(false),
                ResultExpr(qq, Id("v").some).wrapNel,
                keyspace = None,
                unnest   = None,
                let      = Nil,
                filter   = None,
                groupBy  = None,
                orderBy  = Nil).embed
        r  <- n1qlResults[Fix, Eff](s) ∘ (_ >>= {
                case QData.Obj(v) => v.values.toVector
                case v            => Vector(v)
              })
        q  <- EitherT(RenderQuery.compact(s).leftMap(
                FileSystemError.qscriptPlanningFailed(_)
              ).point[Free[Eff, ?]].liftM[PhaseResultT])
      } yield (q, r)).run.run.foldMap(
        reflNT[Task]                            :+:
        MonotonicSeq.fromZero.unsafePerformSync :+:
        Read.constant[Task, Context](ctx)
      ).unsafePerformSync._2

      (r must beRightDisjunction.like { case (q, Vector(d)) =>
        d must beCloseTo(expected).updateMessage(_ ⊹ s"\nquery: $q")
      }).toResult
  }

  def runner(ctx: Context) = new MapFuncStdLibTestRunner {
    def nullaryMapFunc(
      prg: FreeMapA[Fix, Nothing],
      expected: QData
    ): Result =
      skipped

    def unaryMapFunc(
      prg: FreeMapA[Fix, UnaryArg],
      arg: QData,
      expected: QData
    ): Result =
      run(prg, κ(arg), expected, ctx)

    def binaryMapFunc(
      prg: FreeMapA[Fix, BinaryArg],
      arg1: QData, arg2: QData,
      expected: QData
    ): Result =
      run[BinaryArg](prg, _.fold(arg1, arg2), expected, ctx)

    def ternaryMapFunc(
      prg: FreeMapA[Fix, TernaryArg],
      arg1: QData, arg2: QData, arg3: QData,
      expected: QData
    ): Result =
      run[TernaryArg](prg, _.fold(arg1, arg2, arg3), expected, ctx)

    // TODO: remove let once '\\' is fixed in N1QL
    val genPrintableAsciiSansBackslash: Gen[String] =
      Gen.listOf(Gen.frequency(
        (64, Gen.choose('\u0020', '\u005B')),
        (36, Gen.choose('\u005D', '\u007e'))
      )).map(_.mkString)

    val intDomain: Gen[BigInt] = arbitrary[Int] map (BigInt(_))
    val decDomain: Gen[BigDecimal] = arbitrary[Double] map (BigDecimal(_))
    val stringDomain: Gen[String] = genPrintableAsciiSansBackslash

    val dateDomain: Gen[LocalDate] =
      Gen.choose(
        LocalDate.of(1, 1, 1).toEpochDay,
        LocalDate.of(9999, 12, 31).toEpochDay
      ) ∘ (LocalDate.ofEpochDay(_))

  }

  TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
    context(uri).fold(
      err => Task.fail(new RuntimeException(err.shows)),
      ctx => Task.now(backend.name.shows should tests(runner(ctx)))
    ).void
  }).unsafePerformSync

}
