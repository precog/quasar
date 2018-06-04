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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar.{Data => QData, TestConfig, Type => QType}
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.iota.copkTraverse
import quasar.fp.ski.κ
import quasar.fp.tree.{UnaryArg, BinaryArg, TernaryArg}
import quasar.fs.FileSystemError
import quasar.physical.couchbase.common.CBDataCodec
import quasar.physical.couchbase.fs.{parseConfig, FsType}
import quasar.physical.couchbase.Couchbase._, QueryFileModule.n1qlResults
import quasar.physical.couchbase.planner.Planner.mapFuncPlanner
import quasar.fs.Planner.PlannerError
import quasar.qscript._
import quasar.std.StdLibSpec
import quasar.time.{DateGenerators, DateTimeInterval}

import java.time._

import matryoshka._
import matryoshka.data.{Fix, freeRecursive}
import matryoshka.implicits._
import matryoshka.patterns._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.execute._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class CouchbaseStdLibSpec extends StdLibSpec {
  import N1QL._, Select._

  implicit val codec = CBDataCodec

  type F[A] = Free[Eff, A]
  type M[A] = EitherT[F, PlannerError, A]

  private def ignoreSomeUnary(prg: FreeMapA[Fix, UnaryArg], arg: QData)(run: => Result)
      : Result =
    (prg, arg) match {
      case (_, _) if isTemporal(arg.dataType) => pending
      case (ExtractFunc(MapFuncsCore.OffsetDate(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.OffsetDateTime(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.OffsetTime(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.LocalDate(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.LocalDateTime(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.LocalTime(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.ToTimestamp(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.Interval(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.Length(_)), QData.Str(s)) if !isPrintableAscii(s) =>
        Pending("only printable ascii supported")
      case _ => run
    }

  private def ignoreSomeBinary(prg: FreeMapA[Fix, BinaryArg], arg1: QData, arg2: QData)(run: => Result)
      : Result =
    (prg, arg1, arg2) match {
      case (_, _, _) if isTemporal(arg1.dataType) || isTemporal(arg2.dataType) => pending
      case (ExtractFunc(MapFuncsCore.IfUndefined(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.ProjectKey(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.And(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.Or(_,_)), _, _) => pending
      case _ => run
    }

  private def ignoreSomeTernary(prg: FreeMapA[Fix, TernaryArg], arg1: QData, arg2: QData, arg3: QData)(run: => Result)
      : Result =
    (prg, arg1, arg2, arg3) match {
      case (_, _, _, _) if isTemporal(arg1.dataType) || isTemporal(arg2.dataType) || isTemporal(arg3.dataType) => pending
      case (ExtractFunc(MapFuncsCore.Substring(_,_,_)), QData.Str(s), _, _) if !isPrintableAscii(s) =>
        Pending("only printable ascii supported")
      case _ => run
    }

  private def isTemporal(tpe: QType): Boolean =
    (tpe == QType.OffsetDateTime) || (tpe == QType.OffsetDate) || (tpe == QType.OffsetTime) ||
      (tpe == QType.LocalDateTime) || (tpe == QType.LocalDate) || (tpe == QType.LocalTime) ||
      (tpe == QType.Interval)

  def run[A](
    fm: Free[MapFunc[Fix, ?], A],
    args: A => QData,
    expected: QData,
    cfg: Config
  ): Result = {

    def argN1ql(d: QData): M[Fix[N1QL]] = Data[Fix[N1QL]](d).embed.η[M]

    val r: FileSystemError \/ (String, Vector[QData]) = (
      for {
        q  <- ME.unattempt(
                fm.cataM[M, Fix[N1QL]](interpretM(a =>
                    argN1ql(args(a)), mapFuncPlanner[Fix, EitherT[F, PlannerError, ?]].plan))
                  .leftMap(FileSystemError.qscriptPlanningFailed(_)).run.liftB)
        s  =  Select(
                Value(false),
                ResultExpr(q, Id("v").some).wrapNel,
                keyspace = None,
                join     = None,
                unnest   = None,
                let      = Nil,
                filter   = None,
                groupBy  = None,
                orderBy  = Nil).embed
        r  <- n1qlResults(s) ∘ (_ >>= {
                case QData.Obj(v) => v.values.toVector
                case v            => Vector(v)
              })
        rq <- ME.unattempt(
                RenderQuery.compact(s).leftMap(FileSystemError.qscriptPlanningFailed(_)).η[Backend])
      } yield (rq, r)
    ).run.run.run(cfg).foldMap(fs.interp.unsafePerformSync).unsafePerformSync._2

    (r must be_\/-.like { case (q, Vector(d)) =>
      d must beCloseTo(expected).updateMessage(_ ⊹ s"\nquery: $q")
    }).toResult
  }

  def runner(cfg: Config) = new MapFuncStdLibTestRunner {
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
      ignoreSomeUnary(prg, arg)(run(prg, κ(arg), expected, cfg))

    def binaryMapFunc(
      prg: FreeMapA[Fix, BinaryArg],
      arg1: QData, arg2: QData,
      expected: QData
    ): Result =
      ignoreSomeBinary(prg, arg1, arg2)(run[BinaryArg](prg, _.fold(arg1, arg2), expected, cfg))

    def ternaryMapFunc(
      prg: FreeMapA[Fix, TernaryArg],
      arg1: QData, arg2: QData, arg3: QData,
      expected: QData
    ): Result =
      ignoreSomeTernary(prg, arg1, arg2, arg3)(run[TernaryArg](prg, _.fold(arg1, arg2, arg3), expected, cfg))

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

    def timeDomain: Gen[LocalTime] = DateGenerators.genLocalTime
    def intervalDomain: Gen[DateTimeInterval] = DateGenerators.genDateTimeInterval
    def timezoneDomain: Gen[ZoneOffset] = DateGenerators.genZoneOffset
  }

  TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
    parseConfig(uri).fold(
      err => Task.fail(new RuntimeException(err.shows)),
      cfg => Task.now(backend.name.shows should tests(runner(cfg)))
    ).join.void
  }).unsafePerformSync

}
