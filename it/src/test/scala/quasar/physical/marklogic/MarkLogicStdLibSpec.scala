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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.{Data, TestConfig}
import quasar.fp.ski._
import quasar.fp.tree._
import quasar.fp.eitherT._
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._
import quasar.qscript._
import quasar.std._

import com.marklogic.xcc.ContentSource
import matryoshka._
import matryoshka.data.Fix
import org.scalacheck.Arbitrary.arbitrary
import org.specs2.execute._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class MarkLogicStdLibSpec extends StdLibSpec {

  def runner(contentSource: ContentSource) = new MapFuncStdLibTestRunner {
    type F[A] = WriterT[State[Long, ?], Prologs, A]
    type G[A] = EitherT[F, MarkLogicPlannerError, A]
    type H[A] = EitherT[F, Result, A]

    def nullaryMapFunc(
      prg: FreeMapA[Fix, Nothing],
      expected: Data
    ): Result = {
      val xqyPlan = planFreeMap(prg)(absurd)

      run(xqyPlan, expected)
    }

    def unaryMapFunc(
      prg: FreeMapA[Fix, UnaryArg],
      arg: Data,
      expected: Data
    ): Result = {
      val xqyPlan = asXqy(arg) flatMap (a1 => planFreeMap(prg)(κ(a1)))

      run(xqyPlan, expected)
    }

    def binaryMapFunc(
      prg: FreeMapA[Fix, BinaryArg],
      arg1: Data, arg2: Data,
      expected: Data
    ): Result = {
      val xqyPlan = (asXqy(arg1) |@| asXqy(arg2)).tupled flatMap {
        case (a1, a2) => planFreeMap(prg)(_.fold(a1, a2))
      }

      run(xqyPlan, expected)
    }

    def ternaryMapFunc(
      prg: FreeMapA[Fix, TernaryArg],
      arg1: Data, arg2: Data, arg3: Data,
      expected: Data
    ): Result = {
      val xqyPlan = (asXqy(arg1) |@| asXqy(arg2) |@| asXqy(arg3)).tupled flatMap {
        case (a1, a2, a3) => planFreeMap(prg)(_.fold(a1, a2, a3))
      }

      run(xqyPlan, expected)
    }

    def intDomain    = arbitrary[Long]   map (BigInt(_))
    def decDomain    = arbitrary[Double] map (BigDecimal(_))
    def stringDomain = StdLibTestRunner.genPrintableAscii

    ////

    private val cpColl = Prolog.defColl(DefaultCollationDecl(Collation.codepoint))

    private def planFreeMap[A](
      freeMap: FreeMapA[Fix, A])(
      recover: A => XQuery
    ): H[XQuery] =
      planMapFunc[Fix, G, A](freeMap)(recover) leftMap (e => ko(e.shows).toResult)

    private def run(plan: H[XQuery], expected: Data): Result = {
      val (prologs, xqy) = plan.run.run.eval(1) leftMap (_ insert cpColl)

      val result = xqy.traverse(body => for {
        qr <- SessionIO.evaluateModule_(MainModule(Version.`1.0-ml`, prologs, body))
        rs <- SessionIO.liftT(qr.toImmutableArray)
      } yield {
        rs.headOption
          .flatMap(xdmitem.toData[ErrorMessages \/ ?](_).toOption)
          .fold(ko("No results found."))(_ must beCloseTo(expected))
          .toResult
      })

      runSession(result).unsafePerformSync.merge
    }

    private val runSession: SessionIO ~> Task =
      ContentSourceIO.runNT(contentSource) compose ContentSourceIO.runSessionIO

    private def asXqy(d: Data): H[XQuery] =
      EncodeXQuery[EitherT[F, ErrorMessages, ?], Const[Data, ?]]
        .encodeXQuery(Const(d))
        .leftMap(errs => ko(errs intercalate ", ").toResult)
  }

  TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
    contentSourceAt(uri).map(cs => backend.name.shows should tests(runner(cs))).void
  }).unsafePerformSync
}
