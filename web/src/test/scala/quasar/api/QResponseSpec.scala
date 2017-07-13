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

package quasar.api

import slamdata.Predef._
import quasar.fp._, free._
import quasar.fp.ski._

import org.http4s.dsl._
import org.http4s.headers.Host
import scalaz._
import scalaz.syntax.applicative._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scodec.bits.ByteVector
import scala.math.max

class QResponseSpec extends quasar.Qspec {
  import QResponse.{PROCESS_EFFECT_THRESHOLD_BYTES, HttpResponseStreamFailureException}
  import QResponseSpec._

  type StrIO[A] = Coproduct[Str, Task, A]
  type StrIOM[A] = Free[StrIO, A]

  def str(s: String): StrIOM[String] =
    Free.liftF(Inject[Str, StrIO].inj(Str(s, ι)))

  def strs(ss: String*): Process[StrIOM, String] =
    Process.emitAll(ss).map(str).eval

  val errHost: Host = Host("example.com", 443)

  def evalStr(errs: String*): StrIO ~> ResponseOr = {
    val f = new (Str ~> ResponseOr) {
      def apply[A](a: Str[A]) =
        if (errs.toSet contains a.s)
          EitherT.left(BadRequest(a.s).putHeaders(errHost).withBody("FAIL"))
        else
          a.k(a.s).point[ResponseOr]
    }

    f :+: liftMT[Task, ResponseT]
  }

  "toHttpResponse" should {
    "sucessful evaluation" >> {
      "has same status" >> {
        val res = QResponse.empty[StrIO].toHttpResponse(evalStr())
        res.unsafePerformSync.status must_== NoContent
      }

      "has same headers" >> {
        val qr = QResponse.json[String, StrIO](Ok, "foo")
        val res = qr.toHttpResponse(evalStr())
        res.unsafePerformSync.headers.toList must_== qr.headers.toList
      }

      "has body of interpreted values" >> {
        val qr = QResponse.streaming[StrIO, String](
          strs("a", "b", "c", "d", "e"))
        val res = qr.toHttpResponse(evalStr())
        res.as[String].unsafePerformSync must_== "abcde"
      }
    }

    "failed evaluation" >> {
      val failStream =
        QResponse.streaming[StrIO, String](strs("one", "two", "three"))

      "has alternate response status" >> {
        failStream.toHttpResponse(evalStr("one"))
          .unsafePerformSync.status must_== BadRequest
      }

      "has alternate response headers" >> {
        failStream.toHttpResponse(evalStr("one"))
          .unsafePerformSync.headers.get(Host) must beSome(errHost)
      }

      "has alternate response body" >> {
        failStream.toHttpResponse(evalStr("one"))
          .as[String].unsafePerformSync must_== "FAIL"
      }

      "responds with alternate response when a small amount of data before first effect" >> {
        val pad = Process.emit(ByteVector.low(max(0L, PROCESS_EFFECT_THRESHOLD_BYTES - 1)))
        val padStream = failStream.copy(body = pad.append[StrIOM, ByteVector](failStream.body))
        padStream.toHttpResponse(evalStr("one")).as[String].unsafePerformSync must_== "FAIL"
      }

      "responds with alternate response when other internal effects before first effect" >> {
        val hi  = ByteVector.high(1)
        val pad = Process.emit(ByteVector.low(max(0L, PROCESS_EFFECT_THRESHOLD_BYTES / 2)))
        val stm = pad.append[StrIOM, ByteVector](failStream.body intersperse hi)

        failStream.copy(body = stm).toHttpResponse(evalStr("one"))
          .as[String].unsafePerformSync must_== "FAIL"
      }

      "results in response stream failure exception when fails in middle of stream" >> {
        val pad = Process.emit(ByteVector.low(PROCESS_EFFECT_THRESHOLD_BYTES))
        failStream.copy(body = pad.append[StrIOM, ByteVector](failStream.body))
          .toHttpResponse(evalStr("two"))
          .as[String].unsafePerformSync must throwA[HttpResponseStreamFailureException]
      }
    }
  }
}

object QResponseSpec {
  final case class Str[A](s: String, k: String => A)

  object Str {
    implicit val functor: Functor[Str] =
      new Functor[Str] {
        def map[A, B](str: Str[A])(f: A => B) =
          Str(str.s, f compose str.k)
      }
  }
}
