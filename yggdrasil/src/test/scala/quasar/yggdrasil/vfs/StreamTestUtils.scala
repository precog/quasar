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

package quasar.yggdrasil.vfs

import cats.effect.IO

import fs2.{Sink, Stream}

import scalaz.Free

import iotaz.CopK

import scodec.bits.ByteVector

private[vfs] object StreamTestUtils {
  def assertionSinkBV(pred: ByteVector => Unit): Sink[POSIXWithIO, ByteVector] = { s =>
    s flatMap { bv =>
      Stream suspend {
        pred(bv)

        val I = CopK.Inject[IO, POSIXWithIOCopK]

        // this is tricky, but we're doing it specifically so that the
        // number of IO suspensions is equal between failure and
        // success of the predicate (predicate assertion failure will be
        // a suspended IO.raiseError)
        Stream.eval(Free.liftF(I.inj(IO.pure(()))))
      }
    }
  }

  def assertionSink(pred: String => Unit): Sink[POSIXWithIO, ByteVector] =
    assertionSinkBV(bv => pred(new String(bv.toArray)))
}
