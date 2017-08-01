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

package quasar.yggdrasil.vfs

import fs2.{Sink, Stream}

import scalaz.{Coproduct, Free, Inject}
import scalaz.concurrent.Task

import scodec.bits.ByteVector

private[vfs] object StreamTestUtils {
  def assertionSink(pred: String => Unit): Sink[POSIXWithTask, ByteVector] = { s =>
    s flatMap { bv =>
      Stream suspend {
        pred(new String(bv.toArray))

        val I = Inject[Task, Coproduct[POSIXOp, Task, ?]]

        // this is tricky, but we're doing it specifically so that the
        // number of Task suspensions is equal between failure and
        // success of the predicate (predicate assertion failure will be
        // a suspended Task.fail)
        Stream.eval(Free.liftF(I.inj(Task.now(()))))
      }
    }
  }
}
