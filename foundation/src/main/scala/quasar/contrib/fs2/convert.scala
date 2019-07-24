/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.contrib.fs2

import slamdata.Predef.{Boolean, None, Option, Some, Throwable, Unit}

import java.util.Iterator
import java.util.stream.{Stream => JStream}
import scala.util.{Either, Left}

import cats.effect.{Concurrent, Resource, Sync}
import cats.syntax.monadError._
import fs2.concurrent.{NoneTerminatedQueue, Queue, SignallingRef}
import fs2.{Chunk, Stream}
import scalaz.{Functor, StreamT, Scalaz}, Scalaz._
import shims._

object convert {

  // java.util.stream.Stream

  def fromJavaStream[F[_], A](js: F[JStream[A]])(implicit F: Sync[F]): Stream[F, A] = {
    def getNext(i: Iterator[A]): F[Option[(A, Iterator[A])]] =
      F.delay(i.hasNext).ifM(
        F.delay(Some((i.next(), i))),
        F.pure(None))

    Stream.bracket(js)(s => F.delay(s.close()))
      .flatMap(s => Stream.unfoldEval(s.iterator)(getNext))
  }

  // scalaz.StreamT

  def fromChunkedStreamT[F[_]: Functor, A](chunks: StreamT[F, Chunk[A]]): Stream[F, A] =
    Stream.unfoldChunkEval(chunks)(_.step.map(_(
      yieldd = (c, st) => Some((c, st))
    , skip = st => Some((Chunk.empty, st))
    , done = None)))

  def fromStreamT[F[_]: Functor, A](st: StreamT[F, A]): Stream[F, A] =
    fromChunkedStreamT(st.map(Chunk.singleton(_)))

  def toStreamT[F[_]: Concurrent, A](s: Stream[F, A])
      : Resource[F, StreamT[F, A]] =
    Resource(chunkQ(s)) map { startQ =>
      StreamT.wrapEffect(startQ.map(q =>
        for {
          c <- StreamT.unfoldM(q)(_.dequeue1.map(_.sequence).rethrow.map(_.strengthR(q)))
          a <- StreamT.unfoldM(0)(i => (i < c.size).option((c(i), i + 1)).point[F])
        } yield a))
    }

  ////

  private def chunkQ[F[_], A](s: Stream[F, A])(implicit F: Concurrent[F])
      : F[(F[NoneTerminatedQueue[F, Either[Throwable, Chunk[A]]]], F[Unit])] =
    SignallingRef[F, Boolean](false) map { i =>
      val startQ = for {
        q <- Queue.synchronousNoneTerminated[F, Either[Throwable, Chunk[A]]]

        enqueue =
          s.chunks
            .attempt
            .interruptWhen(i)
            .noneTerminate
            .through(q.enqueue)
            .handleErrorWith(t => Stream.eval(q.enqueue1(Some(Left(t)))))

        _ <- F.start(enqueue.compile.drain)
      } yield q

      (startQ, i.set(true))
    }
}
