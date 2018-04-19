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

package quasar.fs

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect.Failure
import quasar.fp.numeric.{Natural, Positive}
import eu.timepit.refined.auto._

import quasar._, RenderTree.ops._
import quasar.effect.LiftedOps
import quasar.fp._

import monocle.Iso
import scalaz._
import scalaz.std.anyVal._
import scalaz.std.tuple._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.stream._

sealed abstract class ReadFile[A]

object ReadFile {
  final case class ReadHandle(file: AFile, id: Long)

  object ReadHandle {
    val tupleIso: Iso[ReadHandle, (AFile, Long)] =
      Iso((h: ReadHandle) => (h.file, h.id))((ReadHandle(_, _)).tupled)

    implicit val show: Show[ReadHandle] = Show.showFromToString

    // TODO: Switch to order once Order[Path[B,T,S]] exists
    implicit val equal: Equal[ReadHandle] = Equal.equalBy(tupleIso.get)
  }

  final case class Open(file: AFile, offset: Natural, limit: Option[Positive])
    extends ReadFile[FileSystemError \/ ReadHandle]

  final case class Read(h: ReadHandle)
    extends ReadFile[FileSystemError \/ Vector[Data]]

  final case class Close(h: ReadHandle)
    extends ReadFile[Unit]

  final class Ops[S[_]](implicit val unsafe: Unsafe[S]) {
    type F[A] = unsafe.FreeS[A]
    type M[A] = unsafe.M[A]

    /** Returns a process which produces data from the given file, beginning
      * at the specified offset. An optional limit may be supplied to restrict
      * the maximum amount of data read.
      */
    def scan(file: AFile, offset: Natural, limit: Option[Positive]): Process[M, Data] = {
      // TODO: use DataCursor.process for the appropriate cursor type
      def closeHandle(h: ReadHandle): Process[M, Nothing] =
        Process.eval_[M, Unit](unsafe.close(h).liftM[FileSystemErrT])
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def readUntilEmpty(h: ReadHandle): Process[M, Data] =
        Process.await(unsafe.read(h)) { data =>
          if (data.isEmpty)
            Process.halt
          else
            Process.emitAll(data) ++ readUntilEmpty(h)
        }

      Process.bracket(unsafe.open(file, offset, limit))(closeHandle)(readUntilEmpty)
    }

    /** Returns a process that produces all the data contained in the
      * given file.
      */
    def scanAll(file: AFile): Process[M, Data] =
      scan(file, 0L, None)

    def scanAll_(file: AFile)(implicit S0: Failure[FileSystemError, ?] :<: S): Process[Free[S, ?], Data] = {
      val nat: M ~> Free[S, ?] = λ[M ~> Free[S, ?]] { x => Failure.Ops[FileSystemError, S].unattempt(x.run) }
      scanAll(file).translate(nat)
    }
  }

  object Ops {
    implicit def apply[S[_]](implicit U: Unsafe[S]): Ops[S] =
      new Ops[S]
  }

  /** Low-level, unsafe operations. Clients are responsible for resource-safety
    * when using these.
    */
  final class Unsafe[S[_]](implicit S: ReadFile :<: S)
    extends LiftedOps[ReadFile, S] {

    type M[A] = FileSystemErrT[FreeS, A]

    /** Returns a read handle for the given file, positioned at the given
      * zero-indexed offset, that may be used to read chunks of data from the
      * file. An optional limit may be supplied to restrict the total amount of
      * data able to be read using the handle.
      *
      * Care must be taken to `close` the returned handle in order to avoid
      * potential resource leaks.
      */
    def open(file: AFile, offset: Natural, limit: Option[Positive]): M[ReadHandle] =
      EitherT(lift(Open(file, offset, limit)))

    /** Read a chunk of data from the file represented by the given handle.
      *
      * An empty `Vector` signals that all data has been read.
      */
    def read(rh: ReadHandle): M[Vector[Data]] =
      EitherT(lift(Read(rh)))

    /** Closes the given read handle, freeing any resources it was using. */
    def close(rh: ReadHandle): FreeS[Unit] =
      lift(Close(rh))
  }

  object Unsafe {
    implicit def apply[S[_]](implicit S: ReadFile :<: S): Unsafe[S] =
      new Unsafe[S]
  }

  implicit def renderTree[A]: RenderTree[ReadFile[A]] =
    new RenderTree[ReadFile[A]] {
      def render(rf: ReadFile[A]) = rf match {
        case Open(file, off, lim) => NonTerminal(List("Open"), None,
          file.render :: Terminal(List("Offset"), Some(off.toString)) ::
            lim.map(l => Terminal(List("Limit"), Some(l.toString))).toList)
        case Read(handle)         => Terminal(List("Read"), handle.shows.some)
        case Close(handle)        => Terminal(List("Close"), handle.shows.some)
      }
    }
}
