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
import quasar.fp.ski._
import quasar.contrib.pathy.sandboxCurrent

import java.util.{zip => jzip}

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._
import scodec.bits.{ByteVector}
import scodec.interop.scalaz._

object Zip {
  // First construct a single Process of Ops which can be performed in
  // sequence to produce the entire archive.
  private sealed abstract class Op extends Product with Serializable
  private object Op {
    final case object Start                           extends Op
    final case class StartEntry(entry: jzip.ZipEntry) extends Op
    final case class Chunk(bytes: ByteVector)         extends Op
    final case object EndEntry                        extends Op
    final case object End                             extends Op
  }
  // Wrap up ZipOutputStream's statefulness in a class offering just two
  // mutating operations: one to accept an Op to be processed, and another
  // to poll for data that's been written.
  private class Buffer[F[_]: Monad] {
    // Assumes that the var is private to Buffer and exposed methods are private to
    // method zipFiles. Further assumes that usage by Process is without contention.
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private[this] var chunks = ByteVector.empty

    private def append(bytes: ByteVector) = chunks = chunks ++ bytes

    private val sink = {
      val os = new java.io.OutputStream {
        @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
        def write(b: Int) = append(ByteVector(b.toByte))

        // NB: overriding here to process each buffer-worth coming from the ZipOS in one call
        @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
        override def write(b: Array[Byte], off: Int, len: Int) = append(ByteVector(b, off, len))
      }
      new jzip.ZipOutputStream(os)
    }

    def accept(op: Op): F[Unit] = (op match {
      case Op.Start             => ()
      case Op.StartEntry(entry) => sink.putNextEntry(entry)
      case Op.Chunk(bytes)      => sink.write(bytes.toArray)
      case Op.EndEntry          => sink.closeEntry
      case Op.End               => sink.close
    }).point[F]

    def poll: F[ByteVector] = {
      val result = chunks
      chunks = ByteVector.empty
      result
    }.point[F]
  }

  def zipFiles[F[_]: Monad](files: Map[RelFile[Sandboxed], Process[F, ByteVector]]): Process[F, ByteVector] = {
    val ops: Process[F, Op] = {
      def fileOps(file: RelFile[Sandboxed], bytes: Process[F, ByteVector]) =
        Process.emit(Op.StartEntry(new jzip.ZipEntry(posixCodec.printPath(file)))) ++
          bytes.map(Op.Chunk(_)) ++
          Process.emit(Op.EndEntry)

      Process.emit(Op.Start) ++
        Process.emitAll(files.toList).flatMap((fileOps _).tupled) ++
        Process.emit(Op.End)
    }

    // Fold the allocation of Buffer instances in to the processing
    // of Ops, so that a new instance is created as needed each time
    // the resulting process is run, then flatMap so that each chunk
    // can be handled in Task.
    ops.zipWithState[Option[Buffer[F]]](None) {
      case (_, None)         => Some(new Buffer)
      case (Op.End, Some(_)) => None
      case (_, buf@Some(_))  => buf
    }.flatMap {
      case (Op.Start, _)   => Process.emit(ByteVector.empty)
      case (op, Some(buf)) =>
        Process.await(for {
          _ <- buf.accept(op)
          b <- buf.poll
        } yield b) { bytes =>
          if (bytes.size ≟ 0) Process.halt
          else Process.emit(bytes)
        }
      case (_, None)       => Process.fail(new RuntimeException("unexpected state"))
    }
  }

  def unzipFiles(zippedBytes: Process[Task, ByteVector]): EitherT[Task, String, Map[RelFile[Sandboxed], ByteVector]] = {
    def entry(zis: jzip.ZipInputStream): OptionT[Task, (String, ByteVector)] =
      for {
        entry <- OptionT(Task.delay(Option(zis.getNextEntry())))
        name  =  entry.getName
        bytes <- contents(zis).liftM[OptionT]
        _     <- Task.delay(zis.closeEntry()).liftM[OptionT]
      } yield (name, bytes)

    def contents(zis: jzip.ZipInputStream): Task[ByteVector] = {
      def read(bufSize: Int)(is: jzip.ZipInputStream): OptionT[Task, ByteVector] =
        OptionT(Task.delay {
          val buf = new Array[Byte](bufSize)
          val n = is.read(buf, 0, bufSize)
          (n >= 0) option ByteVector.view(buf).take(n.toLong)
        })

      Process.unfoldEval(zis)(z => read(4*1024)(z).strengthR(z).run).runFoldMap(ι)
    }

    def entries(zis: jzip.ZipInputStream): Process[Task, (String, ByteVector)] =
      Process.unfoldEval(zis)(z => entry(z).strengthR(z).run)

    def toPath(pathString: String): Task[RelFile[Sandboxed]] =
      posixCodec.parseRelFile(pathString).flatMap(sandboxCurrent).cata(
        p => Task.now(p),
        Task.fail(new RuntimeException(s"relative file path expected; found: $pathString")))

    val is = io.toInputStream(zippedBytes)
    EitherT((for {
      zis <- Task.delay(new jzip.ZipInputStream(is))
      es  <- entries(zis).runLog
      rez <- es.traverse { case (n: String, bs) => toPath(n).strengthR(bs) }
    } yield rez.toMap).attempt.map(_.leftMap {
      case x: jzip.ZipException => s"zip decoding error: $x"
      case x => s"$x"
    }))
  }
}
