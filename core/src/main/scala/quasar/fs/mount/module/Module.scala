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

package quasar.fs.mount.module

import slamdata.Predef._
import quasar._
import quasar.fp.numeric._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.{Failure, LiftedOps}
import quasar.fs._
import quasar.fs.mount._
import quasar.sql._

import matryoshka.data.Fix
import matryoshka.implicits._
import monocle._
import scalaz._, Scalaz._
import scalaz.stream.Process
import pathy.Path._

sealed abstract class Module[A]

object Module {

  final case class ResultHandle(run: Long) extends scala.AnyVal

  object ResultHandle {
    implicit val show: Show[ResultHandle] = Show.showFromToString

    implicit val order: Order[ResultHandle] = Order.orderBy(_.run)
  }

  sealed trait Error

  type ErrorT[M[_], A] = EitherT[M, Error, A]
  type Failure[A] = quasar.effect.Failure[Error, A]

  object Error {
    final case class FSError(fsErr: FileSystemError) extends Error
    final case class SemErrors(semErrs: NonEmptyList[SemanticError]) extends Error
    final case class ArgumentsMissing(missing: List[CIName]) extends Error
    final case class ParsingErr(parsingErr: ParsingError) extends Error

    val fsError = Prism.partial[Error, FileSystemError] {
      case FSError(fsErr) => fsErr
    } (FSError(_))

    val semErrors = Prism.partial[Error, NonEmptyList[SemanticError]] {
      case SemErrors(semErr) => semErr
    } (SemErrors(_))

    val argumentsMissing = Prism.partial[Error, List[CIName]] {
      case ArgumentsMissing(missing) => missing
    } (ArgumentsMissing(_))

    val parsingErr = Prism.partial[Error, ParsingError] {
      case ParsingErr(pErr) => pErr
    } (ParsingErr(_))

    implicit val show: Show[Error] =
      Show.shows {
        case FSError(e) => e.shows
        case SemErrors(e) =>
          s"Encountered the following semantic errors while attempting to invoke function: ${e.shows}"
        case ArgumentsMissing(missing) =>
          s"The following arguments are missing: $missing"
        case ParsingErr(e) => e.shows
      }
  }


  final case class InvokeModuleFunction(path: AFile, args: Map[String, String], offset: Natural, limit: Option[Positive])
    extends Module[Error \/ (List[Data] \/ ResultHandle)]

  final case class More(handle: ResultHandle)
    extends Module[FileSystemError \/ Vector[Data]]

  final case class Close(h: ResultHandle)
    extends Module[Unit]

  /** Low-level, unsafe operations. Clients are responsible for resource-safety
    * when using these.
    */
  final class Unsafe[S[_]](implicit S: Module :<: S)
    extends LiftedOps[Module, S] {

    type M[A] = ErrorT[FreeS, A]

    def invokeFunction(path: AFile, args: Map[String, String], offset: Natural, limit: Option[Positive]): M[List[Data] \/ ResultHandle] =
      EitherT(lift(InvokeModuleFunction(path, args, offset, limit)))

    /** Read a chunk of data from the file represented by the given handle.
      *
      * An empty `Vector` signals that all data has been read.
      */
    def more(h: ResultHandle): FileSystemErrT[FreeS, Vector[Data]] =
      EitherT(lift(More(h)))

    /** Closes the given read handle, freeing any resources it was using. */
    def close(h: ResultHandle): FreeS[Unit] =
      lift(Close(h))
  }

  object Unsafe {
    implicit def apply[S[_]](implicit S: Module :<: S): Unsafe[S] =
      new Unsafe[S]
  }

  final class Ops[S[_]](implicit val unsafe: Unsafe[S]) {
    type M[A] = unsafe.M[A]

    /** Returns mounts located at a path having the given prefix. */
    def invokeFunction(path: AFile, args: Map[String, String], offset: Natural, limit: Option[Positive]): Process[M, Data] = {
      // TODO: use DataCursor.process for the appropriate cursor type
      def closeHandle(dataOrHandle: List[Data] \/ ResultHandle): Process[M, Nothing] =
        dataOrHandle.fold(_ => Process.empty, h => Process.eval_[M, Unit](unsafe.close(h).liftM[ErrorT]))

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def readUntilEmpty(h: ResultHandle): Process[M, Data] =
        Process.await(unsafe.more(h).leftMap(Error.fsError(_))) { data =>
          if (data.isEmpty)
            Process.halt
          else
            Process.emitAll(data) ++ readUntilEmpty(h)
        }

      Process.bracket(unsafe.invokeFunction(path, args, offset, limit))(closeHandle) { dataOrHandle =>
        dataOrHandle.fold(
          data => Process.emitAll(data),
          handle => readUntilEmpty(handle))
      }
    }

    def invokeFunction_(path: AFile, args: Map[String, String], offset: Natural, limit: Option[Positive])(implicit
      SO: Failure :<: S
    ): Process[Free[S, ?], Data] = {
      val nat: M ~> Free[S, ?] = λ[M ~> Free[S, ?]] { x => Failure.Ops[Error, S].unattempt(x.run) }
      invokeFunction(path, args, offset, limit).translate(nat)
    }

  }

  object Ops {
    implicit def apply[S[_]](implicit S: Module :<: S): Ops[S] =
      new Ops[S]
  }

  object impl {

    import Error._
    import FileSystemError._
    import PathError._

    def default[S[_]](implicit query: QueryFile.Unsafe[S], mount: Mounting.Ops[S]): Module ~> Free[S, ?] =
      λ[Module ~> Free[S, ?]] {
        case InvokeModuleFunction(file, args, offset, limit) =>
          val notFoundError = fsError(pathErr(pathNotFound(file)))
          // case insensitive args
          val iArgs = args.map{ case (key, value) => (CIName(key), value)}
          val currentDir = fileParent(file)
          (for {
            moduleConfig <- EitherT(mount.lookupModuleConfig(currentDir)
                              .leftMap(e => semErrors(SemanticError.genericError(e.shows).wrapNel))
                              .run.toRight(notFoundError).run.map(_.join))
            name         =  fileName(file).value
            funcDec      <- EitherT(moduleConfig.declarations.find(_.name.value ≟ name)
                              .toRightDisjunction(notFoundError).point[Free[S, ?]])
            maybeAllArgs =  funcDec.args.map(arg => iArgs.get(arg)).sequence
            missingArgs  =  funcDec.args.filter(arg => !iArgs.contains(arg))
            userArgs     <- EitherT(maybeAllArgs.toRightDisjunction(argumentsMissing(missingArgs)).point[Free[S, ?]])
            parsedArgs   <- EitherT(userArgs.traverse(argString => fixParser.parseExpr(Query(argString)))
                              .leftMap(parsingErr(_)).point[Free[S, ?]])
            scopedExpr   =  ScopedExpr(invokeFunction[Fix[Sql]](CIName(name), parsedArgs).embed, moduleConfig.statements)
            sql          <- EitherT(resolveImports_(scopedExpr, currentDir).leftMap(e => semErrors(e.wrapNel)).run.leftMap(fsError(_))).flattenLeft
            dataOrLP     <- EitherT(quasar.queryPlan(sql, Variables.empty, basePath = currentDir, offset, limit)
                              .run.value.leftMap(semErrors(_)).point[Free[S, ?]])
            dataOrHandle <- dataOrLP.traverse(lp => EitherT(query.eval(lp).run.value).leftMap(fsError(_)))
          } yield dataOrHandle.map(h => ResultHandle(h.run))).run
        case More(handle)  => query.more(QueryFile.ResultHandle(handle.run)).run
        case Close(handle) => query.close(QueryFile.ResultHandle(handle.run))
      }
  }
}
