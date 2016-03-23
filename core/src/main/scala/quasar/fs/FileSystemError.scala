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

package quasar.fs

import quasar.Predef._
import quasar.{Data, LogicalPlan}
import quasar.Planner.{PlannerError => PlannerErr}
import quasar.fp._

import matryoshka._
import pathy.Path.posixCodec
import scalaz._
import scalaz.syntax.show._

sealed trait FileSystemError

object FileSystemError {
  import QueryFile.ResultHandle
  import ReadFile.ReadHandle
  import WriteFile.WriteHandle

  final case class PathErr private (e: PathError)
    extends FileSystemError
  final case class PlannerError private (lp: Fix[LogicalPlan], err: PlannerErr)
    extends FileSystemError
  final case class UnknownResultHandle private (h: ResultHandle)
    extends FileSystemError
  final case class UnknownReadHandle private (h: ReadHandle)
    extends FileSystemError
  final case class UnknownWriteHandle private (h: WriteHandle)
    extends FileSystemError
  final case class PartialWrite private (numFailed: Int)
    extends FileSystemError
  final case class WriteFailed private (data: Data, reason: String)
    extends FileSystemError

  val pathErr = pPrism[FileSystemError, PathError] {
    case PathErr(err) => err
  } (PathErr)

  val plannerError = pPrism[FileSystemError, (Fix[LogicalPlan], PlannerErr)] {
    case PlannerError(lp, e) => (lp, e)
  } (PlannerError.tupled)

  val unknownResultHandle = pPrism[FileSystemError, ResultHandle] {
    case UnknownResultHandle(h) => h
  } (UnknownResultHandle)

  val unknownReadHandle = pPrism[FileSystemError, ReadHandle] {
    case UnknownReadHandle(h) => h
  } (UnknownReadHandle)

  val unknownWriteHandle = pPrism[FileSystemError, WriteHandle] {
    case UnknownWriteHandle(h) => h
  } (UnknownWriteHandle)

  val partialWrite = pPrism[FileSystemError, Int] {
    case PartialWrite(n) => n
  } (PartialWrite)

  val writeFailed = pPrism[FileSystemError, (Data, String)] {
    case WriteFailed(d, r) => (d, r)
  } (WriteFailed.tupled)

  implicit def fileSystemErrorShow: Show[FileSystemError] =
    Show.shows {
      case PathErr(e) =>
        e.shows
      case PlannerError(_, e) =>
        e.shows
      case UnknownResultHandle(h) =>
        s"Attempted to get results from an unknown or closed handle: ${h.run}"
      case UnknownReadHandle(h) =>
        s"Attempted to read from '${posixCodec.printPath(h.file)}' using an unknown or closed handle: ${h.id}"
      case UnknownWriteHandle(h) =>
        s"Attempted to write to '${posixCodec.printPath(h.file)}' using an unknown or closed handle: ${h.id}"
      case PartialWrite(n) =>
        s"Failed to write $n data."
      case WriteFailed(d, r) =>
        s"Failed to write datum: reason='$r', datum=${d.shows}"
    }
}
