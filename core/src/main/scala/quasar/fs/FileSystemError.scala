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
import quasar.Planner.PlannerError
import quasar.fp._

import argonaut.JsonObject
import matryoshka._
import pathy.Path.posixCodec
import scalaz._
import scalaz.syntax.show._

sealed trait FileSystemError

object FileSystemError {
  import QueryFile.ResultHandle
  import ReadFile.ReadHandle
  import WriteFile.WriteHandle

  final case class ExecutionFailed private (
    lp: Fix[LogicalPlan],
    reason: String,
    detail: JsonObject,
    cause: Option[Throwable]
  ) extends FileSystemError
  final case class PathErr private (e: PathError)
    extends FileSystemError
  final case class PlanningFailed private (
    lp: Fix[LogicalPlan],
    err: PlannerError
  ) extends FileSystemError
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

  val executionFailed = pPrism[FileSystemError, (Fix[LogicalPlan], String, JsonObject, Option[Throwable])] {
    case ExecutionFailed(lp, rsn, det, cs) => (lp, rsn, det, cs)
  } (ExecutionFailed.tupled)

  def executionFailed_(lp: Fix[LogicalPlan], reason: String): FileSystemError =
    executionFailed(lp, reason, JsonObject.empty, None)

  val pathErr = pPrism[FileSystemError, PathError] {
    case PathErr(err) => err
  } (PathErr)

  val planningFailed = pPrism[FileSystemError, (Fix[LogicalPlan], PlannerError)] {
    case PlanningFailed(lp, e) => (lp, e)
  } (PlanningFailed.tupled)

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
      case ExecutionFailed(_, rsn, _, c) =>
        s"Plan execution failed: $rsn, cause=${c.map(_.getMessage)}"
      case PathErr(e) =>
        e.shows
      case PlanningFailed(_, e) =>
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
