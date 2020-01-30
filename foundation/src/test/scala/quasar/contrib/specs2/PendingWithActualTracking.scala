/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.contrib.specs2

import slamdata.Predef._
import quasar.contrib.pathy.Helpers._

import java.io.{File => JFile}

import org.specs2.execute._

/**
  * Specs2 trait that is like "pendingUntilFixed" but additionally tracks
  * actual results. It fails a test in case the expected actual is not equal to
  * the (actual) actual.
  *
  * This trait can run in 2 modes:
  * - TestMode: the usual mode for testing
  * - WriteMode: when there are new actuals, running in this mode will
  *     overwrite the old actuals with the new ones
  *
  * Note: Style of this code is adopted from specs2 (PendingUntilFixed.scala).
  */

sealed trait Mode
case object TestMode extends Mode
case object WriteMode extends Mode

trait PendingWithActualTracking {

  val mode: Mode

  implicit def toPendingWithActualTracking[T : AsResult](t: =>T)
    : PendingWithActualTrackingClass[T] = new PendingWithActualTrackingClass(t)

  def unsafeRead(f: JFile): String =
    jtextContents(f).unsafePerformSync

  def unsafeWrite(jFile: jFile, contents: String): Unit = {
    java.nio.file.Files.write(
      jFile.toPath,
      contents.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    ()
  }

  class PendingWithActualTrackingClass[T : AsResult](t: =>T) {
    def pendingWithActual(m: String, file: JFile): Result =
      mode match {
        case TestMode => pendingWithActualTestMode(m, file)
        case WriteMode => pendingWithActualWriteMode(m, file)
      }

    def pendingWithActualTestMode(m: String, file: JFile): Result = ResultExecution.execute(AsResult(t)) match {
      case s @ Success(_,_) =>
        Failure(m + " Fixed now, you should remove the 'pendingWithActual' marker")
      case Failure(_, _, _, FailureDetails(actual, _)) =>
        val expectedActual = unsafeRead(file)
        if (actual != expectedActual)
          Failure(m + " Behaviour has changed. Please review the test and set new expectation. New actual is: " + actual)
        else
          Pending(m + " Pending until fixed (actual unchanged)")
      case other =>
        Failure(m + " Behaviour has changed. Please review the test and set new expectation. Test result is: " + other)
    }

    def pendingWithActualWriteMode(m: String, file: JFile): Result = ResultExecution.execute(AsResult(t)) match {
      case Failure(_, _, _, FailureDetails(actual, _)) =>
        unsafeWrite(file, actual)
        Success(m + s" Wrote file with new actual $file")
      case other =>
        Failure(m + " Unexpected format of test result: actual can not be written")
    }

  }
}
