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

package quasar.contrib.specs2

import slamdata.Predef._

import java.io.{File => JFile}

import org.specs2.execute._

/**
 * Specs2 trait helper that can be used to *write* new actuals that are usually
 * *read* and tested with `PendingWithActualTracking`. This trait is designed
 * to make writing these new actuals as easy as possible: all you have to do is
 * mix in this trait instead of `PendingWithActualTracking` and the test will
 * run all `pendingWithActual` specs in write mode instead.
 *
 * Note: Style of this code is adopted from specs2 (PendingUntilFixed.scala).
 */
trait PendingWithActualTrackingWriteMode {

  implicit def toPendingWithActualTrackingWriteMode[T: AsResult](
      t: => T): PendingWithActualTrackingWriteModeClass[T] =
    new PendingWithActualTrackingWriteModeClass(t)

  private def unsafeWrite(jFile: jFile, contents: String): Unit = {
    java.nio.file.Files
      .write(jFile.toPath, contents.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    ()
  }

  class PendingWithActualTrackingWriteModeClass[T: AsResult](t: => T) {
    def pendingWithActual(m: String, file: JFile): Result =
      ResultExecution.execute(AsResult(t)) match {
        case f @ Failure(msg, e, stackTrace, FailureDetails(actual, expected)) =>
          unsafeWrite(file, actual)
          Success(m + s" Wrote file with new actual $file")
        case other =>
          Failure(m + " Unexpected format of test result: actual can not be written")
      }
  }
}

object PendingWithActualTrackingWriteMode extends PendingWithActualTrackingWriteMode
