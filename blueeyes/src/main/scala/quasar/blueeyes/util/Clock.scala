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

package quasar.blueeyes.util

import quasar.blueeyes._
import scala.concurrent.Future
import java.time.LocalDateTime

trait Clock {

  /** Returns the current time.
    */
  def now(): LocalDateTime

  def instant(): Instant

  def nanoTime(): Long

  /** Times how long the specified future takes to be delivered.
    */
  def time[T](f: => Future[T]): Future[(Period, T)] = {
    val start = now()

    f.map { result =>
      val end = now()

      (start until end, result)
    }
  }

  /** Times a block of code.
    */
  def timeBlock[T](f: => T): (Period, T) = {
    val start = now()

    val result = f

    val end = now()

    (start until end, result)
  }
}

object Clock {
  val System = ClockSystem.realtimeClock
}

trait ClockSystem {
  implicit val realtimeClock = new Clock {
    def now(): LocalDateTime    = dateTime.now()
    def instant(): Instant = quasar.blueeyes.instant.now()
    def nanoTime(): Long   = System.nanoTime()
  }
}
object ClockSystem extends ClockSystem

trait ClockMock {
  protected class MockClock extends Clock {
    private var _now: LocalDateTime  = dateTime.zero
    private var _nanoTime: Long = 0

    def now() = _now

    def instant() = _now.toUtcInstant

    def nanoTime() = _nanoTime

    def setNow(dateTime: LocalDateTime): LocalDateTime = { _now = dateTime; _now }

    def setNow(millis: Long): LocalDateTime = dateTime fromMillis millis

    def setNanoTime(time: Long): Long = { _nanoTime = time; _nanoTime }
  }

  def newMockClock = new MockClock

  implicit val clockMock: MockClock = new MockClock
}
object ClockMock extends ClockMock
