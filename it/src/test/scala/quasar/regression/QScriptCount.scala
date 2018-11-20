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

package quasar.regression

import slamdata.Predef._

import scalaz.Monoid

final case class QScriptCount(
    interpretedRead: QScriptCount.InterpretedReadCount,
    read: QScriptCount.ReadCount,
    leftShift: QScriptCount.LeftShiftCount)

object QScriptCount {
  final case class InterpretedReadCount(count: Int)
  final case class ReadCount(count: Int)
  final case class LeftShiftCount(count: Int)

  implicit val monoid: Monoid[QScriptCount] =
    new Monoid[QScriptCount] {
      def zero: QScriptCount =
        QScriptCount(InterpretedReadCount(0), ReadCount(0), LeftShiftCount(0))

      def append(f1: QScriptCount, f2: => QScriptCount): QScriptCount =
        QScriptCount(
          InterpretedReadCount(f1.interpretedRead.count + f2.interpretedRead.count),
          ReadCount(f1.read.count + f2.read.count),
          LeftShiftCount(f1.leftShift.count + f2.leftShift.count))
    }

  def incrementInterpretedRead: QScriptCount =
    QScriptCount(InterpretedReadCount(1), ReadCount(0), LeftShiftCount(0))

  def incrementRead: QScriptCount =
    QScriptCount(InterpretedReadCount(0), ReadCount(1), LeftShiftCount(0))

  def incrementLeftShift: QScriptCount =
    QScriptCount(InterpretedReadCount(0), ReadCount(0), LeftShiftCount(1))
}
