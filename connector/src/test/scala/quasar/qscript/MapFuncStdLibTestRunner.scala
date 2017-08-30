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

package quasar.qscript

import slamdata.Predef._
import quasar.{Data, NullaryFunc, UnaryFunc, BinaryFunc, TernaryFunc, Mapping}
import quasar.fp._
import quasar.fp.ski.κ
import quasar.fp.tree.{UnaryArg, BinaryArg, TernaryArg}
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP, LogicalPlanR}
import quasar.ejson.implicits._
import quasar.std._

import scala.sys

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute.Result

import scalaz._, Scalaz._
import shapeless.Sized

/** The operations needed to execute the various StdLib tests for a QScript backend. */
trait MapFuncStdLibTestRunner extends StdLibTestRunner {
  val lpf = new LogicalPlanR[Fix[LP]]

  def nullaryMapFunc(
    prg: FreeMapA[Fix, Nothing],
    expected: Data
  ): Result

  def unaryMapFunc(
    prg: FreeMapA[Fix, UnaryArg],
    arg: Data,
    expected: Data
  ): Result

  def binaryMapFunc(
    prg: FreeMapA[Fix, BinaryArg],
    arg1: Data, arg2: Data,
    expected: Data
  ): Result

  def ternaryMapFunc(
    prg: FreeMapA[Fix, TernaryArg],
    arg1: Data, arg2: Data, arg3: Data,
    expected: Data
  ): Result

  val qsr = new Transform[Fix, QScriptTotal[Fix, ?]]

  /** Translate to MapFunc (common to all QScript backends). */
  def translate[A](prg: Fix[LP], args: Symbol => A): Free[MapFunc[Fix, ?], A] =
    prg.cata[Free[MapFunc[Fix, ?], A]] {
      case lp.InvokeUnapply(func @ NullaryFunc(_, _, _, _), Sized())
          if func.effect ≟ Mapping =>
        Free.roll((MapFunc.translateNullaryMapping[Fix, MapFunc[Fix, ?], Free[MapFunc[Fix, ?], A]].apply _)(func))

      case lp.InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1))
          if func.effect ≟ Mapping =>
        Free.roll((MapFunc.translateUnaryMapping[Fix, MapFunc[Fix, ?], Free[MapFunc[Fix, ?], A]].apply _)(func)(a1))

      case lp.InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2))
          if func.effect ≟ Mapping =>
        Free.roll((MapFunc.translateBinaryMapping[Fix, MapFunc[Fix, ?], Free[MapFunc[Fix, ?], A]] _)(func)(a1, a2))

      case lp.InvokeUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3))
          if func.effect ≟ Mapping =>
        Free.roll((MapFunc.translateTernaryMapping[Fix, MapFunc[Fix, ?], Free[MapFunc[Fix, ?], A]] _)(func)(a1, a2, a3))

      case lp.Free(sym) => Free.pure(args(sym))

      case lp.Constant(data) =>
        qsr.fromData(data).fold(
          _ => sys.error("invalid Data"),
          ej => Free.roll(MFC(MapFuncsCore.Constant[Fix, Free[MapFunc[Fix, ?], A]](ej))))

      case lp.TemporalTrunc(part, src) =>
        Free.roll(MFC(MapFuncsCore.TemporalTrunc(part, src)))
    }

  def absurd[A, B](a: A): B = sys.error("impossible!")

  def nullary(
    prg: Fix[LP],
    expected: Data
  ): Result = {
    val mf: FreeMapA[Fix, Nothing] =
      translate[Nothing](prg, absurd)

    nullaryMapFunc(mf, expected)
  }

  def unary(
    prg: Fix[LP] => Fix[LP],
    arg: Data,
    expected: Data
  ): Result = {
    val mf: FreeMapA[Fix, UnaryArg] =
      translate(prg(lpf.free('arg)), κ(UnaryArg._1))

    unaryMapFunc(mf, arg, expected)
  }

  def binary(
    prg: (Fix[LP], Fix[LP]) => Fix[LP],
    arg1: Data, arg2: Data,
    expected: Data
  ): Result = {
    val mf: FreeMapA[Fix, BinaryArg] =
      translate(prg(lpf.free('arg1), lpf.free('arg2)), {
        case 'arg1 => BinaryArg._1
        case 'arg2 => BinaryArg._2
      })

    binaryMapFunc(mf, arg1, arg2, expected)
  }

  def ternary(
    prg: (Fix[LP], Fix[LP], Fix[LP]) => Fix[LP],
    arg1: Data, arg2: Data, arg3: Data,
    expected: Data
  ): Result = {
    val mf: FreeMapA[Fix, TernaryArg] =
      translate(prg(lpf.free('arg1), lpf.free('arg2), lpf.free('arg3)), {
        case 'arg1 => TernaryArg._1
        case 'arg2 => TernaryArg._2
        case 'arg3 => TernaryArg._3
      })

    ternaryMapFunc(mf, arg1, arg2, arg3, expected)
  }

}
