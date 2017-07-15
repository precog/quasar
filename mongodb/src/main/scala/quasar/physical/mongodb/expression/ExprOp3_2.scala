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

package quasar.physical.mongodb.expression

import slamdata.Predef._
import quasar.fp._
import quasar.fp.ski._
import quasar.physical.mongodb.{Bson}

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

/** "Pipeline" operators added in MongoDB version 3.2. */
trait ExprOp3_2F[A]
object ExprOp3_2F {
  final case class $sqrtF[A](value: A)                extends ExprOp3_2F[A]
  final case class $absF[A](value: A)                 extends ExprOp3_2F[A]
  final case class $logF[A](value: A, base: A)        extends ExprOp3_2F[A]
  final case class $log10F[A](value: A)               extends ExprOp3_2F[A]
  final case class $lnF[A](value: A)                  extends ExprOp3_2F[A]
  final case class $powF[A](value: A, exp: A)         extends ExprOp3_2F[A]
  final case class $expF[A](value: A)                 extends ExprOp3_2F[A]
  final case class $truncF[A](value: A)               extends ExprOp3_2F[A]
  final case class $ceilF[A](value: A)                extends ExprOp3_2F[A]
  final case class $floorF[A](value: A)               extends ExprOp3_2F[A]
  final case class $arrayElemAtF[A](array: A, idx: A) extends ExprOp3_2F[A]
  final case class $concatArraysF[A](arrays: List[A]) extends ExprOp3_2F[A]
  final case class $isArrayF[A](array: A)             extends ExprOp3_2F[A]

  implicit val equal: Delay[Equal, ExprOp3_2F] =
    new Delay[Equal, ExprOp3_2F] {
      def apply[A](eq: Equal[A]) = {
        implicit val EQ: Equal[A] = eq
        Equal.equal {
          case ($sqrtF(v1), $sqrtF(v2))                       => v1 ≟ v2
          case ($absF(v1), $absF(v2))                         => v1 ≟ v2
          case ($logF(v1, b1), $logF(v2, b2))                 => (v1 ≟ v2) && (b1 ≟ b2)
          case ($log10F(v1), $log10F(v2))                     => v1 ≟ v2
          case ($lnF(v1), $lnF(v2))                           => v1 ≟ v2
          case ($powF(v1, e1), $powF(v2, e2))                 => (v1 ≟ v2) && (e1 ≟ e2)
          case ($truncF(v1), $truncF(v2))                     => v1 ≟ v2
          case ($ceilF(v1), $ceilF(v2))                       => v1 ≟ v2
          case ($floorF(v1), $floorF(v2))                     => v1 ≟ v2
          case ($arrayElemAtF(a1, i1), $arrayElemAtF(a2, i2)) => a1 ≟ a2 && i1 ≟ i2
          case ($concatArraysF(a1), $concatArraysF(a2))       => a1 ≟ a2
          case ($isArrayF(a1), $isArrayF(a2))                 => a1 ≟ a2
          case _                                              => false
        }
      }
    }

  implicit val traverse: Traverse[ExprOp3_2F] = new Traverse[ExprOp3_2F] {
    def traverseImpl[G[_], A, B](fa: ExprOp3_2F[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[ExprOp3_2F[B]] =
      fa match {
        case $sqrtF(v)           => G.map(f(v))($sqrtF(_))
        case $absF(v)            => G.map(f(v))($absF(_))
        case $logF(v, base)      => (f(v) |@| f(base))($logF(_, _))
        case $log10F(v)          => G.map(f(v))($log10F(_))
        case $lnF(v)             => G.map(f(v))($lnF(_))
        case $powF(v, exp)       => (f(v) |@| f(exp))($powF(_, _))
        case $expF(v)            => G.map(f(v))($expF(_))
        case $truncF(v)          => G.map(f(v))($truncF(_))
        case $ceilF(v)           => G.map(f(v))($ceilF(_))
        case $floorF(v)          => G.map(f(v))($floorF(_))
        case $arrayElemAtF(a, i) => (f(a) |@| f(i))($arrayElemAtF(_, _))
        case $concatArraysF(a)   => G.map(a.traverse(f))($concatArraysF(_))
        case $isArrayF(a)        => G.map(f(a))($isArrayF(_))
      }
  }

  implicit def ops[F[_]: Functor](implicit I: ExprOp3_2F :<: F): ExprOpOps.Aux[ExprOp3_2F, F] = new ExprOpOps[ExprOp3_2F] {
    type OUT[A] = F[A]

    val simplify: AlgebraM[Option, ExprOp3_2F, Fix[F]] = κ(None)

    def bson: Algebra[ExprOp3_2F, Bson] = {
      case $sqrtF(value)             => Bson.Doc("$sqrt" -> value)
      case $absF(value)              => Bson.Doc("$abs" -> value)
      case $logF(value, base)        => Bson.Doc("$log" -> Bson.Arr(value, base))
      case $log10F(value)            => Bson.Doc("$log10" -> value)
      case $lnF(value)               => Bson.Doc("$ln" -> value)
      case $powF(value, exp)         => Bson.Doc("$pow" -> Bson.Arr(value, exp))
      case $truncF(value)            => Bson.Doc("$trunc" -> value)
      case $ceilF(value)             => Bson.Doc("$ceil" -> value)
      case $floorF(value)            => Bson.Doc("$floor" -> value)
      case $arrayElemAtF(array, idx) => Bson.Doc("$arrayElemAt" -> Bson.Arr(array, idx))
      case $concatArraysF(value)     => Bson.Doc("$concatArrays" -> Bson.Arr(value: _*))
      case $isArrayF(value)          => Bson.Doc("$isArray" -> value)
    }

    def rebase[T](base: T)(implicit T: Recursive.Aux[T, OUT]) = I(_).some

    def rewriteRefs0(applyVar: PartialFunction[DocVar, DocVar]) = κ(None)
  }

  final class fixpoint[T, EX[_]: Functor]
    (embed: EX[T] => T)
    (implicit I: ExprOp3_2F :<: EX) {
    @inline private def convert(expr: ExprOp3_2F[T]): T = embed(I.inj(expr))

    def $sqrt(value: T): T                = convert($sqrtF(value))
    def $abs(value: T): T                 = convert($absF(value))
    def $log(value: T, base: T): T        = convert($logF(value, base))
    def $log10(value: T): T               = convert($log10F(value))
    def $ln(value: T): T                  = convert($lnF(value))
    def $pow(value: T, exp: T): T         = convert($powF(value, exp))
    def $exp(value: T): T                 = convert($expF(value))
    def $trunc(value: T): T               = convert($truncF(value))
    def $ceil(value: T): T                = convert($ceilF(value))
    def $floor(value: T): T               = convert($floorF(value))
    def $arrayElemAt(array: T, idx: T): T = convert($arrayElemAtF(array, idx))
    def $concatArrays(value: List[T]): T  = convert($concatArraysF(value))
    def $isArray(value: T): T             = convert($isArrayF(value))
  }
}

object $sqrtF {
  def apply[EX[_], A](value: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$sqrtF(value))
}

object $absF {
  def apply[EX[_], A](value: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$absF(value))
}

object $logF {
  def apply[EX[_], A](value: A, base: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$logF(value, base))
}

object $log10F {
  def apply[EX[_], A](value: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$log10F(value))
}

object $lnF {
  def apply[EX[_], A](value: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$lnF(value))
}

object $powF {
  def apply[EX[_], A](value: A, exp: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$powF(value, exp))
}

object $expF {
  def apply[EX[_], A](value: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$expF(value))
}

object $truncF {
  def apply[EX[_], A](value: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$truncF(value))
}

object $ceilF {
  def apply[EX[_], A](value: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$ceilF(value))
}

object $floorF {
  def apply[EX[_], A](value: A)(implicit I: ExprOp3_2F :<: EX): EX[A] =
    I.inj(ExprOp3_2F.$floorF(value))
}
