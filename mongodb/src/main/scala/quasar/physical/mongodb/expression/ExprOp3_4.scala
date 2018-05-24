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

package quasar.physical.mongodb.expression

import slamdata.Predef._
import quasar.fp._
import quasar.fp.ski._
import quasar.physical.mongodb.{Bson}

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

/** "Pipeline" operators added in MongoDB version 3.4. */
trait ExprOp3_4F[A]
object ExprOp3_4F {
  type BoundsF[A] = Option[(A, Option[A])]

  private def toList[A](bounds: BoundsF[A]) = bounds.fold(nil[A])(p => p._1 :: p._2.toList)

  implicit val traverseBounds: Traverse[BoundsF] = new Traverse[BoundsF] {
    private def unlist[A](l: List[A]): BoundsF[A] = l match {
      case Nil => none
      case a :: Nil => ((a, none)).some
      case a :: b :: _ => ((a, b.some)).some
    }

    def traverseImpl[G[_], A, B](fa: BoundsF[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[BoundsF[B]] =
      G.map(toList(fa).traverse(f))(unlist)
  }

  final case class $indexOfBytesF[A](string: A, substring: A, bounds: BoundsF[A]) extends ExprOp3_4F[A]
  final case class $indexOfCPF[A](string: A, substring: A, bounds: BoundsF[A]) extends ExprOp3_4F[A]
  final case class $splitF[A](string: A, delimiter: A) extends ExprOp3_4F[A]
  final case class $strLenBytesF[A](string: A) extends ExprOp3_4F[A]
  final case class $strLenCPF[A](string: A) extends ExprOp3_4F[A]
  final case class $substrBytesF[A](string: A, start: A, count: A) extends ExprOp3_4F[A]
  final case class $substrCPF[A](string: A, start: A, count: A) extends ExprOp3_4F[A]

  implicit val equal: Delay[Equal, ExprOp3_4F] =
    new Delay[Equal, ExprOp3_4F] {
      def apply[A](eq: Equal[A]) = {
        implicit val EQ: Equal[A] = eq
        Equal.equal {
          case ($indexOfBytesF(s1, t1, b1), $indexOfBytesF(s2, t2, b2)) => s1 ≟ s2 && t1 ≟ t2 && b1 ≟ b2
          case ($indexOfCPF(s1, t1, b1), $indexOfCPF(s2, t2, b2)) => s1 ≟ s2 && t1 ≟ t2 && b1 ≟ b2
          case ($splitF(s1, d1), $splitF(s2, d2)) => s1 ≟ s2 && d1 ≟ d2
          case ($strLenBytesF(s1), $strLenBytesF(s2)) => s1 ≟ s2
          case ($strLenCPF(s1), $strLenCPF(s2)) => s1 ≟ s2
          case ($substrBytesF(s1, i1, c1), $substrBytesF(s2, i2, c2)) => s1 ≟ s2 && i1 ≟ i2 && c1 ≟ c2
          case ($substrCPF(s1, i1, c1), $substrCPF(s2, i2, c2)) => s1 ≟ s2 && i1 ≟ i2 && c1 ≟ c2
          case _ => false
        }
      }
    }

  implicit val traverse: Traverse[ExprOp3_4F] = new Traverse[ExprOp3_4F] {
    def traverseImpl[G[_], A, B](fa: ExprOp3_4F[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[ExprOp3_4F[B]] =
      fa match {
        case $indexOfBytesF(s, t, b) => (f(s) |@| f(t) |@| Traverse[BoundsF].traverse(b)(f))($indexOfBytesF(_, _, _))
        case $indexOfCPF(s, t, b) => (f(s) |@| f(t) |@| Traverse[BoundsF].traverse(b)(f))($indexOfCPF(_, _, _))
        case $splitF(s, d) => (f(s) |@| f(d))($splitF(_, _))
        case $strLenBytesF(s) => G.map(f(s))($strLenBytesF(_))
        case $strLenCPF(s) => G.map(f(s))($strLenCPF(_))
        case $substrBytesF(s, i, c) => (f(s) |@| f(i) |@| f(c))($substrBytesF(_, _, _))
        case $substrCPF(s, i, c) => (f(s) |@| f(i) |@| f(c))($substrCPF(_, _, _))
      }
  }

  implicit def ops[F[_]: Functor](implicit I: ExprOp3_4F :<: F): ExprOpOps.Aux[ExprOp3_4F, F] = new ExprOpOps[ExprOp3_4F] {
    type OUT[A] = F[A]

    val simplify: AlgebraM[Option, ExprOp3_4F, Fix[F]] = κ(None)

    def bson: Algebra[ExprOp3_4F, Bson] = {
      case $indexOfBytesF(s, t, b) => Bson.Doc("$indexOfBytes" -> Bson.Arr(List(List(s, t), toList(b)).join))
      case $indexOfCPF(s, t, b) => Bson.Doc("$indexOfCP" -> Bson.Arr(List(List(s, t), toList(b)).join))
      case $splitF(s, d) => Bson.Doc("$split" -> Bson.Arr(s, d))
      case $strLenBytesF(s) => Bson.Doc("$strLenBytes" -> s)
      case $strLenCPF(s) => Bson.Doc("$strLenCP" -> s)
      case $substrBytesF(s, i, c) => Bson.Doc("$substrBytes" -> Bson.Arr(s, i, c))
      case $substrCPF(s, i, c) => Bson.Doc("$substrCP" -> Bson.Arr(s, i, c))
    }

    def rebase[T](base: T)(implicit T: Recursive.Aux[T, OUT]) = I(_).some

    def rewriteRefsM[M[_]: Monad](applyVar: PartialFunction[DocVar, M[DocVar]]) = κ(None)
  }

  final class fixpoint[T, EX[_]: Functor]
    (embed: EX[T] => T)
    (implicit I: ExprOp3_4F :<: EX) {
    @inline private def convert(expr: ExprOp3_4F[T]): T = embed(I.inj(expr))

    def $indexOfBytes(string: T, substring: T, bounds: Option[(T, Option[T])]) =
      convert($indexOfBytesF(string, substring, bounds))
    def $indexOfCP(string: T, substring: T, bounds: Option[(T, Option[T])]) =
      convert($indexOfCPF(string, substring, bounds))
    def $split(string: T, delimiter: T): T = convert($splitF(string, delimiter))
    def $strLenBytes(string: T): T = convert($strLenBytesF(string))
    def $strLenCP(string: T): T = convert($strLenCPF(string))
    def $substrBytes(string: T, start: T, index: T): T = convert($substrBytesF(string, start, index))
    def $substrCP(string: T, start: T, index: T): T = convert($substrCPF(string, start, index))
  }
}

object $indexOfBytes {
  def apply[EX[_], A](string: A, substring: A, bounds: Option[(A, Option[A])])(implicit I: ExprOp3_4F :<: EX): EX[A] =
    I.inj(ExprOp3_4F.$indexOfBytesF(string, substring, bounds))
}

object $indexOfCP {
  def apply[EX[_], A](string: A, substring: A, bounds: Option[(A, Option[A])])(implicit I: ExprOp3_4F :<: EX): EX[A] =
    I.inj(ExprOp3_4F.$indexOfCPF(string, substring, bounds))
}

object $split {
  def apply[EX[_], A](string: A, delimiter: A)(implicit I: ExprOp3_4F :<: EX): EX[A] =
    I.inj(ExprOp3_4F.$splitF(string, delimiter))
}

object $strLenBytes {
  def apply[EX[_], A](string: A)(implicit I: ExprOp3_4F :<: EX): EX[A] =
    I.inj(ExprOp3_4F.$strLenBytesF(string))
}

object $strLenCP {
  def apply[EX[_], A](string: A)(implicit I: ExprOp3_4F :<: EX): EX[A] =
    I.inj(ExprOp3_4F.$strLenCPF(string))

  def unapply[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], I: ExprOp3_4F :<: EX, EX: Functor[EX]): Option[T] =
    I.prj(T.project(expr)) collect {
      case ExprOp3_4F.$strLenCPF(str) => str
    }
}

object $substrBytes {
  def apply[EX[_], A](string: A, start: A, index: A)(implicit I: ExprOp3_4F :<: EX): EX[A] =
    I.inj(ExprOp3_4F.$substrBytesF(string, start, index))
}

object $substrCP {
  def apply[EX[_], A](string: A, start: A, index: A)(implicit I: ExprOp3_4F :<: EX): EX[A] =
    I.inj(ExprOp3_4F.$substrCPF(string, start, index))
}
