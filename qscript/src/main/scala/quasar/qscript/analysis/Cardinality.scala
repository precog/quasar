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

package quasar.qscript.analysis

import slamdata.Predef.{Map => _, _}

import quasar.contrib.pathy.APath
import quasar.fp._, ski._
import quasar.contrib.iota._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.patterns._
import matryoshka.implicits._
import matryoshka.data._
import scalaz._, Scalaz._
import simulacrum.typeclass
import iotaz.{ CopK, TListK, TNilK }
import iotaz.TListK.:::

@typeclass
trait Cardinality[F[_]] {
  def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, F, Int]
}

object Cardinality {

  implicit def read[A]: Cardinality[Const[Read[A], ?]] =
    new Cardinality[Const[Read[A], ?]] {
      def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, Const[Read[A], ?], Int] =
        (qs: Const[Read[A], Int]) => 1.point[M]
    }

  implicit def shiftedRead[A <: APath]: Cardinality[Const[ShiftedRead[A], ?]] =
    new Cardinality[Const[ShiftedRead[A], ?]] {
      def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, Const[ShiftedRead[A], ?], Int] =
        (qs: Const[ShiftedRead[A], Int]) => pathCard(qs.getConst.path)
    }

  implicit def qscriptCore[T[_[_]]: RecursiveT: ShowT]: Cardinality[QScriptCore[T, ?]] =
    new Cardinality[QScriptCore[T, ?]] {
      val I = CopK.Inject[QScriptCore[T, ?], QScriptTotal[T, ?]]

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, QScriptCore[T, ?], Int] = {
        case Map(card, f) => card.point[M]
        case Reduce(card, bucket, reducers, repair) =>
          (bucket match {
            case Nil => 1
            case _   => card / 2
          }).point[M]
        case Sort(card, bucket, orders) => card.point[M]
        case Filter(card, f) => (card / 2).point[M]
        case Subset(card, from, sel, count) => {
          val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
          def c = count.fold(κ(card / 2), {
            case I(Map(_, MapFuncsCore.RecIntLit(v))) => v.toInt
            case _ => card / 2
          })
          sel match {
            case Drop => card - c
            case _ => c
          }
        }.point[M]

        case LeftShift(card, _, _, _, _, _) => (card * 10).point[M]

        case Union(card, lBranch, rBranch) =>
          interpretBranches(card, pathCard)(lBranch, rBranch)(_ + _)

        case Unreferenced() => 1.point[M]
      }
    }

  implicit def projectBucket[T[_[_]] : RecursiveT: ShowT]: Cardinality[ProjectBucket[T, ?]] =
    new Cardinality[ProjectBucket[T, ?]] {
      def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, ProjectBucket[T, ?], Int] = {
        case BucketKey(card, _, _) => card.point[M]
        case BucketIndex(card, _, _) => card.point[M]
      }
    }

  implicit def equiJoin[T[_[_]]: RecursiveT: ShowT]: Cardinality[EquiJoin[T, ?]] =
    new Cardinality[EquiJoin[T, ?]] {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, EquiJoin[T, ?], Int] = {
        case EquiJoin(card, lBranch, rBranch, _, _, _) =>
          interpretBranches(card, pathCard)(lBranch, rBranch)(_ * _)
      }
    }

  implicit def thetaJoin[T[_[_]] : RecursiveT : ShowT]: Cardinality[ThetaJoin[T, ?]] =
    new Cardinality[ThetaJoin[T, ?]] {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, ThetaJoin[T, ?], Int] = {
        case ThetaJoin(card, lBranch, rBranch, _, _, _) =>
          interpretBranches(card, pathCard)(lBranch, rBranch)(_ * _)
      }
    }

  implicit def deadEnd: Cardinality[Const[DeadEnd, ?]] =
    new Cardinality[Const[DeadEnd, ?]] {
      def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, Const[DeadEnd, ?], Int] = κ(1.point[M])
    }

  implicit def copk[LL <: TListK](implicit M: Materializer[LL]): Cardinality[CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[LL <: TListK] {
    def materialize(offset: Int): Cardinality[CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[F[_]](
      implicit
      F: Cardinality[F]
    ): Materializer[F ::: TNilK] = new Materializer[F ::: TNilK] {
      override def materialize(offset: Int): Cardinality[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new Cardinality[CopK[F ::: TNilK, ?]] {
          override def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, CopK[F ::: TNilK, ?], Int] = {
            case I(fa) => F.calculate(pathCard).apply(fa)
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_], LL <: TListK](
      implicit
      F: Cardinality[F],
      LL: Materializer[LL]
    ): Materializer[F ::: LL] = new Materializer[F ::: LL] {
      override def materialize(offset: Int): Cardinality[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new Cardinality[CopK[F ::: LL, ?]] {
          override def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, CopK[F ::: LL, ?], Int] = {
            case I(fa) => F.calculate(pathCard).apply(fa)
            case other => LL.materialize(offset + 1).calculate(pathCard).apply(other.asInstanceOf[CopK[LL, Int]])
          }
        }
      }
    }
  }

  ////////

  private def interpretBranches[T[_[_]]: RecursiveT: ShowT, M[_]: Monad](
    card: Int, pathCard: APath => M[Int])(
    left: FreeQS[T], right: FreeQS[T])(
    ap: (Int, Int) => Int)
      : M[Int] = {
    val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)

    def interpretBranch(qs: FreeQS[T]): M[Int] =
      qs.cataM[M, Int](
        interpretM[M, QScriptTotal[T, ?], Hole, Int](κ(card.point[M]), compile))

    (interpretBranch(left) |@| interpretBranch(right))(ap)
  }
}
