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

import quasar.contrib.pathy.{AFile, APath, FPath}
import quasar.Data
import quasar.Data._int
import quasar.fs.{FileSystemError, FileSystemErrT, QueryFile}
import quasar.fs.PathError.invalidPath
import quasar.fp._, ski._
import quasar.frontend.logicalplan.{Read => LPRead, LogicalPlan}
import quasar.std.StdLib.agg

import matryoshka.{Hole => _, _}
import matryoshka.patterns._
import matryoshka.implicits._
import matryoshka.data._
import pathy.Path.{file, peel, file1}
import scalaz._, Scalaz._
import simulacrum.typeclass

package object analysis {

  @typeclass
  trait Cardinality[F[_]] {
    def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, F, Int]
  }

  @typeclass
  trait Cost[F[_]] {
    def evaluate[M[_] : Monad](pathCard : APath => M[Int]): GAlgebraM[(Int, ?), M, F, Int]
  }

  def pathCard[S[_]](
    implicit queryOps: QueryFile.Ops[S]
  ): APath => FileSystemErrT[Free[S, ?], Int] = (apath: APath) => {
    val afile: Option[AFile] = peel(apath).map {
      case (dirPath, \/-(fileName)) => dirPath </> file1(fileName)
      case (dirPath, -\/(dirName))  => dirPath </> file(dirName.value)
    }

    def lpFromPath[LP](p: FPath)(implicit LP: Corecursive.Aux[LP, LogicalPlan]): LP =
      LP.embed(agg.Count(LP.embed(LPRead(p))))

    val lp: FileSystemErrT[Free[S, ?], Fix[LogicalPlan]] =
      EitherT.fromDisjunction(afile.map(p => lpFromPath[Fix[LogicalPlan]](p)) \/> FileSystemError.pathErr(invalidPath(apath, "Cardinality unsupported")))
    val dataFromLp: Fix[LogicalPlan] => FileSystemErrT[Free[S, ?], Option[Data]] =
      (lp: Fix[LogicalPlan]) => queryOps.first(lp).mapT(_.value)

    (lp >>= (dataFromLp)) map (_.flatMap(d => _int.getOption(d).map(_.toInt)) | 0)
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
        val I = Inject[QScriptCore[T, ?], QScriptTotal[T, ?]]

        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, QScriptCore[T, ?], Int] = {
          case Map(card, f) => card.point[M]
          case Reduce(card, bucket, reducers, repair) =>
            bucket.fold(κ(card / 2), {
              case MFC(MapFuncsCore.Constant(v)) => 1
              case _ => card / 2
            }).point[M]
          case Sort(card, bucket, orders) => card.point[M]
          case Filter(card, f) => (card / 2).point[M]
          case Subset(card, from, sel, count) => {
            val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            def c = count.fold(κ(card / 2), {
              case I(Map(_, MapFuncsCore.IntLit(v))) => v.toInt
              case _ => card / 2
            })
            sel match {
              case Drop => card - c
              case _ => c
            }
          }.point[M]
          case LeftShift(card, struct, id, repair) => (card * 10).point[M]
          case Union(card, lBranch, rBranch) => {
            val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            (lBranch.cataM(interpretM(κ(card.point[M]), compile)) |@| rBranch.cataM(interpretM(κ(card.point[M]), compile))) { _ + _}
          }
          case Unreferenced() => 1.point[M]
        }
      }
    implicit def projectBucket[T[_[_]] : RecursiveT: ShowT]: Cardinality[ProjectBucket[T, ?]] =
      new Cardinality[ProjectBucket[T, ?]] {
        def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, ProjectBucket[T, ?], Int] = {
          case BucketField(card, _, _) => card.point[M]
          case BucketIndex(card, _, _) => card.point[M]
        }
      }

    implicit def equiJoin[T[_[_]]: RecursiveT: ShowT]: Cardinality[EquiJoin[T, ?]] =
      new Cardinality[EquiJoin[T, ?]] {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, EquiJoin[T, ?], Int] = {
          case EquiJoin(card, lBranch, rBranch, _, _, _, _) =>
            val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            (lBranch.cataM(interpretM(κ(card.point[M]), compile)) |@| rBranch.cataM(interpretM(κ(card.point[M]), compile))) { _ * _}
        }
      }

    implicit def thetaJoin[T[_[_]] : RecursiveT : ShowT]: Cardinality[ThetaJoin[T, ?]] =
      new Cardinality[ThetaJoin[T, ?]] {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, ThetaJoin[T, ?], Int] = {
          case ThetaJoin(card, lBranch, rBranch, _, _, _) =>
            val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            (lBranch.cataM(interpretM(κ(card.point[M]), compile)) |@| rBranch.cataM(interpretM(κ(card.point[M]), compile))) { _ * _}
        }
      }

    implicit def deadEnd: Cardinality[Const[DeadEnd, ?]] =
      new Cardinality[Const[DeadEnd, ?]] {
        def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, Const[DeadEnd, ?], Int] = κ(1.point[M])
      }

    implicit def coproduct[F[_], G[_]](
      implicit F: Cardinality[F], G: Cardinality[G]):
        Cardinality[Coproduct[F, G, ?]] =
      new Cardinality[Coproduct[F, G, ?]] {
        def calculate[M[_] : Monad](pathCard: APath => M[Int]): AlgebraM[M, Coproduct[F, G, ?], Int] =
          _.run.fold(F.calculate(pathCard), G.calculate(pathCard))
      }
  }

  /**
    * This is a "generic" implementation for `Cost` that can be used by any connector.
    * Can be used for newly created connectors. More mature connectors should provide
    * their own instance that will take into account connector-specific information.
    */
  object Cost {
    implicit def deadEnd: Cost[Const[DeadEnd, ?]] = new Cost[Const[DeadEnd, ?]] {
        def evaluate[M[_] : Monad](pathCard: APath => M[Int]): GAlgebraM[(Int, ?), M, Const[DeadEnd, ?], Int] =
          (qs: Const[DeadEnd, (Int, Int)]) => 1.point[M]
      }

    implicit def read[A]: Cost[Const[Read[A], ?]] =
      new Cost[Const[Read[A], ?]] {
        def evaluate[M[_] : Monad](pathCard: APath => M[Int]): GAlgebraM[(Int, ?), M, Const[Read[A], ?], Int] =
          (qs: Const[Read[A], (Int, Int)]) => 1.point[M]
      }
    implicit def shiftedRead[A]: Cost[Const[ShiftedRead[A], ?]] =
      new Cost[Const[ShiftedRead[A], ?]] {
        def evaluate[M[_] : Monad](pathCard: APath => M[Int]): GAlgebraM[(Int, ?), M, Const[ShiftedRead[A], ?], Int] =
          (qs: Const[ShiftedRead[A], (Int, Int)]) => 1.point[M]
      }

    implicit def qscriptCore[T[_[_]]: RecursiveT: ShowT]: Cost[QScriptCore[T, ?]] =
      new Cost[QScriptCore[T, ?]] {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def evaluate[M[_] : Monad](pathCard: APath => M[Int]): GAlgebraM[(Int, ?), M, QScriptCore[T, ?], Int] = {
          case Map((card, cost), f) => (card + cost).point[M]
          case Reduce((card, cost), bucket, reducers, repair) => (card + cost).point[M]
          case Sort((card, cost), bucket, orders) => (card + cost).point[M]
          case Filter((card, cost), f) => (card + cost).point[M]
          case Subset((card, cost), from, sel, count) => (card + cost).point[M]
          case LeftShift((card, cost), struct, id, repair) => (card + cost).point[M]
          case Union((card, cost), lBranch, rBranch) => {
            val compileCardinality = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            val compileCost = Cost[QScriptTotal[T, ?]].evaluate(pathCard)
            val left = lBranch.zygoM(interpretM(κ(card.point[M]), compileCardinality), ginterpretM(κ(cost.point[M]), compileCost))
            val right = lBranch.zygoM(interpretM(κ(card.point[M]), compileCardinality), ginterpretM(κ(cost.point[M]), compileCost))
            (left |@| right)( (l, r) => (l + r) / 2)
          }
          case Unreferenced() => 0.point[M]
        }
      }

    implicit def projectBucket[T[_[_]]]: Cost[ProjectBucket[T, ?]] =
      new Cost[ProjectBucket[T, ?]] {
        def evaluate[M[_] : Monad](pathCard: APath => M[Int]): GAlgebraM[(Int, ?), M, ProjectBucket[T, ?], Int] = {
          case BucketField((card, cost), _, _) => (card + cost).point[M]
          case BucketIndex((card, cost), _, _) => (card + cost).point[M]
        }
      }

    implicit def equiJoin[T[_[_]]: RecursiveT: ShowT]: Cost[EquiJoin[T, ?]] =
      new Cost[EquiJoin[T, ?]] {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def evaluate[M[_] : Monad](pathCard: APath => M[Int]): GAlgebraM[(Int, ?), M, EquiJoin[T, ?], Int] = {
          case EquiJoin((card, cost), lBranch, rBranch, lKey, rKey, jt, combine) =>
            val compileCardinality = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            val compileCost = Cost[QScriptTotal[T, ?]].evaluate(pathCard)
            (lBranch.zygoM(interpretM(κ(card.point[M]), compileCardinality), ginterpretM(κ(cost.point[M]), compileCost)) |@|
            rBranch.zygoM(interpretM(κ(card.point[M]), compileCardinality), ginterpretM(κ(cost.point[M]), compileCost))) { _ + _ }
        }
      }
    implicit def thetaJoin[T[_[_]] : RecursiveT : ShowT]: Cost[ThetaJoin[T, ?]] =
      new Cost[ThetaJoin[T, ?]] {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def evaluate[M[_] : Monad](pathCard: APath => M[Int]): GAlgebraM[(Int, ?), M, ThetaJoin[T, ?], Int] = {
          case ThetaJoin((card, cost), lBranch, rBranch, _, _, _) => {
            val compileCardinality = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            val compileCost = Cost[QScriptTotal[T, ?]].evaluate(pathCard)
            (lBranch.zygoM(interpretM(κ(card.point[M]), compileCardinality), ginterpretM(κ(cost.point[M]), compileCost)) |@|
            rBranch.zygoM(interpretM(κ(card.point[M]), compileCardinality), ginterpretM(κ(cost.point[M]), compileCost))) { _ + _ }
          }
        }
      }

    implicit def coproduct[F[_], G[_]](implicit F: Cost[F], G: Cost[G]):
        Cost[Coproduct[F, G, ?]] =
      new Cost[Coproduct[F, G, ?]] {
        def evaluate[M[_] : Monad](pathCard: APath => M[Int]): GAlgebraM[(Int, ?), M, Coproduct[F, G, ?], Int] =
          _.run.fold(F.evaluate(pathCard), G.evaluate(pathCard))
      }

  }

}
