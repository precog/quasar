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

import quasar.{RenderTree, Terminal}
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz.zipper._
import quasar.ejson
import quasar.ejson.{EJson, ExtEJson, CommonEJson, Extension}
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.κ
import quasar.qscript._
import quasar.tpe.CompositeType

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import simulacrum.typeclass
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

/** Computes the statically known shape of `F[_]`.
  *
  * NB: Given a QScript typer and optional schema info, this could
  *     be replaced by a structural type analysis.
  */
@typeclass
trait Outline[F[_]] {
  def outlineƒ: Algebra[F, Outline.Shape]
}

object Outline extends OutlineInstances {
  type Shape = Free[EJson, Figure]

  sealed abstract class Figure

  object Figure {
    /** Indicates invalid structure. */
    case object Undefined extends Figure
    /** A value with uknown shape. */
    case object Unknown extends Figure
    /** A structure of the given type. */
    final case class Struct(tpe: CompositeType) extends Figure

    val undefined: Figure = Undefined
    val unknown: Figure = Unknown
    val struct: CompositeType => Figure = Struct(_)

    implicit val equal: Equal[Figure] =
      Equal.equalBy {
        case Undefined => false.left[CompositeType]
        case Unknown => true.left[CompositeType]
        case Struct(t)  => t.right[Boolean]
      }

    implicit val show: Show[Figure] =
      Show.shows {
        case Undefined => "Undefined"
        case Unknown => "Unknown"
        case Struct(t) => t.shows
      }

    implicit val renderTree: RenderTree[Figure] =
      RenderTree.make {
        case Undefined => Terminal(List("⊥"), none)
        case Unknown   => Terminal(List("?"), none)
        case Struct(t) => Terminal(List("Struct"), some(CompositeType.name(t)))
      }
  }

  val undefinedF: Shape = Free.pure(Figure.undefined)
  val unknownF: Shape = Free.pure(Figure.unknown)
  val arrF: Shape = Free.pure(Figure.struct(CompositeType.Arr))
  val mapF: Shape = Free.pure(Figure.struct(CompositeType.Map))

  object outline {
    def apply[F[_]] = new PartiallyApplied[F]
    final class PartiallyApplied[F[_]] {
      def apply[T](t: T)(
          implicit
          T: Recursive.Aux[T, F],
          F: Functor[F],
          O: Outline[F])
          : Shape =
        t.cata(O.outlineƒ)
    }
  }

  def outlineF[F[_]: Functor, A]
      (fa: Free[F, A])
      (f: A => Shape)
      (implicit O: Outline[F])
      : Shape =
    fa.cata(interpret(f, O.outlineƒ))

  /** Annotate a structure with its `Shape`.
    *
    * TODO{matryoshka}: Generalize once matryoshka is updated to 0.21.0+
    */
  object outlined {
    def apply[F[_]] = new PartiallyApplied[F]
    final class PartiallyApplied[F[_]] {
      def apply[T](t: T)(
          implicit
          TR: Recursive.Aux[T, F],
          F: Functor[F],
          O: Outline[F])
          : Cofree[F, Shape] =
        t.cata(attributeAlgebra[F, Shape](O.outlineƒ))
    }
  }

  // TODO: Is there a way to avoid this? Metadata must be elided prior to
  //       comparing for equality.
  implicit def freeEJsonEqual[A: Equal]: Equal[Free[EJson, A]] =
    new Equal[Free[EJson, A]] {
      type T = Free[EJson, A]

      implicit val ejsonEqual = Extension.structuralEqual

      // TODO: Is it possible to refactor EJson.elideMetadata to be reusable here?
      val elideMetadata: CoEnv[A, EJson, T] => CoEnv[A, EJson, T] = totally {
        case CoEnv(\/-(ExtEJson(ejson.Meta(v, _)))) => v.project
      }

      def equal(x: T, y: T) =
        freeEqual[EJson].apply(Equal[A]).equal(
          x.transCata[T](elideMetadata),
          y.transCata[T](elideMetadata))
    }
}

sealed abstract class OutlineInstances {
  import Outline._

  implicit def copk[LL <: TListK](implicit M: Materializer[LL]): Outline[CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[LL <: TListK] {
    def materialize(offset: Int): Outline[CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[F[_]](
      implicit
      F: Outline[F]
    ): Materializer[F ::: TNilK] = new Materializer[F ::: TNilK] {
      override def materialize(offset: Int): Outline[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new Outline[CopK[F ::: TNilK, ?]] {
          override def outlineƒ: Algebra[CopK[F ::: TNilK, ?], Shape] = {
            case I(fa) => F.outlineƒ(fa)
          }
        }
      }
    }


    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_], LL <: TListK](
      implicit
      F: Outline[F],
      LL: Materializer[LL]
    ): Materializer[F ::: LL] = new Materializer[F ::: LL] {
      override def materialize(offset: Int): Outline[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new Outline[CopK[F ::: LL, ?]] {
          override def outlineƒ: Algebra[CopK[F ::: LL, ?], Shape] = {
            case I(fa) => F.outlineƒ(fa)
            case other => LL.materialize(offset + 1).outlineƒ(other.asInstanceOf[CopK[LL, Shape]])
          }
        }
      }
    }
  }

  implicit def mapFuncCore[T[_[_]]: BirecursiveT]: Outline[MapFuncCore[T, ?]] =
    new Outline[MapFuncCore[T, ?]] {
      import MapFuncsCore._

      def outlineƒ: Algebra[MapFuncCore[T, ?], Shape] = {
        case Constant(ejs) => convertToFree[EJson, Figure](ejs)
        case Undefined() => undefinedF

        // Maps
        case MakeMap(k @ Embed(CoEnv(\/-(_))), v) =>
          Free.roll(ExtEJson(ejson.Map(List((k, v)))))

        case MakeMap(_, _) => mapF

        case ConcatMaps(
          Embed(CoEnv(\/-(ExtEJson(ejson.Map(ls))))),
          Embed(CoEnv(\/-(ExtEJson(ejson.Map(rs)))))) =>

          Free.roll(ExtEJson(ejson.Map(ls.toZipper.fold(rs) { z =>
            rs.foldLeft(z) { (kvs, r) =>
              z.findMap { case (k, _) => (k === r._1) option r } | z.end.insert(r)
            }.toList
          })))

        case ConcatMaps(_, _) => mapF

        case DeleteKey(
          Embed(CoEnv(\/-(ExtEJson(ejson.Map(kvs))))),
          k @ Embed(CoEnv(\/-(_)))) =>

          Free.roll(ExtEJson(ejson.Map(kvs.filterNot(_._1 === k))))

        case DeleteKey(_, _) => mapF

        case ProjectKey(
          Embed(CoEnv(\/-(ExtEJson(ejson.Map(kvs))))),
          k @ Embed(CoEnv(\/-(_)))) =>

          kvs.find(_._1 === k).cata(_._2, undefinedF)

        // Arrays
        case MakeArray(v) =>
          Free.roll(CommonEJson(ejson.Arr(List(v))))

        case ConcatArrays(
          Embed(CoEnv(\/-(CommonEJson(ejson.Arr(ls))))),
          Embed(CoEnv(\/-(CommonEJson(ejson.Arr(rs)))))) =>

          Free.roll(CommonEJson(ejson.Arr(ls ::: rs)))

        case ConcatArrays(_, _) => arrF

        case ProjectIndex(
          Embed(CoEnv(\/-(CommonEJson(ejson.Arr(xs))))),
          Embed(CoEnv(\/-(ExtEJson(ejson.Int(i0)))))) =>

          some(i0)
            .filter(i => (i >= 0) && i.isValidInt)
            .flatMap(i => xs.index(i.intValue))
            .getOrElse(undefinedF)

        case _ => unknownF
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def qScriptCore[T[_[_]]: BirecursiveT]: Outline[QScriptCore[T, ?]] =
    new Outline[QScriptCore[T, ?]] {
      val outlineƒ: Algebra[QScriptCore[T, ?], Shape] = {
        case Map(inShape, fm) =>
          outlineF(fm.linearize)(κ(inShape))

        case LeftShift(inShape, _, idStatus, _, _, repair) =>
          outlineF(repair) {
            case LeftSide => inShape
            case RightSide => idStatus match {
              case IncludeId =>
                Free.roll(CommonEJson(ejson.Arr(List(
                  unknownF,
                  unknownF))))
              case _ => unknownF
            }
          }

        case Reduce(inShape, buckets, reducers, repair) =>
          import ReduceFuncs._

          val bucketShapes = buckets map (b => outlineF(b)(κ(inShape)))

          // TODO: Improve this if we're ever able to use TypeF
          val reducerShapes = reducers map {
            case Count(_)         => unknownF
            case Sum(_)           => unknownF
            case Min(_)           => unknownF
            case Max(_)           => unknownF
            case Avg(_)           => unknownF
            case Arbitrary(fm)    => outlineF(fm)(κ(inShape))
            case First(fm)        => outlineF(fm)(κ(inShape))
            case Last(fm)         => outlineF(fm)(κ(inShape))
            case UnshiftArray(_)  => arrF
            case UnshiftMap(_, _) => mapF
          }

          outlineF(repair)(_.idx.fold(bucketShapes, reducerShapes))

        case Subset(srcShape, from, _, _) =>
          outlineF(from)(κ(srcShape))

        // TODO: Current representation cannot express this.
        case Union(_, _, _) => unknownF

        case Sort(srcShape, _, _) => srcShape
        case Filter(srcShape, _) => srcShape

        case Unreferenced() => undefinedF
      }
    }

  implicit def thetaJoin[T[_[_]]: BirecursiveT]: Outline[ThetaJoin[T, ?]] =
    new Outline[ThetaJoin[T, ?]] {
      val outlineƒ: Algebra[ThetaJoin[T, ?], Shape] = {
        case ThetaJoin(inShape, l, r, _, _, combine) =>
          outlineBranches(inShape, l, r, combine)
      }
    }

  implicit def equiJoin[T[_[_]]: BirecursiveT]: Outline[EquiJoin[T, ?]] =
    new Outline[EquiJoin[T, ?]] {
      val outlineƒ: Algebra[EquiJoin[T, ?], Shape] = {
        case EquiJoin(inShape, l, r, _, _, combine) =>
          outlineBranches(inShape, l, r, combine)
      }
    }

  // Unknown

  implicit def mapFuncDerived[T[_[_]]]: Outline[MapFuncDerived[T, ?]] = unknownInstance
  implicit def projectBucket[T[_[_]]]: Outline[ProjectBucket[T, ?]] = unknownInstance
  implicit def constRead[A]: Outline[Const[Read[A], ?]] = unknownInstance
  implicit def constShiftedRead[A]: Outline[Const[ShiftedRead[A], ?]] = unknownInstance
  implicit def constDeadEnd: Outline[Const[DeadEnd, ?]] = unknownInstance

  ////

  private def outlineBranches[T[_[_]]: BirecursiveT](
      srcShape: Shape,
      lBranch: FreeQS[T],
      rBranch: FreeQS[T],
      combine: JoinFunc[T])
      : Shape = {

    val lShape = outlineF(lBranch)(κ(srcShape))
    val rShape = outlineF(rBranch)(κ(srcShape))

    outlineF(combine) {
      case LeftSide  => lShape
      case RightSide => rShape
    }
  }

  private def unknownInstance[F[_]]: Outline[F] =
    new Outline[F] {
      val outlineƒ: Algebra[F, Shape] =
        κ(unknownF)
    }
}
