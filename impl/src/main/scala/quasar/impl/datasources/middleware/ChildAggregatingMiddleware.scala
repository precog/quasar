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

package quasar.impl.datasources.middleware

import slamdata.Predef.{Map => SMap, _}
import quasar.{ScalarStages, ScalarStage}, ScalarStage._
import quasar.api.resource.ResourcePath
import quasar.api.table.ColumnType
import quasar.common.{CPath, CPathField}
import quasar.connector.{Datasource, MonadResourceErr}
import quasar.ejson.{EJson, Fixed}
import quasar.impl.datasource.{AggregateResult, ChildAggregatingDatasource}
import quasar.impl.datasources.ManagedDatasource
import quasar.qscript.{Hole, InterpretedRead, Map, QScriptEducated, RecFreeMap, construction}
import quasar.qscript.RecFreeS._

import scala.util.{Either, Left}

import cats.Monad
import cats.instances.list._
import cats.instances.option._
import cats.instances.string._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._
import fs2.Stream
import matryoshka.data.Fix
import pathy.Path.posixCodec
import shims._

// imports for implicit of type quasar.RenderTree[quasar.qscript.RecFreeMap[matryoshka.data.Fix]]
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.fp._
import matryoshka._
import matryoshka.data._


object ChildAggregatingMiddleware {
  def apply[T[_[_]], F[_]: Monad: MonadResourceErr, I, R](
      sourceKey: String,
      valueKey: String)(
      datasourceId: I,
      mds: ManagedDatasource[T, F, Stream[F, ?], R])
      : F[ManagedDatasource[T, F, Stream[F, ?], Either[R, AggregateResult[F, Map[Fix, R]]]]] =
    Monad[F].pure(mds) map {
      case ManagedDatasource.ManagedLightweight(lw) =>
        ManagedDatasource.lightweight[T](
          ChildAggregatingDatasource(lw)(
            _.path,
            rewriteInstructions(sourceKey, valueKey),
            Map[Fix, R](_, _)))

      // TODO: union all in QScript?
      case ManagedDatasource.ManagedHeavyweight(hw) =>
        type Q = T[QScriptEducated[T, ?]]
        ManagedDatasource.heavyweight(
          Datasource.pevaluator[F, Stream[F, ?], Q, R, Q, Either[R, AggregateResult[F, Map[Fix, R]]]]
            .modify(_.map(Left(_)))(hw))
    }

  ////

  private val rec = construction.RecFunc[Fix]
  private val ejs = Fixed[Fix[EJson]]

  def rewriteInstructions(
      sourceKey: String,
      valueKey: String)(
      ir: InterpretedRead[ResourcePath],
      cp: ResourcePath)
      : (InterpretedRead[ResourcePath], RecFreeMap[Fix]) = {

    val SrcField = CPath.parse("." + sourceKey)
    val ValField = CPath.parse("." + valueKey)

    val Undefined: (List[ScalarStage], Either[List[String], (Option[List[String]], Option[List[String]])]) =
      (Nil, Right((None, None)))

    val SourceFunc =
      rec.Constant[Hole](ejs.str(posixCodec.printPath(cp.toPath)))

    def reifyPath(path: List[String]): RecFreeMap[Fix] =
      path.foldRight(rec.Hole)(rec.MakeMapS)

    def reifyStructure(
        sourceLoc: Option[List[String]],
        valueLoc: Option[List[String]])
        : RecFreeMap[Fix] =
      (sourceLoc, valueLoc) match {
        case (Some(sloc), Some(vloc)) =>
          rec.ConcatMaps(reifyPath(sloc) >> SourceFunc, reifyPath(vloc))

        case (Some(sloc), None) => reifyPath(sloc) >> SourceFunc
        case (None, Some(vloc)) => reifyPath(vloc)
        case (None, None) => rec.Undefined
      }

    def injectSource(fields: List[String]): RecFreeMap[Fix] =
      fields.foldLeft(rec.Hole) { (obj, field) =>
        rec.ConcatMaps(obj, rec.MakeMapS(field, SourceFunc))
      }

    def cpath(l: List[String]) = CPath.parse(l.mkString(".", ".", ""))

    /** Returns the rewritten parse instructions and either what sourced-valued fields
      * to add to the output object or whether a new output object should be created
      * having one of, or both, of the source and output value.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def go(in: List[ScalarStage], sKey: Option[List[String]], vKey: Option[List[String]])
        : (List[ScalarStage], Either[List[String], (Option[List[String]], Option[List[String]])]) = {

      val spath: Option[CPath] = sKey.map(cpath)
      val vpath: Option[CPath] = vKey.map(cpath)

      in match {
        case Nil =>
          (Nil, Right((sKey, vKey)))
        case Project(p) :: t =>
          if (p === CPath.Identity)
            go(t, sKey, vKey)
          else if (p.some === spath)
            //project source
            //(t, Right((Some(Nil), None)))
            go(t, Some(Nil), None) //TODO recurse or not?
          else if (p.some === vpath)
            // project value
            (t, Right((None, Some(Nil))))
          else
            vpath.flatMap(p.dropPrefix(_)).fold(Undefined)(p0 => (Project(p0) :: t, Right((None, Some(Nil)))))

        case w @ Wrap(_) :: Nil =>
          (w, Right((sKey, vKey)))

        case m @ Mask(_) :: t if sKey === None && vKey === Some(Nil) =>
          (m, Right((sKey, vKey)))

        case m @ Mask(mask) :: t =>

          val exclude: Option[List[String]] = None
          val excludeMask: Option[SMap[CPath, Set[ColumnType]]] = None

          val (mask1, sloc, vloc) =
            mask.foldLeft((excludeMask, exclude, exclude)) {
              case ( acc @ (m, sp, vp), (k, v)) =>
                if (k.some === spath && v.contains(ColumnType.String))
                  (m, sKey, vp)
                else {
                  vpath.flatMap(k.dropPrefix(_)).fold(acc) { droppedK =>
                    val newM = m
                      .getOrElse(SMap[CPath, Set[ColumnType]]())
                      .updated(droppedK, v)
                    (Some(newM), sp, Some(Nil))
                  }
                }
            }
          go(mask1.fold(t)(Mask(_) :: t), sloc, vloc)

        case Cartesian(cs) :: Nil =>
          val init: (SMap[CPathField, (CPathField, List[Focused])], List[String]) = (SMap(), Nil)
          val (cart, sourceFields) = cs.foldLeft(init) {
            case ((m, srcs), entry@(out, (in, _))) =>
              if (in.name === sourceKey)
                (m, out.name :: srcs)
              else if (in.name === valueKey)
                (m + entry, srcs)
              else
                (m, srcs)
          }
          (cart.toList, sourceFields) match {
            case (Nil, Nil) => Undefined
            case (Nil, sfs) => (Nil, Right((Some(sfs), None)))
            case ((o, (_, f)) :: Nil, Nil) => go(Wrap(o.name) :: f, None, Some(Nil))
            case _ => ???
              //(List(Wrap(valueKey), Cartesian(cart)), Left(sourceFields))
          }
        case other =>
          ???
          //(other, Right((sKey, vKey)))
      }
    }

    ir.stages match {
      case rest =>
        val (out, struct) = go(rest.stages, Some(List(sourceKey)), Some(List(valueKey)))
        val structure = struct.fold(injectSource, (reifyStructure _).tupled)
        val ir = InterpretedRead(cp, ScalarStages(rest.idStatus, out))

        import quasar.RenderTree.ops._
        import scalaz.syntax.show._
        println(s"IR $ir")
        println("Struct")
        println(structure.render.show)
        (ir, structure)

    }
  }
}
