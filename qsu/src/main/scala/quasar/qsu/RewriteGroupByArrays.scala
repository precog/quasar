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

package quasar.qsu

import quasar.ejson
import quasar.common.effect.NameGenerator
import quasar.qscript.{SrcHole, LeftSide, MapFuncsCore, MFC, RightSide}
import quasar.qsu.{QScriptUniform => QSU}
import slamdata.Predef._

import matryoshka.{BirecursiveT, ShowT}
import scalaz.{Monad, Scalaz, StateT}, Scalaz._

final class RewriteGroupByArrays[T[_[_]]: BirecursiveT: ShowT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  val json = ejson.Fixed[T[ejson.EJson]]

  // recognize the pattern generated in LP of squishing groupbys together
  def apply[F[_]: Monad: NameGenerator](qgraph: QSUGraph): F[QSUGraph] = {
    type G[A] = StateT[F, RevIdx, A]

    val back = qgraph rewriteM {
      case qgraph @ GroupBy(target, NAryArray(keys @ _*)) =>
        val nestedM = keys.toList.foldLeftM(target) { (inner, key) =>
          // this is a bizarre rewrite, because it generates invalid graphs along the way
          // this happens because inner and target don't necessarily exist in key's vertices
          for {
            key2 <- key.replaceWithRename[G](target.root, inner.root)
            replaced <- QSUGraph.withName[T, G]("rga")(QSU.GroupBy[T, Symbol](inner.root, key2.root))
          } yield replaced :++ inner :++ key2
        }

        nestedM.map(nested => qgraph.overwriteAtRoot(nested.unfold.map(_.root)) :++ nested)
    }

    back.eval(qgraph.generateRevIndex)
  }

  object NAryArray {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def unapplySeq(qgraph: QSUGraph): Option[Vector[QSUGraph]] = qgraph match {
      // TODO: the commuted cases should be just as important,
      //       we're just ignoring them for now because this is
      //       how LP always associates flattened group keys
      case AutoJoin2C(
        NAryArray(elems @ _*),
        Unary(last, MFC(MapFuncsCore.MakeArray(SrcHole))),
        MapFuncsCore.ConcatArrays(LeftSide, RightSide)) => Some(elems.toVector :+ last)

      case AutoJoin2C(
        NAryArray(elems @ _*),
        c @ Unary(_, MFC(MapFuncsCore.Constant(json.arr(_)))),
        MapFuncsCore.ConcatArrays(LeftSide, RightSide)) => Some(elems.toVector :+ c)

      case Unary(last, MFC(MapFuncsCore.MakeArray(SrcHole))) => Some(Vector(last))
      case c @ Unary(_, MFC(MapFuncsCore.Constant(json.arr(_)))) => Some(Vector(c))

      case _ => None
    }
  }
}

object RewriteGroupByArrays {
  def apply[
      T[_[_]]: BirecursiveT: ShowT,
      F[_]: Monad: NameGenerator](
      qgraph: QSUGraph[T])
      : F[QSUGraph[T]] =
    new RewriteGroupByArrays[T].apply[F](qgraph)
}
