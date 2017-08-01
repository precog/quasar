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

package quasar.yggdrasil
package table

import quasar.blueeyes._, json._
import quasar.precog.common._
import quasar.precog.common.security.APIKey

import scalaz._, Scalaz._

import scala.annotation.tailrec

trait BlockStoreTestModule[M[+_]] extends BaseBlockStoreTestModule[M] {
  implicit def M: Monad[M] with Comonad[M]

  type GroupId = String
  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = "groupId(" + groupId.getAndIncrement + ")"

  trait TableCompanion extends BaseBlockStoreTestTableCompanion

  object Table extends TableCompanion
}

trait BaseBlockStoreTestModule[M[+_]] extends ColumnarTableModuleTestSupport[M]
    with SliceColumnarTableModule[M]
    with StubProjectionModule[M, Slice] {

  import trans._

  implicit def M: Monad[M] with Comonad[M]

  object Projection extends ProjectionCompanion

  case class Projection(data: Stream[JValue]) extends ProjectionLike[M, Slice] {
    type Key = JArray

    private val slices = fromJson(data).slices.toStream.copoint

    val length: Long = data.length
    val xyz = slices.foldLeft(Set.empty[ColumnRef]) {
      case (acc, slice) => acc ++ slice.columns.keySet
    }
    def structure(implicit M: Monad[M]) = M.point(xyz)

    def getBlockAfter(id: Option[JArray], colSelection: Option[Set[ColumnRef]])(implicit M: Monad[M]) = M.point {
      @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
        blocks.filterNot(_.isEmpty) match {
          case x #:: xs =>
            if ((x.toJson(x.size - 1).getOrElse(JUndefined) \ "key") > id) Some(x) else findBlockAfter(id, xs)

          case _ => None
        }
      }

      val slice = id map (findBlockAfter(_, slices)) getOrElse slices.headOption

      slice map { s =>
        val s0 = new Slice {
          val size = s.size
          val columns = colSelection.map { reqCols =>
            s.columns.filter {
              case (ref @ ColumnRef(jpath, ctype), _) =>
                jpath.nodes.head == CPathField("key") || reqCols.exists { ref =>
                  (CPathField("value") \ ref.selector) == jpath && ref.ctype == ctype
                }
            }
          }.getOrElse(s.columns)
        }

        BlockProjectionData[JArray, Slice](s0.toJson(0).getOrElse(JUndefined) \ "key" --> classOf[JArray], s0.toJson(s0.size - 1).getOrElse(JUndefined) \ "key" --> classOf[JArray], s0)
      }
    }
  }

  trait BaseBlockStoreTestTableCompanion extends SliceColumnarTableCompanion

  def userMetadataView(apiKey: APIKey) = sys.error("TODO")

  def compliesWithSchema(jv: JValue, ctype: CType): Boolean = (jv, ctype) match {
    case (_: JNum, CNum | CLong | CDouble) => true
    case (JUndefined, CUndefined)          => true
    case (JNull, CNull)                    => true
    case (_: JBool, CBoolean)              => true
    case (_: JString, CString)             => true
    case (JObject(fields), CEmptyObject)   => fields.isEmpty
    case (JArray(Nil), CEmptyArray)        => true
    case _                                 => false
  }

  def sortTransspec(sortKeys: CPath*): TransSpec1 = InnerObjectConcat(sortKeys.zipWithIndex.map {
    case (sortKey, idx) => WrapObject(
      sortKey.nodes.foldLeft[TransSpec1](DerefObjectStatic(Leaf(Source), CPathField("value"))) {
        case (innerSpec, field: CPathField) => DerefObjectStatic(innerSpec, field)
        case (innerSpec, index: CPathIndex) => DerefArrayStatic(innerSpec, index)
        case x                              => sys.error(s"Unexpected arg $x")
      },
      "%09d".format(idx)
    )
  }: _*)
}

object BlockStoreTestModule {
  def empty[M[+_]](implicit M0: Monad[M] with Comonad[M]) = new BlockStoreTestModule[M] {
    val M = M0
    val projections = Map.empty[Path, Projection]
  }
}
