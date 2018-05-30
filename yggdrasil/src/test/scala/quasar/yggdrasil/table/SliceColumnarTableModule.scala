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

package quasar.yggdrasil.table

import quasar.precog.common._
import quasar.yggdrasil._
import quasar.yggdrasil.bytecode._

import cats.effect.IO
import scalaz._, Scalaz._
import shims._

//FIXME: This is only used in test at this point, kill with fire in favor of VFSColumnarTableModule
trait SliceColumnarTableModule extends BlockStoreColumnarTableModule with ProjectionModule[Slice] {
  type TableCompanion <: SliceColumnarTableCompanion

  trait SliceColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, tpe: JType): EitherT[IO, vfs.ResourceError, Table] = EitherT.rightT {
      for {
        paths <- pathsM(table)
        projections <- paths.toList.traverse(Projection(_)).map(_.flatten)
        totalLength = projections.map(_.length).sum
      } yield {
        def slices(proj: Projection, constraints: Option[Set[ColumnRef]]): StreamT[IO, Slice] = {
          StreamT.unfoldM[IO, Slice, Option[proj.Key]](None) { key =>
            proj.getBlockAfter(key, constraints).map { b =>
              b.map {
                case BlockProjectionData(_, maxKey, slice) =>
                  (slice, Some(maxKey))
              }
            }
          }
        }

        val stream = projections.foldLeft(StreamT.empty[IO, Slice]) { (acc, proj) =>
          // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
          val constraints: IO[Option[Set[ColumnRef]]] =
            IO.pure(Some(Schema.flatten(tpe, proj.structure.toList).toSet))

          acc ++ StreamT.wrapEffect(constraints map { c =>
            slices(proj, c)
          })
        }

        Table(stream, ExactSize(totalLength))
      }
    }
  }
}
