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

package quasar.yggdrasil.table

import quasar.yggdrasil._
import quasar.yggdrasil.bytecode.JType
import quasar.precog.common._
import quasar.precog.common.security._
import quasar.yggdrasil.vfs._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global // FIXME what is this thing

import akka.pattern.AskSupport

import scalaz._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import org.slf4s.Logging

import TableModule._

trait VFSColumnarTableModule extends BlockStoreColumnarTableModule[Future] with SecureVFSModule[Future, Slice] with AskSupport with Logging {
  def vfs: SecureVFS

  trait VFSColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, apiKey: APIKey, tpe: JType): EitherT[Future, ResourceError, Table] = {
      for {
        _ <- EitherT.right(table.toJson map { json => log.trace("Starting load from " + json.toList.map(_.renderCompact)) })
        paths <- EitherT.right(pathsM(table))
        projections <- paths.toList.traverse[({ type l[a] = EitherT[Future, ResourceError, a] })#l, Projection] { path =>
          log.debug("Loading path: " + path)
          vfs.readProjection(apiKey, path, Version.Current, AccessMode.Read) leftMap { error =>
            log.warn("An error was encountered in loading path %s: %s".format(path, error))
            error
          }
        }
      } yield {
        val length = projections.map(_.length).sum
        val stream = projections.foldLeft(StreamT.empty[Future, Slice]) { (acc, proj) =>
          // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
          val constraints = proj.structure.map { struct =>
            Some(Schema.flatten(tpe, struct.toList))
          }

          log.debug("Appending from projection: " + proj)
          acc ++ StreamT.wrapEffect(constraints map { c => proj.getBlockStream(c) })
        }

        Table(stream, ExactSize(length))
      }
    }
  }
}
