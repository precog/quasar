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

package quasar.physical.marklogic

import slamdata.Predef._
import quasar.Data
import quasar.connector.EnvironmentError
import quasar.contrib.scalaz.eitherT._
import quasar.effect._
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.fs.mount._

import com.marklogic.xcc._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object testing {
  import xcc._, xquery._, fs._

  def multiFormatDef(
    uri: ConnectionUri,
    readChunkSize: Positive,
    writeChunkSize: Positive
  ): Task[(BackendEffect ~> Task, BackendEffect ~> Task, Task[Unit])] = {
    def failOnError[A](err: BackendDef.DefinitionError): Task[A] =
      err.fold[Task[A]](
        errs => Task.fail(new RuntimeException(errs intercalate ", ")),
        ee   => Task.fail(new RuntimeException(ee.shows)))

    val defn = MarkLogic(readChunkSize, writeChunkSize).definition

    MarkLogicConfig.fromUriString[EitherT[Task, ErrorMessages, ?]](uri.value)
      .leftMap(_.left[EnvironmentError])
      .flatMap { cfg =>
        val js = defn(FsType, ConnectionUri(cfg.copy(docType = DocType.json).asUriString))
        val xml = defn(FsType, ConnectionUri(cfg.copy(docType = DocType.xml).asUriString))

        js.tuple(xml) map {
          case (jsdr, xmldr) => (jsdr.run, xmldr.run, jsdr.close *> xmldr.close)
        }
      } foldM (failOnError, _.point[Task])
  }

  /** Returns the results, as `Data`, of evaluating the module or `None` if
    * evaluation succeded without producing any results.
    */
  def moduleResults[F[_]: Monad: Capture: Catchable: CSourceReader](main: MainModule): F[ErrorMessages \/ Option[Data]] =
    contentsource.defaultSession[F] >>= Xcc[ReaderT[F, Session, ?]].results(main) map { items =>
      items.headOption traverse xdmitem.toData[ErrorMessages \/ ?] _
    }
}
