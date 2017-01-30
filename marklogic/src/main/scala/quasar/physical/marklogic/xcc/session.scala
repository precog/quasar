/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.xcc

import quasar.Predef._
import quasar.effect.Capture
import quasar.fp.eitherT._
import quasar.physical.marklogic.xquery.{MainModule, Version, XQuery}

import scala.collection.JavaConverters._
import java.math.BigInteger
import java.util.{List => JList}

import com.marklogic.xcc.{Version => _, _}
import com.marklogic.xcc.exceptions.{RequestException, XQueryException}
import com.marklogic.xcc.types.XdmItem
import scalaz._, Scalaz._

object session {
  import Executed.executed

  def currentServerPointInTime[F[_]: Monad: Capture: SessionReader: XccErr]: F[BigInt] =
    withSession[F, BigInteger](_.getCurrentServerPointInTime) map (BigInt(_))

  def evaluateModule[F[_]: Monad: Capture: SessionReader: XccErr](
    main: MainModule,
    options: RequestOptions
  ): F[QueryResults] =
    evaluateModule0[F](main, options) map (new QueryResults(_))

  def evaluateModule_[F[_]: Monad: Capture: SessionReader: XccErr](main: MainModule): F[QueryResults] =
    evaluateModule[F](main, new RequestOptions)

  def executeModule[F[_]: Monad: SessionReader: XccErr](
    main: MainModule,
    options: RequestOptions
  )(implicit C: Capture[F]): F[Executed] = {
    options.setCacheResult(false)
    evaluateModule0[F](main, options)
      .flatMap(rs => C.delay(rs.close()))
      .as(executed)
  }

  def executeModule_[F[_]: Monad: Capture: SessionReader: XccErr](main: MainModule): F[Executed] =
    executeModule[F](main, new RequestOptions)

  def evaluateQuery[F[_]: Monad: Capture: SessionReader: XccErr](query: XQuery, options: RequestOptions): F[QueryResults] =
    evaluateModule[F](defaultModule(query), options)

  def evaluateQuery_[F[_]: Monad: Capture: SessionReader: XccErr](query: XQuery): F[QueryResults] =
    evaluateQuery[F](query, new RequestOptions)

  def executeQuery[F[_]: Monad: Capture: SessionReader: XccErr](query: XQuery, options: RequestOptions): F[Executed] =
    executeModule[F](defaultModule(query), options)

  def executeQuery_[F[_]: Monad: Capture: SessionReader: XccErr](query: XQuery): F[Executed] =
    executeQuery[F](query, new RequestOptions)

  def insertContent[F[_]: Monad: Capture: SessionReader: XccErr, C[_]: Foldable](content: C[Content]): F[Executed] =
    withSession[F, Unit](_.insertContent(content.to[Array])).as(executed)

  def insertContentCollectErrors[F[_]: Monad: Capture: SessionReader, C[_]: Foldable](content: C[Content]): F[List[XccError]] =
    withSession[EitherT[F, XccError, ?], JList[RequestException]](_.insertContentCollectErrors(content.to[Array]))
      .fold(List(_), errs => Option(errs).toList flatMap (_.asScala.toList) map (XccError.requestError(_)))

  def resultsOf[F[_]: Monad: Capture: SessionReader: XccErr](query: XQuery, options: RequestOptions): F[ImmutableArray[XdmItem]] =
    evaluateQuery[F](query, options) >>= (_.toImmutableArray[F])

  def resultsOf_[F[_]: Monad: Capture: SessionReader: XccErr](query: XQuery): F[ImmutableArray[XdmItem]] =
    resultsOf[F](query, new RequestOptions)

  ////

  private def defaultModule(query: XQuery): MainModule =
    MainModule(Version.`1.0-ml`, ISet.empty, query)

  private def evaluateModule0[F[_]: Monad: Capture: SessionReader](
    main: MainModule,
    options: RequestOptions
  )(implicit E: XccErr[F]): F[ResultSequence] = {
    val xqy = main.render
    E.handleError(withSession[F, ResultSequence](s => s.submitRequest(s.newAdhocQuery(xqy, options)))) {
      case XccError.RequestError(ex: XQueryException) => E.raiseError(XccError.xqueryError(xqy, ex))
      case other                                      => E.raiseError(other)
    }
  }

  private def withSession[F[_]: Monad, A](f: Session => A)(implicit C: Capture[F], E: XccErr[F], R: SessionReader[F]): F[A] =
    R.ask >>= (s => C.delay(try {
      f(s).point[F]
    } catch {
      case ex: RequestException => E.raiseError[A](XccError.requestError(ex))
    }).join)
}
