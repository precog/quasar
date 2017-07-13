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

package quasar.yggdrasil.execution

import quasar.yggdrasil.TableModule
import quasar.yggdrasil.vfs._

import quasar.precog.common._

import quasar.precog.{MimeType, MimeTypes}

import scala.concurrent.duration.Duration

import scalaz._
import scalaz.NonEmptyList.nels

sealed trait EvaluationError
case class InvalidStateError(message: String) extends EvaluationError
case class StorageError(error: ResourceError) extends EvaluationError
case class SystemError(error: Throwable) extends EvaluationError
case class AccumulatedErrors(errors: NonEmptyList[EvaluationError]) extends EvaluationError

object EvaluationError {
  def invalidState(message: String): EvaluationError = InvalidStateError(message)
  def storageError(error: ResourceError): EvaluationError = StorageError(error)
  def systemError(error: Throwable): EvaluationError = SystemError(error)
  def acc(errors: NonEmptyList[EvaluationError]): EvaluationError = AccumulatedErrors(errors)

  implicit val semigroup: Semigroup[EvaluationError] = new Semigroup[EvaluationError] {
    def append(a: EvaluationError, b: => EvaluationError) = (a, b) match {
      case (AccumulatedErrors(a0), AccumulatedErrors(b0)) => AccumulatedErrors(a0 append b0)
      case (a0, AccumulatedErrors(b0)) => AccumulatedErrors(a0 <:: b0)
      case (AccumulatedErrors(a0), b0) => AccumulatedErrors(b0 <:: a0)
      case (a0, b0) => AccumulatedErrors(nels(a0, b0))
    }
  }
}

case class QueryOptions(
  page: Option[(Long, Long)] = None,
  sortOn: List[CPath] = Nil,
  sortOrder: TableModule.DesiredSortOrder = TableModule.SortAscending,
  timeout: Option[Duration] = None,
  output: MimeType = MimeTypes.application/MimeTypes.json,
  cacheControl: CacheControl = CacheControl.NoCache)

case class CacheControl(maxAge: Option[Long], recacheAfter: Option[Long], cacheable: Boolean, onlyIfCached: Boolean)

object CacheControl {
  import quasar.blueeyes.CacheDirective
  import quasar.blueeyes.CacheDirectives.{ `max-age`, `no-cache`, `only-if-cached`, `max-stale` }
  import scalaz.syntax.semigroup._
  import scalaz.std.option._
  import scalaz.std.anyVal._

  val NoCache = CacheControl(None, None, false, false)

  def fromCacheDirectives(cacheDirectives: CacheDirective*) = {
    val maxAge = cacheDirectives.collectFirst { case `max-age`(Some(n)) => n.number * 1000 }
    val maxStale = cacheDirectives.collectFirst { case `max-stale`(Some(n)) => n.number * 1000 }
    val cacheable = cacheDirectives exists { _ != `no-cache`}
    val onlyIfCached = cacheDirectives exists { _ == `only-if-cached`}
    CacheControl(maxAge |+| maxStale, maxAge, cacheable, onlyIfCached)
  }
}

trait QueryExecutor[M[+_], A] { self =>
  def execute(query: String, context: EvaluationContext, opts: QueryOptions): EitherT[M, EvaluationError, A]

  def map[B](f: A => B)(implicit M: Functor[M]): QueryExecutor[M, B] = new QueryExecutor[M, B] {
    def execute(query: String, context: EvaluationContext, opts: QueryOptions): EitherT[M, EvaluationError, B] = {
      self.execute(query, context, opts) map f
    }
  }
}
