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
import quasar.SKI._
import quasar.physical.marklogic.xquery.{MainModule, Version, XQuery}

import java.net.URI
import scala.collection.JavaConverters._

import com.marklogic.xcc.{Version => _, _}
import com.marklogic.xcc.exceptions.RequestException
import com.marklogic.xcc.types.XdmItem
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final class SessionIO[A] private (protected val k: Kleisli[Task, Session, A]) {
  def map[B](f: A => B): SessionIO[B] =
    new SessionIO(k map f)

  def flatMap[B](f: A => SessionIO[B]): SessionIO[B] =
    new SessionIO(k flatMap (a => f(a).k))

  def attempt: SessionIO[Throwable \/ A] =
    new SessionIO(k mapK (_.attempt))

  def run(s: Session): Task[A] =
    k.run(s)
}

object SessionIO {
  import Executed.executed

  def commit: SessionIO[Executed] =
    SessionIO(_.commit).as(executed)

  def connectionUri: OptionT[SessionIO, URI] =
    OptionT(SessionIO(s => Option(s.getConnectionUri)))

  def evaluateModule(main: MainModule, options: RequestOptions): SessionIO[QueryResults] =
    evaluateModule0(main, options) map (new QueryResults(_))

  def evaluateModule_(main: MainModule): SessionIO[QueryResults] =
    evaluateModule(main, new RequestOptions)

  def executeModule(main: MainModule, options: RequestOptions): SessionIO[Executed] = {
    options.setCacheResult(false)
    evaluateModule0(main, options)
      .flatMap(rs => liftT(Task.delay(rs.close())))
      .as(executed)
  }

  def executeModule_(main: MainModule): SessionIO[Executed] =
    executeModule(main, new RequestOptions)

  def evaluateQuery(query: XQuery, options: RequestOptions): SessionIO[QueryResults] =
    evaluateModule(defaultModule(query), options)

  def evaluateQuery_(query: XQuery): SessionIO[QueryResults] =
    evaluateQuery(query, new RequestOptions)

  def executeQuery(query: XQuery, options: RequestOptions): SessionIO[Executed] =
    executeModule(defaultModule(query), options)

  def executeQuery_(query: XQuery): SessionIO[Executed] =
    executeQuery(query, new RequestOptions)

  def insertContent[F[_]: Foldable](content: F[Content]): SessionIO[Executed] =
    SessionIO(_.insertContent(content.to[Array])).as(executed)

  def insertContentCollectErrors[F[_]: Foldable](content: F[Content]): SessionIO[List[RequestException]] =
    SessionIO(_.insertContentCollectErrors(content.to[Array]))
      .map(errs => Option(errs).toList flatMap (_.asScala.toList))

  def isClosed: SessionIO[Boolean] =
    SessionIO(_.isClosed)

  def resultsOf(query: XQuery, options: RequestOptions): SessionIO[ImmutableArray[XdmItem]] =
    evaluateQuery(query, options) >>= (qr => liftT(qr.toImmutableArray))

  def resultsOf_(query: XQuery): SessionIO[ImmutableArray[XdmItem]] =
    resultsOf(query, new RequestOptions)

  def rollback: SessionIO[Executed] =
    SessionIO(_.rollback).as(executed)

  def setTransactionMode(tm: Session.TransactionMode): SessionIO[Executed] =
    SessionIO(_.setTransactionMode(tm)).as(executed)

  def transactionMode: SessionIO[Session.TransactionMode] =
    SessionIO(_.getTransactionMode)

  def fail[A](t: Throwable): SessionIO[A] =
    liftT(Task.fail(t))

  val liftT: Task ~> SessionIO =
    new (Task ~> SessionIO) {
      def apply[A](ta: Task[A]) = lift(κ(ta))
    }

  implicit val sessionIOInstance: Monad[SessionIO] with Catchable[SessionIO] =
    new Monad[SessionIO] with Catchable[SessionIO] {
      override def map[A, B](fa: SessionIO[A])(f: A => B) = fa map f
      def point[A](a: => A) = liftT(Task.now(a))
      def bind[A, B](fa: SessionIO[A])(f: A => SessionIO[B]) = fa flatMap f
      def fail[A](t: Throwable) = SessionIO.fail(t)
      def attempt[A](fa: SessionIO[A]) = fa.attempt
    }

  ////

  private def apply[A](f: Session => A): SessionIO[A] =
    lift(s => Task.delay(f(s)))

  private def defaultModule(query: XQuery): MainModule =
    MainModule(Version.`1.0-ml`, ISet.empty, query)

  private def evaluateModule0(main: MainModule, options: RequestOptions): SessionIO[ResultSequence] =
    SessionIO(s => s.submitRequest(s.newAdhocQuery(main.render, options)))

  private def lift[A](f: Session => Task[A]): SessionIO[A] =
    new SessionIO(Kleisli(f))
}
