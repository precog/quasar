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

package quasar.impl.table

import slamdata.Predef._

import cats.effect.IO

import fs2.Stream
import fs2.concurrent.SignallingRef

import org.specs2.mutable._

import quasar.Condition
import quasar.api.table.PreparationEvent

import scalaz.std.anyVal._
import scalaz.std.string._
import scalaz.syntax.monad._

import shims._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object PreparationsManagerSpec extends Specification {
  import PreparationsManager._

  implicit val cs = IO.contextShift(global)

  "preparations manager" should {
    "prepare a table upon request" in {
      val Id = "table-id"
      val Query = "select * from foo"

      var evaluated = List[String]()
      var stored = List[(String, Int)]()

      val results = for {
        manager <- PreparationsManager[IO, String, String, Int](
          { q =>
            IO {
              evaluated ::= q
              q.length
            }
          })((id, result) => IO.pure(Stream.eval(IO(stored ::= ((id, result))))))

        cond <- Stream.eval(manager.prepareTable(Id, Query))
        _ <- Stream.eval(IO(cond mustEqual Condition.normal()))

        event <- rethrowInStream(manager.notifications).take(1)

        _ <- Stream.eval(IO {
          event must beLike {
            case PreparationEvent.PreparationSucceeded(id, _, duration) =>
              duration.unit mustEqual SECONDS
              id mustEqual Id
          }

          evaluated mustEqual List(Query)
          stored mustEqual List((Id, Query.length))
        })
      } yield ()

      results.compile.drain.unsafeRunSync

      ok
    }

    "return Status.Started during evaluation" in {
      val Id = "table-id"

      val results = for {
        a <- Stream.eval(SignallingRef[IO, Boolean](false))
        b <- Stream.eval(SignallingRef[IO, Boolean](false))

        manager <- PreparationsManager[IO, String, String, Unit](
          _ => IO.pure(()))(
          (_, _) => IO.pure(Stream.eval(a.set(true) *> latchGet(b))))

        _ <- Stream.eval(manager.prepareTable(Id, "fubar"))

        _ <- Stream.eval(latchGet(a))
        status <- Stream.eval(manager.preparationStatus(Id))
        // we never actually need to release the latch :-P
      } yield status

      val status = results.compile.last.unsafeRunSync

      status must beLike {
        case Some(Status.Started(_)) => ok
      }
    }

    "return Status.Pending before evaluation" in {
      val Id = "table-id"

      val results = for {
        manager <- PreparationsManager[IO, String, PreparationsManager[IO, String, _, Unit], Unit](
          { manager =>    // is this not clever? (actually it's probably really stupid; feel free to say so)
            for {
              status <- manager.preparationStatus(Id)
              // we do the assertion here because it's during the setup but before eval
              _ <- IO(status mustEqual Status.Pending)
            } yield ()
          })(
          (_, _) => IO.pure(Stream.empty))

        _ <- Stream.eval(manager.prepareTable(Id, manager))   // tie the knot on the fixedpoint
      } yield ()

      results.compile.drain.unsafeRunSync

      ok
    }

    "return Status.Unknown by default" in {
      val results = for {
        manager <- PreparationsManager[IO, Unit, Unit, Unit](
          _ => IO.pure(()))(
          (_, _) => IO.pure(Stream.empty))

        status <- Stream.eval(manager.preparationStatus(()))
      } yield status

      val status = results.compile.last.unsafeRunSync

      status must beSome(Status.Unknown)
    }

    "report preparation error" in {
      case object TestException extends Exception

      val results = for {
        manager <- PreparationsManager[IO, Unit, Unit, Unit](
          _ => IO.pure(()))(
          (_, _) => IO.pure(Stream.raiseError[IO](TestException)))

        _ <- Stream.eval(manager.prepareTable((), ()))
        event <- manager.notifications.take(1)    // note: this doesn't catch repeated events
      } yield event

      val event = results.compile.last.unsafeRunSync

      event must beLike {
        case Some(PreparationEvent.PreparationErrored(_, _, duration, TestException)) =>
          duration.unit mustEqual SECONDS
      }
    }

    "suppress success reports on error" in {
      case object TestException extends Exception

      val results = for {
        a <- Stream.eval(SignallingRef[IO, Boolean](false))

        manager <- PreparationsManager[IO, Unit, Unit, Unit](
          _ => IO.pure(()))(
          (_, _) => IO.pure(Stream.raiseError[IO](TestException).onComplete(Stream.eval_(a.set(true)))))

        _ <- Stream.eval(manager.prepareTable((), ()))
        _ <- Stream.eval(latchGet(a))
      } yield manager.notifications

      // terminating the outer stream will shutdown the queue...
      results.compile.last.unsafeRunSync must beLike {
        case Some(notifications) =>
          // ...so we can safely run the inner stream to completion
          val events = notifications.compile.toVector.unsafeRunSync

          // race condition alert! we might beat the error enqueue. we have other tests for that though
          (events must haveSize(1)) or (events must beEmpty)

        case None => ko
      }
    }

    "reject redundant preparation when pending" in {
      val Id = "table-id"

      val results = for {
        a <- Stream.eval(SignallingRef[IO, Boolean](false))

        manager <- PreparationsManager[IO, String, PreparationsManager[IO, String, _, Unit], Unit](
          { manager =>    // is this not clever? (actually it's probably really stupid; feel free to say so)
            for {
              status <- manager.preparationStatus(Id)
              // we do the assertion here because it's during the setup but before eval
              _ <- IO(status mustEqual Status.Pending)
            } yield ()
          })(
          (_, _) => IO.pure(Stream.eval(latchGet(a))))

        status1 <- Stream.eval(manager.prepareTable(Id, manager))   // tie the knot on the fixedpoint
        status2 <- Stream.eval(manager.prepareTable(Id, manager))

        _ <- Stream.eval(IO {
          status1 mustEqual Condition.normal()
          status2 mustEqual Condition.abnormal(InProgressError(Id))
        })
      } yield ()

      results.compile.drain.unsafeRunSync

      ok
    }

    "reject redundant preparation when started" in {
      val Id = "table-id"

      val results = for {
        a <- Stream.eval(SignallingRef[IO, Boolean](false))
        b <- Stream.eval(SignallingRef[IO, Boolean](false))

        manager <- PreparationsManager[IO, String, String, Unit](
          _ => IO.pure(()))(
          (_, _) => IO.pure(Stream.eval(a.set(true) *> latchGet(b))))

        status1 <- Stream.eval(manager.prepareTable(Id, "fubar"))
        _ <- Stream.eval(latchGet(a))
        status2 <- Stream.eval(manager.prepareTable(Id, "fubar"))

        _ <- Stream.eval(IO {
          status1 mustEqual Condition.normal()
          status2 mustEqual Condition.abnormal(InProgressError(Id))
        })
      } yield ()

      results.compile.drain.unsafeRunSync

      ok
    }

    "cancel preparation when pending" in {
      val Id1 = "identity-1"
      val Id2 = "identity-2"

      val results = for {
        a <- Stream.eval(SignallingRef[IO, Boolean](false))

        manager <- PreparationsManager[IO, String, Unit, Unit](
          _ => IO.pure(()),
          maxConcurrency = 1)(    // we force sequential evaluation of preparations
          (_, _) => IO.pure(Stream.eval(IO.never)))

        // enqueue the first table, which will never finish preparing
        _ <- Stream.eval(manager.prepareTable(Id1, ()))

        // enqueue the second, which we will test
        _ <- Stream.eval(manager.prepareTable(Id2, ()))
        initStatus <- Stream.eval(manager.preparationStatus(Id2))
        _ <- Stream.eval(IO(initStatus mustEqual Status.Pending))

        status1 <- Stream.eval(manager.cancelPreparation(Id2))
        _ <- Stream.eval(IO(status1 mustEqual Condition.normal()))

        status2 <- Stream.eval(manager.preparationStatus(Id2))
        _ <- Stream.eval(IO(status2 mustEqual Status.Unknown))
      } yield ()

      results.compile.drain.unsafeRunTimed(5.seconds) must beSome
    }

    "cancel preparation when started" in {
      val Id = "table-id"

      val results = for {
        a <- Stream.eval(SignallingRef[IO, Boolean](false))
        b <- Stream.eval(SignallingRef[IO, Boolean](false))

        manager <- PreparationsManager[IO, String, String, Unit](
          _ => IO.pure(()))(
          (_, _) => IO.pure(Stream.eval(a.set(true) *> latchGet(b))))

        _ <- Stream.eval(manager.prepareTable(Id, "fubar"))
        _ <- Stream.eval(latchGet(a))

        status1 <- Stream.eval(manager.cancelPreparation(Id))
        _ <- Stream.eval(IO(status1 mustEqual Condition.normal()))

        status2 <- Stream.eval(manager.preparationStatus(Id))
        _ <- Stream.eval(IO(status2 mustEqual Status.Unknown))
      } yield ()

      results.compile.drain.unsafeRunSync

      ok
    }

    "fail to cancel unknown preparations" in {
      val results = for {
        manager <- PreparationsManager[IO, Unit, Unit, Unit](
          _ => IO.pure(()))(
          (_, _) => IO.pure(Stream.empty))

        status <- Stream.eval(manager.cancelPreparation(()))
      } yield status

      val status = results.compile.last.unsafeRunSync

      status must beSome(Condition.abnormal(NotInProgressError(())))
    }

    "cancel all preparations" in {
      val Id1 = "table-id-1"
      val Id2 = "table-id-2"

      val results = for {
        a <- Stream.eval(SignallingRef[IO, Boolean](false))

        manager <- PreparationsManager[IO, String, Unit, Unit](
          _ => IO.pure(()))(
          (_, _) => IO.pure(Stream.eval(latchGet(a))))

        status1 <- Stream.eval(manager.prepareTable(Id1, ()))
        status2 <- Stream.eval(manager.prepareTable(Id2, ()))

        _ <- Stream.eval(IO {
          status1 mustEqual Condition.normal()
          status2 mustEqual Condition.normal()
        })

        _ <- Stream.eval(manager.cancelAll)

        status1 <- Stream.eval(manager.preparationStatus(Id1))
        status2 <- Stream.eval(manager.preparationStatus(Id2))

        _ <- Stream.eval(IO {
          status1 mustEqual Status.Unknown
          status2 mustEqual Status.Unknown
        })
      } yield ()

      results.compile.drain.unsafeRunSync

      ok
    }

    "immediately return when evaluation takes forever" in {
      val results = for {
        manager <- PreparationsManager[IO, Unit, Unit, Unit](
          _ => IO.never)(
          (_, _) => IO.pure(Stream.empty))

        _ <- Stream.eval(manager.prepareTable((), ()))
        _ <- Stream.eval(manager.cancelPreparation(()))

        status <- Stream.eval(manager.preparationStatus(()))
        _ <- Stream.eval(IO(status mustEqual Status.Unknown))
      } yield ()

      results.compile.drain.unsafeRunTimed(1.second) must beSome
    }

    "report error in evaluating query" in {
      case object EvalException extends Exception

      val results = for {
        manager <- PreparationsManager[IO, Unit, Unit, Unit](
          _ => IO.raiseError(EvalException))(
          (_, _) => IO.pure(Stream.empty))

        _ <- Stream.eval(manager.prepareTable((), ()))

        event <- manager.notifications.take(1)
        _ <- Stream.eval(IO {
          event must beLike {
            case PreparationEvent.PreparationErrored((), _, _, EvalException) => ok
          }
        })
      } yield ()

      results.compile.drain.unsafeRunTimed(1.second) must beSome
    }
  }

  def latchGet(s: SignallingRef[IO, Boolean]): IO[Unit] =
    s.discrete.filter(_ == true).take(1).compile.drain

  def rethrowInStream(
      events: Stream[IO, PreparationEvent[String]])
      : Stream[IO, PreparationEvent[String]] = {

    events flatMap {
      case PreparationEvent.PreparationErrored(_, _, _, t) =>
        Stream.raiseError[IO](t)

      case event =>
        Stream(event)
    }
  }
}
