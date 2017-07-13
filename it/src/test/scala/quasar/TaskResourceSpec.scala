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

package quasar

import slamdata.Predef._
import quasar.fp.TaskRef

import scala.concurrent.duration._
import scalaz._, Scalaz._
import scalaz.concurrent.{Task, Strategy}

class TaskResourceSpec extends QuasarSpecification {

  "TaskResource" should {
    def loggingRsrc(ref: TaskRef[List[String]]): Task[TaskResource[Int]] =
      TaskResource(
        ref.modify(_ :+ "acq").as(0), Strategy.DefaultStrategy)(
        _ => ref.modify(_ :+ "rel").void)

    val failingRsrc: Task[TaskResource[Int]] =
      TaskResource(
        Task.fail(new RuntimeException("simulated failure")), Strategy.DefaultStrategy)(
        _ => Task.now(()))

    "acquire once and release once" in {
      (for {
        ref  <- TaskRef(List[String]())
        rsrc <- loggingRsrc(ref)

        _    <- rsrc.get
        _    <- rsrc.release

        log  <- ref.read
      } yield {
        log must_=== List("acq", "rel")
      }).timed(1000.milliseconds).unsafePerformSync
    }

    "acquire once and release once with multiple concurrent requests" in {
      (for {
        ref  <- TaskRef(List[String]())
        rsrc <- loggingRsrc(ref)

        get  =  rsrc.get
        rel  =  rsrc.release

        _ <- Nondeterminism[Task].gatherUnordered(List.fill(10)(get))

        _ <- Nondeterminism[Task].gatherUnordered(List.fill(7)(rel))

        log  <- ref.read
      } yield {
        log must_=== List("acq", "rel")
      }).timed(1000.milliseconds).unsafePerformSync
    }

    "fail immediately when acquire fails" in {
      (for {
        rsrc <- failingRsrc

        rez  <- rsrc.get.attempt.map(_.swap.as(()))
      } yield {
        rez must beRightDisjunction
      }).timed(1000.milliseconds).unsafePerformSync
    }


    "fail multiple requests immediately when first acquire fails" in {
      (for {
        rsrc <- failingRsrc

        acq  =  rsrc.get.attempt.map(_.swap.as(()))
        rez  <- Nondeterminism[Task].gatherUnordered(List.fill(10)(acq))
      } yield {
        rez must_=== List.fill(10)(\/-(()))
      }).timed(1000.milliseconds).unsafePerformSync
    }
  }
}
