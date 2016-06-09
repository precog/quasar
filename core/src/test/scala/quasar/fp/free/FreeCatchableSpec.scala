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

package quasar.fp.free

import quasar.Predef._
import quasar.fp.{injectFT, TaskRef}

import org.specs2.mutable
import scalaz._, Id.Id
import scalaz.syntax.id._
import scalaz.concurrent.Task

final class FreeCatchableSpec extends mutable.Specification {
  import FreeCatchableSpec._

  type Eff[A]  = Coproduct[Task, CounterF, A]
  type EffM[A] = Free[Eff, A]

  val runEff: EffM ~> Id =
    new (EffM ~> Id) {
      def apply[A](effm: EffM[A]) =
        TaskRef(0).flatMap(ref =>
          effm.foldMap(
            NaturalTransformation.refl[Task] :+:
            Coyoneda.liftTF(runCounter(ref)))
        ).unsafePerformSync
    }

  case class IntError(n: Int) extends scala.Exception

  val catchable = Catchable[EffM]

  "FreeCatchable" should {
    "surface any exceptions emitted via fail" >> {
      runEff(catchable.attempt(for {
        n <- next[Eff]
        _ <- catchable.fail[String](IntError(5))
      } yield n)).toEither must beLeft(IntError(5))
    }
  }
}

object FreeCatchableSpec {
  type CounterF[A] = Coyoneda[Counter, A]

  sealed trait Counter[A]
  final case object Next extends Counter[Int]

  def next[S[_]: Functor](implicit S: CounterF :<: S): Free[S, Int] =
    injectFT[CounterF, S].apply(Coyoneda.lift(Next))

  def runCounter(ref: TaskRef[Int]): Counter ~> Task =
    new (Counter ~> Task) {
      def apply[A](cnt: Counter[A]) = cnt match {
        case Next => ref.modifyS(n => (n + 1).squared)
      }
    }
}
