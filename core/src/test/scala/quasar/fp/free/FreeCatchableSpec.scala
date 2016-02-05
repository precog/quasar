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
          effm.foldMap(interpret2[Task, CounterF, Task](
            NaturalTransformation.refl,
            Coyoneda.liftTF(runCounter(ref))))
        ).run
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

    "catch ambient exceptions in map" >> {
      runEff(catchable.attempt(
        next[Eff].map(n => throw IntError(7))
      )).toEither must beLeft(IntError(7))
    }

    "catch ambient exceptions in flatMap" >> {
      runEff(catchable.attempt(for {
        i <- next[Eff]
        j <- next[Eff]
        k <- if ((i + j) > 0) throw IntError(23) else next[Eff]
      } yield k)).toEither must beLeft(IntError(23))
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
