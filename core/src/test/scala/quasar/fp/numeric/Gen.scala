package quasar.fp.numeric

import org.scalacheck.Gen.Choose
import org.scalacheck.{Arbitrary, Gen}
import quasar.Predef._
import scala.Numeric

import eu.timepit.refined.refineT
import eu.timepit.refined.numeric.{NonNegative, Positive => RPositive, Negative}
import shapeless.tag.@@

package object scalacheck {
  // TODO: Get rid of .get once we update versions of scalacheck
  implicit def positive[T:Choose](implicit num: Numeric[T]): Arbitrary[T @@ RPositive] =
    Arbitrary.apply(Gen.choose(num.fromInt(1), num.fromInt(Short.MaxValue)).map(short => refineT[RPositive](short).right.toOption.get))
  implicit def nonNegative[T:Choose](implicit num: Numeric[T]): Arbitrary[T @@ NonNegative] =
    Arbitrary.apply(Gen.choose(num.fromInt(0), num.fromInt(Short.MaxValue)).map(t => refineT[NonNegative](t).right.toOption.get))
  implicit def negative[T:Choose](implicit num: Numeric[T]): Arbitrary[T @@ Negative] =
    Arbitrary.apply(Gen.choose(num.fromInt(Int.MinValue), num.fromInt(-1)).map(refineT[Negative](_).right.toOption.get))
  // For some reason a Long @@ Negative as a proposition argument gets converted to (long, Negative) and the
  // arbTuple Arbitrary instance causes a diverging implicit resolution. These hardcoded versions seem to
  // sidestep the problem
  implicit val longPositive: Arbitrary[Positive] = positive[Long]
  implicit val longNegative: Arbitrary[Long @@ Negative] = negative[Long]
  implicit val natural:      Arbitrary[Natural] = nonNegative[Long]
}