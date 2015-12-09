package quasar.fp

import quasar.Predef._

import shapeless.tag.@@
import eu.timepit.refined.numeric.{NonNegative, Positive => RPositive, Negative}
import eu.timepit.refined.refineT
import scalaz.{Equal, Show}

package object numeric {

  type Natural = Long @@ NonNegative
  type Positive = Long @@ RPositive

  def Positive(a: Long): Option[Positive] = refineT[RPositive](a).right.toOption
  def Natural(a: Long): Option[Natural] = refineT[NonNegative](a).right.toOption

  def add[T:scala.Numeric](a:T @@ NonNegative, b: T @@ NonNegative): T @@ NonNegative =
    refineT[NonNegative].force(implicitly[scala.Numeric[T]].plus(a,b))

  implicit def equalNonNegative[T]: Equal[T @@ NonNegative] = Equal.equalA[T @@ NonNegative]

  implicit def showNonNegative[T]: Show[T @@ NonNegative] = Show.showA[T @@ NonNegative]

  implicit def equalPositive[T]: Equal[T @@ RPositive] = Equal.equalA[T @@ RPositive]

  implicit def showPositive[T]: Show[T @@ RPositive] = Show.showA[T @@ RPositive]

  implicit def equalNegative[T]: Equal[T @@ Negative] = Equal.equalA[T @@ Negative]

  implicit def showNegative[T]: Show[T @@ Negative] = Show.showA[T @@ Negative]
}
