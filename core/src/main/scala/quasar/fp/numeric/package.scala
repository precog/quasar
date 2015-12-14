package quasar.fp

import quasar.Predef._

import shapeless.tag.@@
import eu.timepit.refined.numeric.{NonNegative, Positive => RPositive, Negative}
import eu.timepit.refined.refineT
import eu.timepit.refined.api.RefType
import scalaz.{Equal, Show}
import scalaz.syntax.show._

package object numeric {

  type Natural = Long @@ NonNegative
  type Positive = Long @@ RPositive

  def Positive(a: Long): Option[Positive] = refineT[RPositive](a).right.toOption
  def Natural(a: Long): Option[Natural] = refineT[NonNegative](a).right.toOption

  def add[T:scala.Numeric](a:T @@ NonNegative, b: T @@ NonNegative): T @@ NonNegative =
    refineT[NonNegative].force(implicitly[scala.Numeric[T]].plus(a,b))

  implicit def equal[F[_,_],T:Equal,M](implicit rt: RefType[F]): Equal[F[T,M]] = Equal.equalBy(rt.unwrap)

  implicit def show[F[_,_],T:Show,M](implicit rt: RefType[F]): Show[F[T,M]] = Show.shows(f => rt.unwrap(f).shows)
}
