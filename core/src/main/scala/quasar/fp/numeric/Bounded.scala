package quasar.fp.numeric

import quasar.Predef._

trait Bounded[T] {
  def minValue:T
  def maxValue:T
}

object Bounded {
  def apply[T](min: T, max:T): Bounded[T] = new Bounded[T] {
    val minValue = min
    val maxValue = max
  }
  implicit val int: Bounded[Int] = Bounded(Int.MinValue,Int.MaxValue)
  implicit val short: Bounded[Short] = Bounded(Short.MinValue, Short.MaxValue)
  implicit val long: Bounded[Long] = Bounded(Long.MinValue, Long.MaxValue)
}
