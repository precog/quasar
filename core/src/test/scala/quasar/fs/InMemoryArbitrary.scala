package quasar.fs

import quasar.Predef._
import quasar._
import quasar.DataArbitrary._

import InMemory.InMemState

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.{arbitrary => arb}
import pathy.scalacheck.PathyArbitrary._

trait InMemoryArbitrary {
  implicit val arbitraryInMemState: Arbitrary[InMemState] =
    Arbitrary(arb[Map[AFile, Vector[Data]]].map(InMemState(0, _, Map(), Map(), Map(), Map())))
}

object InMemoryArbitrary extends InMemoryArbitrary