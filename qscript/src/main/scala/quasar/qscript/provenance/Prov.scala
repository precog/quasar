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

package quasar.qscript.provenance

import slamdata.Predef._
import quasar.contrib.matryoshka.birecursiveIso
import quasar.fp.ski.{ι, κ}

import matryoshka._
import matryoshka.implicits._
import monocle.Prism
import scalaz.{NonEmptyList => NEL, _}, Scalaz._

/**
  * @tparam D type of data
  * @tparam I type of identity
  * @tparam P provenance representation
  *
  */
trait Prov[D, I, P] {
  type PF[A] = ProvF[D, I, A]

  implicit def PC: Corecursive.Aux[P, PF]
  implicit def PR: Recursive.Aux[P, PF]

  def dataId: D => I

  import ProvF._
  private val O = ProvF.Optics[D, I]

  // Optics

  def nada: Prism[P, Unit] =
    birecursiveIso[P, PF] composePrism O.nada[P]

  def proj: Prism[P, D] =
    birecursiveIso[P, PF] composePrism O.proj[P]

  def value: Prism[P, I] =
    birecursiveIso[P, PF] composePrism O.value[P]

  def both: Prism[P, (P, P)] =
    birecursiveIso[P, PF] composePrism O.both[P]

  def oneOf: Prism[P, (P, P)] =
    birecursiveIso[P, PF] composePrism O.oneOf[P]

  def thenn: Prism[P, (P, P)] =
    birecursiveIso[P, PF] composePrism O.thenn[P]

  // Operations

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def joinKeys(left: P, right: P)(implicit D: Equal[D]): JoinKeys[I] = {
    // NB: We don't want to consider identities themselves when checking for
    //     joinability, just that both sides are Value(_).
    implicit val ignoreI: Equal[I] = Equal.equalBy(κ(()))

    def joinBoths(l0: P, r0: P): JoinKeys[I] =
      JoinKeys((for {
        l <- flattenBoth(l0).join
        r <- flattenBoth(r0).join
        if l ≟ r
        k <- joinKeys(l, r).keys
      } yield k).foldMap1Opt(ι).toIList)

    def joinOneOfs(l0: P, r0: P): JoinKeys[I] =
      JoinKeys((for {
        l <- flattenOneOf(l0)
        r <- flattenOneOf(r0)
        if l ≟ r
        k <- joinKeys(l, r).keys
      } yield k).foldMap1Opt(ι).toIList)

    def joinThens(l0: P, r0: P): JoinKeys[I] =
      JoinKeys(for {
        ls <- flattenThen(l0)
        rs <- flattenThen(r0)
        (l, r) <- ls.reverse.fzip(rs.reverse) takeWhile { case (x, y) => x ≟ y }
        k  <- joinKeys(l, r).keys
      } yield k)

    (left.project, right.project) match {
      case (Nada(), _)          => mempty[JoinKeys, I]
      case (_, Nada())          => mempty[JoinKeys, I]
      case (Proj(_), Proj(_))   => mempty[JoinKeys, I]
      case (Value(l), Value(r)) => JoinKeys.singleton(l, r)
      case (Value(l), Proj(r))  => JoinKeys.singleton(l, dataId(r))
      case (Proj(l), Value(r))  => JoinKeys.singleton(dataId(l), r)
      case (Then(_, _), _)      => joinThens(left, right)
      case (_, Then(_, _))      => joinThens(left, right)
      case (Both(_, _), _)      => joinBoths(left, right)
      case (_, Both(_, _))      => joinBoths(left, right)
      case (OneOf(_, _), _)     => joinOneOfs(left, right)
      case (_, OneOf(_, _))     => joinOneOfs(left, right)
    }
  }

  // eliminate duplicates within contiguous Both/OneOf and reassociate to the right
  def normalize(p: P)(implicit eqD: Equal[D], eqI: Equal[I]): P = {
    def normalize0(alternates: NEL[NEL[P]]): P =
      alternates
        .map(_.distinctE1.foldRight1(both(_, _)))
        .distinctE1
        .foldRight1(oneOf(_, _))

    val normalizeƒ: Algebra[PF, NEL[NEL[P]]] = {
      case Both(l, r)  => (l |@| r)(_ append _)
      case OneOf(l, r) => l append r
      case Then(l, r)  => NEL(NEL(thenn(normalize0(l), normalize0(r))))
      case Value(i)    => NEL(NEL(value(i)))
      case Proj(d)     => NEL(NEL(proj(d)))
      case Nada()      => NEL(NEL(nada()))
    }

    normalize0(p.cata(normalizeƒ))
  }


  // Instances

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def provenanceEqual(implicit eqD: Equal[D], eqI: Equal[I]): Equal[P] = {
    implicit def listSetPEqual: Equal[IList[P] @@ AsSet] =
      asSetEqual[IList, P](Foldable[IList], provenanceEqual(eqD, eqI))

    def thenEq(l: P, r: P): Boolean =
      AsSet(flattenThen(l)) ≟ AsSet(flattenThen(r))

    def bothEq(l: P, r: P): Boolean =
      AsSet(flattenBoth(l) map (ls => AsSet(nubNada(ls)))) ≟
        AsSet(flattenBoth(r) map (rs => AsSet(nubNada(rs))))

    def oneOfEq(l: P, r: P): Boolean =
      AsSet(nubNada(flattenOneOf(l))) ≟ AsSet(nubNada(flattenOneOf(r)))

    Equal.equal((x, y) => (x.project, y.project) match {
      case (Nada(), Nada())     => true
      case (Value(l), Value(r)) => l ≟ r
      case (Proj(l), Proj(r))   => l ≟ r
      case (Both(_, _), _)      => bothEq(x, y)
      case (_, Both(_, _))      => bothEq(x, y)
      case (OneOf(_, _), _)     => oneOfEq(x, y)
      case (_, OneOf(_, _))     => oneOfEq(x, y)
      case (Then(_, _), _)      => thenEq(x, y)
      case (_, Then(_, _))      => thenEq(x, y)
      case _                    => false
    })
  }

  ////

  /** The disjunction of sets arising from distributing `OneOf` over trees
    * of `Both`.
    */
  private def flattenBoth(p: P): IList[IList[P]] =
    p.elgotPara[IList[IList[P]]](flattenBothƒ)

  private val flattenBothƒ: ElgotAlgebra[(P, ?), PF, IList[IList[P]]] = {
    case (_, Both(l, r))  => (l |@| r)(_ ++ _)
    case (_, OneOf(l, r)) => l ++ r
    case (other, _)       => IList(IList(other))
  }

  /** The disjunction described by a tree of `OneOf`. */
  private def flattenOneOf(p: P): IList[P] =
    p.elgotPara[IList[P]](flattenOneOfƒ)

  private val flattenOneOfƒ: ElgotAlgebra[(P, ?), PF, IList[P]] = {
    case (_, Both(l, r))  => l ++ r
    case (_, Then(l, r))  => l ++ r
    case (_, OneOf(l, r)) => l ++ r
    case (other, _)       => IList(other)
  }

  /** The disjunction of sequences arising from distributing `OneOf` over trees
    * of `Then`.
    */
  private def flattenThen(p: P): IList[IList[P]] =
    p.elgotPara[IList[IList[P]]](flattenThenƒ)

  private val flattenThenƒ: ElgotAlgebra[(P, ?), PF, IList[IList[P]]] = {
    case (_, Then(l, r))  => (l |@| r)(_ ++ _)
    case (_, OneOf(l, r)) => l ++ r
    case (other, _)       => IList(IList(other))
  }

  private def nubNada(ps: IList[P]): IList[P] =
    ps.filter(nada.isEmpty)
}

object Prov {
  def apply[D, I, P]
      (dataId0: D => I)
      (implicit
        TC: Corecursive.Aux[P, ProvF[D, I, ?]],
        TR: Recursive.Aux[P, ProvF[D, I, ?]])
      : Prov[D, I, P] =
    new Prov[D, I, P] {
      val PC = TC
      val PR = TR
      val dataId = dataId0
    }
}
