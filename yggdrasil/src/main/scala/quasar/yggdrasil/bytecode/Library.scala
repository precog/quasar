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

package quasar.yggdrasil.bytecode

sealed trait IdentityPolicy
object IdentityPolicy {
  sealed trait Retain extends IdentityPolicy
  object Retain {

    /** Right IDs are discarded, left IDs are kept, in order. */
    case object Left extends Retain

    /** Left IDs are discarded, right IDs are kept, in order. */
    case object Right extends Retain

    /**
      * All IDs are kept. Prefix first, then remaining left IDs, then remaining
      * right IDs. The result is in order of the prefix/key.
      *
      * This should also be used in Morph1 to indicate the IDs are retained.
      *
      * TODO: Much like join, custom Morph2's should be allowed to specify order
      *       after the join.
      */
    case object Merge extends Retain

    /**
      * Both IDs are kept, with the left sides first. The left IDs remain in
      * order.
      */
    case object Cross extends Retain
  }

  /** A new single column of IDs are synthesized and all other IDs are discarded. */
  case object Synthesize extends IdentityPolicy

  /** All IDs are discarded. */
  case object Strip extends IdentityPolicy

  /** Both identity policies are adhered to, and then concatenated uniquely.
    * Differs from `Retain.Cross` in that it distincts its identities, whereas
    * cross retains all identities.
    */
  case class Product(left: IdentityPolicy, right: IdentityPolicy) extends IdentityPolicy
}

sealed trait FunctionLike[T] {
  val tpe: T
  val namespace: Vector[String]
  val name: String
  val opcode: Int
  val rowLevel: Boolean

  lazy val fqn          = namespace :+ name mkString "::"
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait Morphism1Like extends FunctionLike[UnaryOperationType] {
  /** This specifies how identities are returned by the Morphism1. */
  val idPolicy: IdentityPolicy = IdentityPolicy.Strip // TODO remove this default
}
trait Morphism2Like extends FunctionLike[BinaryOperationType] {
  /** This specifies how identities are returned by the Morphism2. */
  val idPolicy: IdentityPolicy = IdentityPolicy.Strip // TODO remove this default
}
trait Op1Like extends FunctionLike[UnaryOperationType]
trait Op2Like extends FunctionLike[BinaryOperationType]
trait ReductionLike extends FunctionLike[UnaryOperationType]

trait Library {
  type Morphism1 <: Morphism1Like
  type Morphism2 <: Morphism2Like
  type Op1 <: Op1Like
  type Op2 <: Op2Like
  type Reduction <: ReductionLike

  def libMorphism1: Set[Morphism1]
  def libMorphism2: Set[Morphism2]
  def lib1: Set[Op1]
  def lib2: Set[Op2]
  def libReduction: Set[Reduction]
}
