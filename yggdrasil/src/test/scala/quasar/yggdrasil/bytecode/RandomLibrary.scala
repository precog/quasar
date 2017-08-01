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

import org.scalacheck._
import Gen._

trait RandomLibrary extends Library {

  private lazy val genMorphism1 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Morphism1(Vector(ns: _*), n, op)

  private lazy val genMorphism2 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Morphism2(Vector(ns: _*), n, op)

  private lazy val genOp1 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Op1(Vector(ns: _*), n, op)

  private lazy val genOp2 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Op2(Vector(ns: _*), n, op)

  private lazy val genReduction = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Reduction(Vector(ns: _*), n, op)

  val reductions = Set(
    Reduction(Vector(), "count", 0x2000),
    Reduction(Vector(), "max", 0x2001),
    Reduction(Vector(), "min", 0x2004),
    Reduction(Vector(), "sum", 0x2002),
    Reduction(Vector(), "mean", 0x2013),
    Reduction(Vector(), "geometricMean", 0x2003),
    Reduction(Vector(), "sumSq", 0x2005),
    Reduction(Vector(), "variance", 0x2006),
    Reduction(Vector(), "stdDev", 0x2007),
    Reduction(Vector(), "median", 0x2008),
    Reduction(Vector(), "mode", 0x2009))


  lazy val libMorphism1 = containerOfN[Set, Morphism1](30, genMorphism1).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val libMorphism2 = containerOfN[Set, Morphism2](30, genMorphism2).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val lib1 = containerOfN[Set, Op1](30, genOp1).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val lib2 = containerOfN[Set, Op2](30, genOp2).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val libReduction = reductions ++ containerOfN[Set, Reduction](30, genReduction).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes

  lazy val expandGlob = Morphism1(Vector("std", "fs"), "expandGlob", 0x0004)


  case class Morphism1(namespace: Vector[String], name: String, opcode: Int) extends Morphism1Like {
    val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)
    val rowLevel: Boolean = false
  }

  case class Morphism2(namespace: Vector[String], name: String, opcode: Int) extends Morphism2Like {
    val tpe = BinaryOperationType(JType.JUniverseT, JType.JUniverseT, JType.JUniverseT)
    val rowLevel: Boolean = false
  }

  case class Op1(namespace: Vector[String], name: String, opcode: Int) extends Op1Like with Morphism1Like {
    val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)
    val rowLevel: Boolean = true
  }

  case class Op2(namespace: Vector[String], name: String, opcode: Int) extends Op2Like with Morphism2Like {
    val tpe = BinaryOperationType(JType.JUniverseT, JType.JUniverseT, JType.JUniverseT)
    val rowLevel: Boolean = true
  }

  case class Reduction(namespace: Vector[String], name: String, opcode: Int) extends ReductionLike with Morphism1Like {
    val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)
    val rowLevel: Boolean = false
  }
}
