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

package quasar.std

import quasar.Predef._

trait StdLib extends Library {
  val math = MathLib

  val structural = StructuralLib

  val agg = AggLib

  val identity = IdentityLib

  val relations = RelationsLib

  val set = SetLib

  val array = ArrayLib

  val string = StringLib

  val date = DateLib

  val unaryFunctions = math.unaryFunctions ++ structural.unaryFunctions ++ agg.unaryFunctions ++ identity.unaryFunctions ++ relations.unaryFunctions ++ set.unaryFunctions ++ array.unaryFunctions ++ string.unaryFunctions ++ date.unaryFunctions ++ Nil
  val binaryFunctions = math.binaryFunctions ++ structural.binaryFunctions ++ agg.binaryFunctions ++ identity.binaryFunctions ++ relations.binaryFunctions ++ set.binaryFunctions ++ array.binaryFunctions ++ string.binaryFunctions ++ date.binaryFunctions ++ Nil
  val ternaryFunctions = math.ternaryFunctions ++ structural.ternaryFunctions ++ agg.ternaryFunctions ++ identity.ternaryFunctions ++ relations.ternaryFunctions ++ set.ternaryFunctions ++ array.ternaryFunctions ++ string.ternaryFunctions ++ date.ternaryFunctions ++ Nil
}
object StdLib extends StdLib
