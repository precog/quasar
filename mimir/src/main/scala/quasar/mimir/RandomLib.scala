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

package quasar.mimir

import quasar.yggdrasil.bytecode._

import quasar.yggdrasil._
import quasar.yggdrasil.table._
import TransSpecModule._

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.foldable._

trait RandomLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  trait RandomLib extends ColumnarTableLib {
    import trans._

    val RandomNamespace = Vector("std", "random")

    override def _libMorphism1 = super._libMorphism1 ++ Set(UniformDistribution)

    object UniformDistribution extends Morphism1(RandomNamespace, "uniform") {
      // todo currently we are seeding with a number, change this to a String
      val tpe = UnaryOperationType(JNumberT, JNumberT)
      type Result = Option[Long]

      def reducer = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val cols = schema.columns(JObjectFixedT(Map(paths.Value.name -> JNumberT)))

          val result: Set[Result] = cols map {
            case (c: LongColumn)                              =>
              range collectFirst { case i if c.isDefinedAt(i) => i } map { c(_) }

            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(implicitly[Monoid[Result]])
        }
      }

      def extract(res: Result): Table = {
        res map { resultSeed =>
          val distTable = Table.uniformDistribution(MmixPrng(resultSeed))
          distTable.transform(buildConstantWrapSpec(TransSpec1.Id))
        } getOrElse Table.empty
      }

      def apply(table: Table): M[Table] =
        table.reduce(reducer) map extract
    }
  }
}
