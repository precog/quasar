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

trait UnaryLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  trait UnaryLib extends ColumnarTableLib {
    import trans._
    import StdLib.{ BoolFrom, DoubleFrom, LongFrom, NumFrom, doubleIsDefined }

    object Unary {
      val UnaryNamespace = Vector("std", "unary")

      object Comp extends Op1F1(UnaryNamespace, "comp") {
        val tpe = UnaryOperationType(JBooleanT, JBooleanT)
        def f1(ctx: MorphContext): F1 = CF1P("builtin::unary::comp") {
          case c: BoolColumn => new BoolFrom.B(c, !_)
        }

        def spec[A <: SourceType](ctx: MorphContext): TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1(ctx))
        }
      }

      object Neg extends Op1F1(UnaryNamespace, "neg") {
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1(ctx: MorphContext): F1 = CF1P("builtin::unary::neg") {
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, -_)
          case c: LongColumn   => new LongFrom.L(c, n => true, -_)
          case c: NumColumn    => new NumFrom.N(c, n => true, -_)
        }

        def spec[A <: SourceType](ctx: MorphContext): TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1(ctx))
        }
      }
    }
  }
}
