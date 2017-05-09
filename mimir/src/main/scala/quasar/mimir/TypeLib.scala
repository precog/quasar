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

trait TypeLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  trait TypeLib extends ColumnarTableLib {
    import trans._

    val TypeNamespace = Vector("std", "type")

    override def _lib1 = super._lib1 ++ Set(isNumber, isBoolean, isNull, isString, isObject, isArray)

    object isNumber extends Op1(TypeNamespace, "isNumber") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JNumberT)
    }

    object isBoolean extends Op1(TypeNamespace, "isBoolean") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JBooleanT)
    }

    object isNull extends Op1(TypeNamespace, "isNull") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JNullT)
    }

    object isString extends Op1(TypeNamespace, "isString") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JTextT)
    }

    object isObject extends Op1(TypeNamespace, "isObject") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JObjectUnfixedT)
    }

    object isArray extends Op1(TypeNamespace, "isArray") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JArrayUnfixedT)
    }
  }
}
