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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.{RenderTree, Terminal}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import monocle.Prism
import scalaz._, Scalaz._

package object expression {

  /** The type for expressions targeting MongoDB 2.6 specifically. */
  type Expr2_6[A] = ExprOpCoreF[A]
  /** The type for expressions targeting MongoDB 3.0 specifically. */
  type Expr3_0[A] = Coproduct[ExprOp3_0F, ExprOpCoreF, A]
  /** The type for expressions targeting MongoDB 3.2 specifically. */
  type Expr3_2[A] = Coproduct[ExprOp3_2F, Expr3_0, A]

  /** The type for expressions supporting the most advanced capabilities. */
  type ExprOp[A] = Expr3_2[A]

  val fixExprOp =
    new ExprOpCoreF.fixpoint[Fix[ExprOp], ExprOp](_.embed)

  val DocField = Prism.partial[DocVar, BsonField] {
    case DocVar.ROOT(Some(tail)) => tail
  } (DocVar.ROOT(_))

  // FIXME: no way to put this in anybody's companion where it will be found?
  implicit def exprOpRenderTree[T[_[_]]: RecursiveT, EX[_]: Functor](implicit ops: ExprOpOps.Uni[EX]): RenderTree[T[EX]] =
    new RenderTree[T[EX]] {
      def render(v: T[EX]) = Terminal(List("ExprOp"), v.cata(ops.bson).toJs.pprint(0).some)
    }
}
