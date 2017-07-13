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

package quasar.physical.couchbase.planner

import slamdata.Predef._
import quasar.{Data => QData, NameGenerator}
import quasar.contrib.pathy.AFile
import quasar.physical.couchbase._,
  common.ContextReader,
  N1QL.{Eq, Id, _},
  Select.{Filter, Value, _}
import quasar.qscript, qscript._

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

final class ShiftedReadFilePlanner[T[_[_]]: CorecursiveT, F[_]: Applicative: ContextReader: NameGenerator]
  extends Planner[T, F, Const[ShiftedRead[AFile], ?]] {

  def str(v: String) = Data[T[N1QL]](QData.Str(v))
  def id(v: String)  = Id[T[N1QL]](v)

  val plan: AlgebraM[F, Const[ShiftedRead[AFile], ?], T[N1QL]] = {
    case Const(ShiftedRead(absFile, idStatus)) =>
      (genId[T[N1QL], F] ⊛ ContextReader[F].ask) { (gId, ctx) =>
        val collection = common.docTypeValueFromPath(absFile)

        val v =
          IfMissing(
            SelectField(gId.embed, str("value").embed).embed,
            gId.embed).embed

        val mId = SelectField(Meta(gId.embed).embed, str("id").embed)

        val r = idStatus match {
          case IdOnly    => gId.embed
          case IncludeId => Arr(List(gId.embed, v)).embed
          case ExcludeId => v
        }

        Select(
          Value(true),
          ResultExpr(r, none).wrapNel,
          Keyspace(id(ctx.bucket.v).embed, gId.some).some,
          join    = none,
          unnest  = none,
          let     = nil,
          filter  = collection.v.nonEmpty.option(
                      Eq(id(ctx.docTypeKey.v).embed, str(collection.v).embed).embed
                    ) ∘ (Filter(_)),
          groupBy = none,
          orderBy = nil).embed
      }
  }
}
