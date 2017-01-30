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

package quasar.physical.marklogic.qscript

import quasar.Data
import quasar.physical.marklogic.optics._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class DataPlanner[M[_]: Monad, FMT](
  implicit SP: StructuralPlanner[M, FMT]
) extends Planner[M, FMT, Const[Data, ?]] {

  val plan: AlgebraM[M, Const[Data, ?], XQuery] = _.getConst match {
    case Data.Binary(bytes) => xs.base64Binary(base64Bytes(bytes).xs).point[M]
    case Data.Bool(b)       => b.fold(fn.True, fn.False).point[M]
    case Data.Date(d)       => xs.date(isoLocalDate(d).xs).point[M]
    case Data.Dec(d)        => xs.double(d.toString.xqy).point[M]
    case Data.Id(id)        => id.xs.point[M]
    case Data.Int(i)        => xs.integer(i.toString.xqy).point[M]
    case Data.Interval(d)   => xs.duration(isoDuration(d).xs).point[M]
    case Data.NA            => expr.emptySeq.point[M]
    case Data.Null          => SP.null_
    case Data.Str(s)        => s.xs.point[M]
    case Data.Time(t)       => xs.time(isoLocalTime(t).xs).point[M]
    case Data.Timestamp(ts) => xs.dateTime(isoInstant(ts).xs).point[M]

    case Data.Arr(elements) =>
      elements.traverse(planK >==> SP.mkArrayElt _) >>= (xs => SP.mkArray(mkSeq(xs)))

    case Data.Obj(entries)  =>
      entries.toList.traverse { case (key, value) =>
        planK(value) >>= (SP.mkObjectEntry(key.xs, _))
      } >>= (ents => SP.mkObject(mkSeq(ents)))

    case Data.Set(elements) =>
      elements.traverse(planK) map (mkSeq(_))
  }

  ////

  private val planK: Kleisli[M, Data, XQuery] =
    Kleisli(plan compose (Const(_)))
}
