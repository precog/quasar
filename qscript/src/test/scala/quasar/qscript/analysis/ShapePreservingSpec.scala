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

package quasar.qscript.analysis

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.qscript._

import org.specs2.scalacheck._
import pathy.Path._
import scalaz._, Scalaz._

final class ShapePreservingSpec extends quasar.Qspec with QScriptHelpers {

  implicit val params = Parameters(maxSize = 10)

  import qstdsl._

  "ShapePreserving QScriptCore" >> {

    def sr(id: IdStatus) = free.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), id)
    val count = free.Map(free.Unreferenced, recFunc.Constant(json.int(1)))
    def mkSubset(src: Option[IdStatus], from: FreeQS) = Subset(src, from, Take, count)
    def sp(qs: QScriptCore[Option[IdStatus]]): Option[IdStatus] =
      ShapePreserving[QScriptCore].shapePreservingƒ(qs)

    "Subset preserves shape" >> {
        sp(mkSubset(None, sr(ExcludeId))) must_===(ExcludeId.some)
        sp(mkSubset(None, free.Hole)) must_===(None)
        sp(mkSubset(IncludeId.some, free.Hole)) must_===(IncludeId.some)
    }

    "Union preserves shape" >> {
        sp(Union(None, sr(ExcludeId), sr(ExcludeId))) must_===(ExcludeId.some)
        sp(Union(None, sr(ExcludeId), sr(IncludeId))) must_===(None)
        sp(Union(IncludeId.some, free.Hole, free.Hole)) must_===(IncludeId.some)
        sp(Union(None, free.Hole, free.Hole)) must_===(None)
    }
  }
}
