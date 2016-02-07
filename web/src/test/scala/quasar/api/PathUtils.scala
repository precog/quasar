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

package quasar.api

import quasar.Predef._

import pathy.Path, Path._
import scalaz._, Scalaz._

trait PathUtils {
  // NB: these paths confuse the codec and don't seem important at the moment.
  // See https://github.com/slamdata/scala-pathy/issues/23.
  def hasDot(p: Path[_, _, _]): Boolean =
    flatten(false, false, false, d => d == "." || d == "..", f => f == "." || f == "..", p).toList.contains(true)
}

object PathUtils extends PathUtils
