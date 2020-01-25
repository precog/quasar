/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.api.resource

import slamdata.Predef.Some

import pathy.Path._
import scalaz.std.option._
import scalaz.std.tuple._

final class ResourcePathSpec extends quasar.Qspec {
  import ResourceNameGenerator._

  "append name" >> prop { (x: ResourceName, y: ResourceName) =>
    val f = rootDir </> dir(x.value) </> file(y.value)
    (ResourcePath.root() / x / y) must_= ResourcePath.leaf(f)
  }

  "prepend name" >> prop { (x: ResourceName, y: ResourceName) =>
    val f = rootDir </> dir(x.value) </> file(y.value)
    (x /: y /: ResourcePath.root()) must_= ResourcePath.leaf(f)
  }

  "uncons" >> prop { (x: ResourceName, y: ResourceName) =>
    val p = ResourcePath.root() / x / y
    p.uncons must_= Some((x, ResourcePath.root() / y))
  }

  "uncons root" >> {
    ResourcePath.root().uncons must beNone
  }

  "unsnoc" >> prop { (x: ResourceName, y: ResourceName) =>
    val p = ResourcePath.root() / x / y
    p.unsnoc must_= Some((ResourcePath.root() / x, y))
  }

  "unsnoc root" >> {
    ResourcePath.root().unsnoc must beNone
  }
}
