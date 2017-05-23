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

package quasar.yggdrasil

import quasar.blueeyes._
import quasar.precog.common._

trait FNModule {
  type F1
  type F2

  implicit def liftF1(f1: F1): F1Like
  trait F1Like {
    def compose(f1: F1): F1
    def andThen(f1: F1): F1
  }

  implicit def liftF2(f2: F2): F2Like
  trait F2Like {
    def applyl(cv: CValue): F1
    def applyr(cv: CValue): F1

    def andThen(f1: F1): F2
  }
}

trait FNDummyModule extends FNModule {
  import table.CFId
  type F1 = table.CF1
  type F2 = table.CF2

  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = f.partialLeft(cv)
    def applyr(cv: CValue) = f.partialRight(cv)

    def andThen(f1: F1) = table.CF2(CFId("liftF2DummyandThen")) { (c1, c2) =>
      f(c1, c2) flatMap f1.apply
    }
  }
}

