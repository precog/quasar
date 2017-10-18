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

package quasar.contrib.scalaz

import slamdata.Predef.{Int, Unit}

import org.specs2.mutable.Specification
import scalaz.concurrent.Task

class MoreBindOpsSpec extends Specification {
  import bind._

  "MoreBindOps" >> {
    "<<" >> {
      var state = true
      val t1: Task[Int] = Task.delay{state = true; 7}
      val t2: Task[Unit]  = Task.delay(state = false)
      ((t1 << t2).unsafePerformSync must_=== 7) and (state must_=== false)
    }
  }
}