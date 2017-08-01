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

package quasar.niflheim

import quasar.precog.util.IOUtils

import java.util.concurrent.ScheduledThreadPoolExecutor

import org.specs2.mutable.{After, Specification}

class CookStateLogSpecs extends Specification {
  val txLogScheduler = new ScheduledThreadPoolExecutor(5)

  trait LogState extends After {
    val workDir = IOUtils.createTmpDir("cookstatespecs").unsafePerformIO

    def after = {
      IOUtils.recursiveDelete(workDir).unsafePerformIO
    }
  }

  "CookStateLog" should {
    "Properly initialize" in new LogState {
      val txLog = new CookStateLog(workDir, txLogScheduler)

      txLog.currentBlockId mustEqual 0l
      txLog.pendingCookIds must beEmpty
    }

    "Lock its directory during operation" in new LogState {
      val txLog = new CookStateLog(workDir, txLogScheduler)

      (new CookStateLog(workDir, txLogScheduler)) must throwAn[Exception]
    }
  }
}
