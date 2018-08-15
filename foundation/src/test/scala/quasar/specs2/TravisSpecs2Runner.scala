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

package quasar.specs2

import slamdata.Predef._
import org.specs2.runner.{BaseSbtRunner, MasterSbtRunner, SbtTask, SlaveSbtRunner, Specs2Framework}
import sbt.testing.{EventHandler, Logger, Task, TaskDef}
import scala.language.reflectiveCalls
import java.lang.{ClassLoader, System}

class TravisSpecs2Runner extends Specs2Framework {
  override def runner(args: Array[String], remoteArgs: Array[String], loader: ClassLoader) =
    new MasterSbtRunner(args, remoteArgs, loader) with TravisRunnerInjection

  override def slaveRunner(args: Array[String], remoteArgs: Array[String], loader: ClassLoader, send: String => Unit) =
    new SlaveSbtRunner(args, remoteArgs, loader, send) with TravisRunnerInjection
}

private[specs2] trait TravisRunnerInjection extends BaseSbtRunner {
  private val loader = this.asInstanceOf[{ val loader: ClassLoader }].loader     // please don't hurt me, Eric...

  override def newTask(aTaskDef: TaskDef): Task = {
    new SbtTask(aTaskDef, env, loader) {
      override def execute(handler: EventHandler, loggers: Array[Logger]) = {
        System.err.println(s"travis_fold:start:specs2:${aTaskDef.fullyQualifiedName}")
        val back = super.execute(handler, loggers)
        System.err.println(s"travis_fold:end:specs2:${aTaskDef.fullyQualifiedName}")
        back
      }
    }
  }
}
