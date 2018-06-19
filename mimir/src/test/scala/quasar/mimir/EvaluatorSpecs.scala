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

package quasar.mimir

import quasar.blueeyes._, json._

import quasar.precog.TestSupport._
import quasar.precog.common._

import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.execution.EvaluationContext
import quasar.yggdrasil.table._
import quasar.yggdrasil.vfs._
import quasar.yggdrasil.util._

import scalaz._, Scalaz._, Validation._

import java.io.File

import scala.collection.mutable

trait EvaluatorSpecification extends Specification with EvaluatorTestSupport

trait EvaluatorTestSupport extends StdLibEvaluatorStack
    with BaseBlockStoreTestModule
    with IdSourceScannerModule
    with SpecificationHelp { outer =>

  def Evaluator =
    new Evaluator {
      val report = new LoggingQueryLogger[Unit] with ExceptionQueryLogger[Unit] with TimingQueryLogger[Unit]
    }

  private val groupId = new java.util.concurrent.atomic.AtomicInteger

  def newGroupId = groupId.getAndIncrement

  val defaultEvaluationContext = EvaluationContext(Path.Root, Path.Root, dateTime.now)

  val projections = Map.empty[Path, Projection]
  def vfs = sys.error("VFS metadata not supported in test.")

  trait TableCompanion extends BaseBlockStoreTestTableCompanion {
    override def load(table: Table, jtpe: JType) = EitherT {
      table.toJson map { events =>
        val eventsV = events.toStream.map(_.toJValue).traverse[Validation[ResourceError, ?], Stream[JValue]] {
          case JString(pathStr) => Validation.success {
            indexLock synchronized {      // block the WHOLE WORLD
              val path = Path(pathStr)

              val index = initialIndices get path getOrElse {
                initialIndices += (path -> currentIndex)
                currentIndex
              }

              val prefix = "filesystem"
              val target = path.path.replaceAll("/$", ".json").replaceAll("^/" + prefix, prefix)

              val src = if (target startsWith prefix)
                scala.io.Source.fromFile(new File(target.substring(prefix.length)))
              else
                scala.io.Source.fromInputStream(getClass.getResourceAsStream(target))

              val parsed: Stream[JValue] = src.getLines map JParser.parseUnsafe toStream

              currentIndex += parsed.length

              parsed zip (Stream from index) map {
                case (value, id) => JObject(JField("key", JArray(JNum(id) :: Nil)) :: JField("value", value) :: Nil)
              }
            }
          }

          case x =>
            Validation.failure(ResourceError.corrupt("Attempted to load JSON as a table from something that wasn't a string: " + x))
        }

        eventsV.disjunction.map(ss => fromJson(ss.flatten))
      }
    }
  }

  object Table extends TableCompanion

  private val initialIndices = mutable.Map[Path, Int]() // if we were doing this for real: j.u.c.HashMap
  private var currentIndex   = 0                        // if we were doing this for real: j.u.c.a.AtomicInteger
  private val indexLock      = new AnyRef               // if we were doing this for real: DIE IN A FIRE!!!
}
