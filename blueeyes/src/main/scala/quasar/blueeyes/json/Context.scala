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

package quasar.blueeyes.json

import quasar.blueeyes._

// context used by JParser, taken from jawn under MIT license.
// (https://github.com/non/jawn)

private[json] sealed trait Context {
  def add(s: String): Unit
  def add(v: JValue): Unit
  def finish: JValue
  def isObj: Boolean
}

private[json] final class SingleContext extends Context {
  var value: JValue = null
  def add(s: String): Unit = value = JString(s)
  def add(v: JValue): Unit = value = v
  def finish               = value
  def isObj                = false
}

private[json] final class ArrContext extends Context {
  private val vs = ArrayBuffer[JValue]()

  def add(s: String): Unit = vs.append(JString(s))
  def add(v: JValue): Unit = vs.append(v)
  def finish               = new JArray(vs.toList)
  def isObj                = false
}

private[json] final class ObjContext extends Context {
  private var key: String = null
  private val vs          = scmMap[String, JValue]()

  def add(s: String): Unit =
    if (key == null) {
      key = s
    } else {
      vs(key) = JString(s)
      key = null
    }

  def add(v: JValue): Unit = { vs(key) = v; key = null }

  def finish = JObject(vs.toMap)
  def isObj  = true
}
