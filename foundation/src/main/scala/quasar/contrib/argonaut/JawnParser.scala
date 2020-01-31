/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.contrib.argonaut

import argonaut._
import java.lang.CharSequence
import scala.collection.mutable
import slamdata.Predef._
import org.typelevel.jawn.{Facade, FContext, SupportParser}

// Copied from https://github.com/argonaut-io/argonaut/pull/331 for now.
// TODO remove once argonaut supports jawn 1.0.0
object JawnParser extends SupportParser[Json] {
  implicit val facade: Facade[Json] =
    new Facade.NoIndexFacade[Json] {
      def jnull = Json.jNull
      def jfalse = Json.jFalse
      def jtrue = Json.jTrue
      def jnum(s: CharSequence, decIndex: Int, expIndex: Int) = Json.jNumber(JsonNumber.unsafeDecimal(s.toString))
      def jstring(s: CharSequence) = Json.jString(s.toString)

      def singleContext() = new FContext.NoIndexFContext[Json] {
        var value: Json = null
        def add(s: CharSequence) = { value = jstring(s.toString) }
        def add(v: Json) = { value = v }
        def finish: Json = value
        def isObj: Boolean = false
      }

      def arrayContext() = new FContext.NoIndexFContext[Json] {
        val vs = mutable.ListBuffer.empty[Json]
        def add(s: CharSequence) = { vs += jstring(s.toString) }
        def add(v: Json) = { vs += v }
        def finish: Json = Json.jArray(vs.toList)
        def isObj: Boolean = false
      }

      def objectContext() = new FContext.NoIndexFContext[Json] {
        var key: String = null
        var vs = JsonObject.empty
        def add(s: CharSequence): Unit =
          if (key == null) { key = s.toString } else { vs = vs + (key, jstring(s.toString)); key = null }
        def add(v: Json): Unit =
        { vs = vs + (key, v); key = null }
        def finish = Json.jObject(vs)
        def isObj = true
      }
    }
}