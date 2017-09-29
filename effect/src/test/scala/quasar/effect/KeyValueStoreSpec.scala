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

package quasar.effect

import slamdata.Predef._
import quasar.fp._, ski._

import org.scalacheck.Arbitrary
import scalaz._, Scalaz._

abstract class KeyValueStoreSpec[K: Arbitrary: Equal, V: Arbitrary: Equal: Show]
    extends quasar.Qspec {

  type S[A] = KeyValueStore[K, V, A]

  def eval[A](program: Free[S, A]): A

  val ops = KeyValueStore.Ops[K, V, S]

  "KeyValueStore" should {
    "retrieve an entry" >> prop { (key: K, value: V) =>
      eval(
        ops.put(key, value) *> ops.get(key).run
      ) must_= value.some
    }
    "delete an entry" >> prop { (key: K, value: V) =>
      eval(
        ops.put(key, value) *> ops.delete(key) *> ops.get(key).run
      ) must_= None
    }
    "modify an entry" >> prop { (key: K, value: V, newValue: V) =>
      eval(
        ops.put(key, value) *> ops.modify(key, κ(newValue)) *> ops.get(key).run
      ) must_= newValue.some
    }
    "move a key" >> prop { (key: K, value: V, newKey: K) =>
      eval(
        ops.put(key, value) *> ops.move(key, newKey) *> ops.get(newKey).run
      ) must_= value.some
    }
    "overwrite a key" >> prop { (key: K, value: V, newValue: V) =>
      eval(
        ops.put(key, value) *> ops.put(key, newValue) *> ops.get(key).run
      ) must_= newValue.some
    }
    "retrieve all keys" >> prop { (key: K, value: V, otherKey: K, otherValue: V) =>
      key ≠ otherKey ==> {
        eval(
          ops.put(key, value) *> ops.put(otherKey, otherValue) *> ops.keys
        ) must contain(key, otherKey)
      }
    }
  }
}

object DefaultEmptyImpl extends KeyValueStoreSpec[Int, String] {
  def eval[A](program: Free[S, A]) =
    KeyValueStore.impl.default[Int, String].flatMap(program foldMap _).unsafePerformSync
}
