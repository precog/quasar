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

package quasar

import java.nio.file.{Files, Paths}

package object precog {
  type jPath       = java.nio.file.Path
  type =?>[-A, +B] = scala.PartialFunction[A, B]
  type CTag[A]     = scala.reflect.ClassTag[A]

  def ctag[A](implicit z: CTag[A]): CTag[A] = z

  def jPath(path: String): jPath = Paths get path

  implicit class jPathOps(private val p: jPath) {
    def slurpBytes(): Array[Byte] = Files readAllBytes p
  }
}
