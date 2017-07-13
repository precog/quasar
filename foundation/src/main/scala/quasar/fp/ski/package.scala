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

package quasar.fp

import slamdata.Predef._

package object ski extends Serializable {
  // NB: Unicode has double-struck and bold versions of the letters, which might
  //     be more appropriate, but the code points are larger than 2 bytes, so
  //     Scala doesn't handle them.


  /** Probably not useful; implemented here mostly because it's amusing. */
  def σ[A, B, C](x: A => B => C, y: A => B, z: A): C = x(z)(y(z))

  /** A shorter name for the constant function of 1, 2, 3, or 6 args.
    * NB: the argument is eager here, so use `_ => ...` instead if you need it
    *     to be thunked.
    */
  def κ[A, B](x: B): A => B                                  = _ => x
  def κ2[A, B, C](x: C): (A, B) => C                         = (_, _) => x
  def κ3[A, B, C, D](x: D): (A, B, C) => D                   = (_, _, _) => x
  def κ6[A, B, C, D, E, F, G](x: G): (A, B, C, D, E, F) => G = (_, _, _, _, _, _) => x

  /** A shorter name for the identity function. */
  def ι[A]: A => A = x => x
}
