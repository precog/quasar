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

import slamdata.Predef._

import scalaz._, Scalaz._

final class ZipperOps[A] private[scalaz] (self: Zipper[A]) {
  /** Returns a `Zipper` focused on the first value for which the function was
    * defined, updating the focused value with the result of the function.
    */
  final def findMap(f: A => Option[A]): Option[Zipper[A]] = {
    def updated(z: Zipper[A]): Option[Zipper[A]] =
      f(z.focus) map (z.update(_))

    updated(self) orElse {
      val c = self.positions
      findMap_(std.stream.interleave(c.lefts, c.rights))(updated)
    }
  }

  ////

  @tailrec
  private def findMap_[A, B](fa: Stream[A])(f: A => Option[B]): Option[B] = {
    fa match {
      case h #:: t => f(h) match {
        case None => findMap_(t)(f)
        case b    => b
      }
      case _ => none
    }
  }
}

trait ToZipperOps {
  implicit def toZipperOps[A](self: Zipper[A]): ZipperOps[A] =
    new ZipperOps(self)
}

object zipper extends ToZipperOps
