/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.fs.mount

import quasar.Predef._
import quasar.fs.ADir

import pathy.Path._
import scalaz._, Scalaz._

/** A mapping of values to directory paths, maintains the invariant that no
  * path is a prefix of any other path.
  *
  * The current implementation is linear in the number of mounts, might be able
  * to do better using a different data structure that takes advantage of the
  * invariant and structure of the `ADir` keys.
  */
final class Mounts[A] private (val toMap: Map[ADir, A]) {
  /** Returns whether the given directory is a candidate as a mapping key,
    * a "left" result describes why the key is not a candidate and a "right"
    * signifies a valid candidate.
    */
  def candidacy(d: ADir): String \/ Unit =
    if (toMap contains d)
      ().right
    else
      toMap.keys.toStream.traverseU_ { mnt =>
        mnt.relativeTo(d)
          .as("existing mount below: " + posixCodec.printPath(mnt))
          .toLeftDisjunction(()) *>
        d.relativeTo(mnt)
          .as("existing mount above: " + posixCodec.printPath(mnt))
          .toLeftDisjunction(())
      }

  def add(at: ADir, a: A): String \/ Mounts[A] =
    candidacy(at) as (new Mounts(toMap + (at -> a)))

  def + (mount: (ADir, A)): String \/ Mounts[A] =
    add(mount._1, mount._2)

  def remove(at: ADir): Mounts[A] =
    new Mounts(toMap - at)

  def - (at: ADir): Mounts[A] =
    remove(at)

  def lookup(at: ADir): Option[A] =
    toMap.get(at)

  def mapWithDir[B](f: (ADir, A) => B): Mounts[B] =
    new Mounts(toMap map { case (d, a) => (d, f(d, a)) })

  def map[B](f: A => B): Mounts[B] =
    mapWithDir((_, a) => f(a))

  /** Right-biased union of two `Mounts`. */
  def union(other: Mounts[A]): String \/ Mounts[A] =
    other.toMap.toList.foldLeftM[String \/ ?, Mounts[A]](this)(_ + _)
}

object Mounts {
  def empty[A] = _empty.asInstanceOf[Mounts[A]]

  def singleton[A](dir: ADir, a: A): Mounts[A] =
    new Mounts(Map(dir -> a))

  def fromFoldable[F[_]: Foldable, A](entries: F[(ADir, A)]): String \/ Mounts[A] =
    entries.foldLeftM[String \/ ?, Mounts[A]](empty[A])(_ + _)

  implicit val mountsFunctor: Functor[Mounts] =
    new Functor[Mounts] {
      def map[A, B](fa: Mounts[A])(f: A => B) = fa map f
    }

  ////

  private val _empty = new Mounts(Map())
}
