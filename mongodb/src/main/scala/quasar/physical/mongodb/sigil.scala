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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.qscript._

import matryoshka._
import org.bson.BsonValue
import scalaz.{Free, Kleisli}
import scalaz.std.option._
import scalaz.syntax.bind._
import scalaz.syntax.plus._
import scalaz.syntax.std.option._

/** MongoDB Document keys posessing special meaning. */
object sigil {
  import MapFuncsCore._

  /** Quasar-specific sigil, used to construct a singleton result object under
    * the following conditions:
    *
    * 1. Emitting a document value in aggregation when `$replaceRoot` is not applicable.
    *
    * 2. Emitting a value in aggregation when its type is unknown.
    *
    * 3. Emitting map-reduce results as the final stage of a query.
    *
    * NB: This can end up in data at rest due to write-back queries or caching, thus
    *     we must always support eliding this particular name when reading or
    *     querying data, even if the "primary" sigil name is changed.
    */
  val Quasar = "__quasar_mongodb_sigil"

  /** MapReduce result expression key. */
  val Value = "value"

  /** MapReduce result identity key. */
  val Id = "_id"


  @simulacrum.typeclass
  trait Sigil[A] {
    /** Returns the value of the named field or `None` if the input isn't
      * a document or the field doesn't exist.
      */
    def fieldValue(name: String): A => Option[A]

    // derived functions

    /** Returns the value under any Quasar sigil or the input if not found. */
    def elideQuasarSigil: A => A =
      v => (quasarValue(v) <+> mapReduceQuasarValue(v)) | v

    /** The value under the MapReduce value field, if found. */
    def mapReduceValue: A => Option[A] =
      fieldValue(Value)

    /** The value under the Quasar sigil in a MapReduce result, if found. */
    def mapReduceQuasarValue: A => Option[A] =
      Kleisli(quasarValue) <==< mapReduceValue

    /** Whether the given value contains the Quasar sigil nested in a MapReduce result. */
    def mapReduceQuasarSigilExists: A => Boolean =
      mapReduceValue andThen (_ exists quasarSigilExists)

    /** Whether the given value contains the Quasar sigil. */
    def quasarSigilExists: A => Boolean =
      quasarValue(_).isDefined

    /** The value under the Quasar sigil, if found. */
    def quasarValue: A => Option[A] =
      fieldValue(Quasar)
  }

  object Sigil {
    implicit val sigilBson: Sigil[Bson] = new Sigil[Bson] {
      def fieldValue(name: String): Bson => Option[Bson] =
        b => Bson._doc.getOption(b).flatMap(m => m.get(name))
    }

    implicit val sigilBsonValue: Sigil[BsonValue] = new Sigil[BsonValue] {
      def fieldValue(name: String): BsonValue => Option[BsonValue] =
        v => bsonvalue.document.getOption(v).flatMap(d => Option(d.get(name)))
    }
  }

  // Sigils in QScript

  /** Projects the Quasar sigil. */
  def projectQuasarValue[T[_[_]]: CorecursiveT]: FreeMap[T] =
    projectField(Quasar)

  /** Projects the Quasar sigil in a MapReduce result. */
  def projectMapReduceQuasarValue[T[_[_]]: CorecursiveT]: FreeMap[T] =
    projectQuasarValue[T] >> projectField(Value)

  ////

  private def projectField[T[_[_]]: CorecursiveT](f: String): FreeMap[T] =
    Free.roll(MFC(ProjectKey(HoleF, StrLit(f))))
}
