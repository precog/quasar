/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.physical

import quasar.Predef._
import quasar.fs.Path.PathError
import quasar.fs.Path.PathError.{PathTypeError, InvalidPathError}
import quasar.fs.{FileNode, DirNode, Path}
import quasar.mongo.Collection

import scalaz._, Scalaz._

import quasar.{Terminal, RenderTree, jscore}

package object mongodb {

  final case class NameGen(nameGen: Int)

  // used by State(T).runZero
  implicit val NameGenMonoid: Monoid[NameGen] = new Monoid[NameGen] {
    def zero = NameGen(0)
    def append(f1: NameGen, f2: => NameGen) = NameGen(f1.nameGen max f2.nameGen)
  }

  def freshId(label: String): State[NameGen, String] = for {
    n <- State((s: NameGen) => s.copy(nameGen = s.nameGen + 1) -> s.nameGen)
  } yield "__" + label + n.toString

  // TODO: parameterize over label (#510)
  def freshName: State[NameGen, BsonField.Name] =
    freshId("tmp").map(BsonField.Name)

  implicit class AugmentedCollection(a: Collection) {
    def asPath: Path = {
      val first = Collection.DatabaseNameUnparser(a.databaseName)
      val rest = Collection.CollectionNameUnparser(a.collectionName)
      val segs = NonEmptyList(first, rest: _*)
      Path(DirNode.Current :: segs.list.dropRight(1).map(DirNode(_)), Some(FileNode(segs.last)))
    }
  }

  implicit class AugmentedCollectionCompanion(a: Collection.type) {
    import PathError._

    def foldPath[A](path: Path)(clusterF: => A, dbF: String => A, collF: Collection => A):
    PathError \/ A = {
      val abs = path.asAbsolute
      val segs = abs.dir.map(_.value) ++ abs.file.map(_.value).toList
      segs match {
        case Nil => \/-(clusterF)
        case first :: rest => for {
          db       <- Collection.DatabaseNameParser(first).leftMap(InvalidPathError(_))
          collSegs = rest.map(Collection.CollectionSegmentParser(_))
          coll     =  collSegs.mkString(".")
          _        <- if (Collection.utf8length(db) + 1 + Collection.utf8length(coll) > 120)
            -\/(InvalidPathError("database/collection name too long (> 120 bytes): " + db + "." + coll))
          else \/-(())
        } yield if (collSegs.isEmpty) dbF(db) else collF(Collection(db, coll))
      }
    }

    def fromPath(path: Path): PathError \/ Collection =
      foldPath[PathError \/ Collection](path)(
        -\/(PathTypeError(path, Some("has no segments"))),
        κ(-\/(InvalidPathError("path names a database, but no collection: " + path))),
        \/-(_)).join
  }

  implicit val CollectionRenderTree = new RenderTree[Collection] {
    def render(v: Collection) = Terminal(List("Collection"), Some(v.databaseName + "; " + v.collectionName))
  }
}
