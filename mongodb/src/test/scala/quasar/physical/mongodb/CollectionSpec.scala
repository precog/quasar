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

package quasar.physical.mongodb

import org.specs2.execute.Result
import slamdata.Predef._
import quasar.fs.SpecialStr

import pathy.Path._
import pathy.scalacheck._

class CollectionSpec extends quasar.Qspec {
  import CollectionUtil._

  "Collection.fromFile" should {

    "handle simple name" in {
      Collection.fromFile(rootDir </> dir("db") </> file("foo")) must
        beRightDisjunction(collection("db", "foo"))
    }

    "handle simple relative path" in {
      Collection.fromFile(rootDir </> dir("db") </> dir("foo") </> file("bar")) must
        beRightDisjunction(collection("db", "foo.bar"))
    }

    "escape leading '.'" in {
      Collection.fromFile(rootDir </> dir("db") </> file(".hidden")) must
        beRightDisjunction(collection("db", "\\.hidden"))
    }

    "escape '.' with path separators" in {
      Collection.fromFile(rootDir </> dir("db") </> dir("foo") </> file("bar.baz")) must
        beRightDisjunction(collection("db", "foo.bar\\.baz"))
    }

    "escape '$'" in {
      Collection.fromFile(rootDir </> dir("db") </> file("foo$")) must
        beRightDisjunction(collection("db", "foo\\d"))
    }

    "escape '\\'" in {
      Collection.fromFile(rootDir </> dir("db") </> file("foo\\bar")) must
        beRightDisjunction(collection("db", "foo\\\\bar"))
    }

    "accept path with 120 characters" in {
      val longName = Stream.continually("A").take(117).mkString
      Collection.fromFile(rootDir </> dir("db") </> file(longName)) must
        beRightDisjunction(collection("db", longName))
    }

    "reject path longer than 120 characters" in {
      val longName = Stream.continually("B").take(118).mkString
      Collection.fromFile(rootDir </> dir("db") </> file(longName)) must beLeftDisjunction
    }

    "reject path that translates to more than 120 characters" in {
      val longName = "." + Stream.continually("C").take(116).mkString
      Collection.fromFile(rootDir </> dir("db") </> file(longName)) must beLeftDisjunction
    }

    "preserve space" in {
      Collection.fromFile(rootDir </> dir("db") </> dir("foo") </> file("bar baz")) must
        beRightDisjunction(collection("db", "foo.bar baz"))
    }

    "reject path with db but no collection" in {
      Collection.fromFile(rootDir </> file("db")) must beLeftDisjunction
    }

    "escape space in db name" in {
      Collection.fromFile(rootDir </> dir("db 1") </> file("foo")) must
        beRightDisjunction(collection("db+1", "foo"))
    }

    "escape leading dot in db name" in {
      Collection.fromFile(rootDir </> dir(".trash") </> file("foo")) must
        beRightDisjunction(collection("~trash", "foo"))
    }

    "escape MongoDB-reserved chars in db name" in {
      Collection.fromFile(rootDir </> dir("db/\\\"") </> file("foo")) must
        beRightDisjunction(collection("db%div%esc%quot", "foo"))
    }

    "escape Windows-only MongoDB-reserved chars in db name" in {
      Collection.fromFile(rootDir </> dir("db*<>:|?") </> file("foo")) must
        beRightDisjunction(collection("db%mul%lt%gt%colon%bar%qmark", "foo"))
    }

    "escape escape characters in db name" in {
      Collection.fromFile(rootDir </> dir("db%+~") </> file("foo")) must
        beRightDisjunction(collection("db%%%add%tilde", "foo"))
    }

    "fail with sequence of escapes exceeding maximum length" in {
      Collection.fromFile(rootDir </> dir("~:?~:?~:?~:") </> file("foo")) must beLeftDisjunction
    }

    "succeed with db name of exactly 64 bytes when encoded" in {
      val dbName = List.fill(64/4)("💩").mkString
      Collection.fromFile(rootDir </> dir(dbName) </> file("foo")) must beRightDisjunction
    }

    "fail with db name exceeding 64 bytes when encoded" in {
      val dbName = List.fill(64/4 + 1)("💩").mkString
      Collection.fromFile(rootDir </> dir(dbName) </> file("foo")) must beLeftDisjunction
    }

    "succeed with crazy char" in {
      Collection.fromFile(rootDir </> dir("*_") </> dir("_ⶡ\\݅ ᐠ") </> file("儨")) must
        beRightDisjunction
    }

    "never emit an invalid db name" >> prop { (db: SpecialStr, c: SpecialStr) =>
      val f = rootDir </> dir(db.str) </> file(c.str)
      val notTooLong = posixCodec.printPath(f).length < 30
      // NB: as long as the path is not too long, it should convert to something that's legal
      notTooLong ==> {
        Collection.fromFile(f).fold(
          err => scala.sys.error(err.toString),
          coll => {
            Result.foreach(" ./\\*<>:|?") { c => coll.database.value.toList must not(contain(c)) }
          })
      }
    }.set(maxSize = 5)

    "round-trip" >> prop { f: AbsFileOf[SpecialStr] =>
      // NB: the path might be too long to convert
      val r = Collection.fromFile(f.path)
      (r.isRight) ==> {
        r.fold(
          err  => scala.sys.error(err.toString),
          coll => identicalPath(f.path, coll.asFile) must beTrue)
      }
      // MongoDB doesn't like "paths" that are longer than 120 bytes of utf-8 encoded characters
      // So there is absolutely no point generating paths longer than 120 characters. However, a 120 character path still
      // has a good change of being refused by MongoDB because many of the characters generated by SpecialStr have large
      // utf-encodings so we retryUntil we find an appropriate one even if that is somewhat dangerous (unbounded execution time)
      // For this particular test, execution time does not seem to be a large concern (couple seconds).
    }.set(minSize = 2, maxSize = 120).setGen(PathOf.absFileOfArbitrary[SpecialStr].arbitrary.retryUntil(f => Collection.fromFile(f.path).isRight))
  }

  "Collection.prefixFromDir" should {
    "return a collection prefix" in {
      Collection.prefixFromDir(rootDir </> dir("foo") </> dir("bar")) must beRightDisjunction(CollectionName("bar"))
    }

    "reject path without collection" in {
      Collection.prefixFromDir(rootDir </> dir("foo")) must beLeftDisjunction
    }

    "reject path without db or collection" in {
      Collection.prefixFromDir(rootDir) must beLeftDisjunction
    }
  }

  "Collection.asFile" should {

    "handle simple name" in {
      collection("db", "foo").asFile must_==
        rootDir </> dir("db") </> file("foo")
    }

    "handle simple path" in {
      collection("db", "foo.bar").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar")
    }

    "preserve space" in {
      collection("db", "foo.bar baz").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar baz")
    }

    "unescape leading '.'" in {
      collection("db", "\\.hidden").asFile must_==
        rootDir </> dir("db") </> file(".hidden")
    }

    "unescape '$'" in {
      collection("db", "foo\\d").asFile must_==
        rootDir </> dir("db") </> file("foo$")
    }

    "unescape '\\'" in {
      collection("db", "foo\\\\bar").asFile must_==
        rootDir </> dir("db") </> file("foo\\bar")
    }

    "unescape '.' with path separators" in {
      collection("db", "foo.bar\\.baz").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar.baz")
    }

    "ignore slash" in {
      collection("db", "foo/bar").asFile must_==
        rootDir </> dir("db") </> file("foo/bar")
    }

    "ignore unrecognized escape in database name" in {
      collection("%foo", "bar").asFile must_==
        rootDir </> dir("%foo") </> file("bar")
    }

    "not explode on empty collection name" in {
      collection("foo", "").asFile must_==
        rootDir </> dir("foo") </> file("")
    }
  }

  "dbName <-> dirName are inverses" >> prop { db: SpecialStr =>
    val name = Collection.dbNameFromPath(rootDir </> dir(db.str))
    // NB: can fail if the path has enough multi-byte chars to exceed 64 bytes
    name.isRight ==> {
      name.map(Collection.dirNameFromDbName(_)) must beRightDisjunction(DirName(db.str))
    }
  }.set(maxSize = 20)
}

object CollectionUtil {
  // NB: _not_ validating these strings at all
  def collection(dbName: String, collectionName: String): Collection =
    Collection(DatabaseName(dbName), CollectionName(collectionName))
}
