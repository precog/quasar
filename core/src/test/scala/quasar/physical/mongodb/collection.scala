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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.fs.SpecialStr

import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck._

class CollectionSpec extends Specification with ScalaCheck with DisjunctionMatchers {

  "Collection.fromPath" should {

    "handle simple name" in {
      Collection.fromPath(rootDir </> dir("db") </> file("foo")) must
        beRightDisjunction(Collection("db", "foo"))
    }

    "handle simple relative path" in {
      Collection.fromPath(rootDir </> dir("db") </> dir("foo") </> file("bar")) must
        beRightDisjunction(Collection("db", "foo.bar"))
    }

    "escape leading '.'" in {
      Collection.fromPath(rootDir </> dir("db") </> file(".hidden")) must
        beRightDisjunction(Collection("db", "\\.hidden"))
    }

    "escape '.' with path separators" in {
      Collection.fromPath(rootDir </> dir("db") </> dir("foo") </> file("bar.baz")) must
        beRightDisjunction(Collection("db", "foo.bar\\.baz"))
    }

    "escape '$'" in {
      Collection.fromPath(rootDir </> dir("db") </> file("foo$")) must
        beRightDisjunction(Collection("db", "foo\\d"))
    }

    "escape '\\'" in {
      Collection.fromPath(rootDir </> dir("db") </> file("foo\\bar")) must
        beRightDisjunction(Collection("db", "foo\\\\bar"))
    }

    "accept path with 120 characters" in {
      val longName = Stream.continually("A").take(117).mkString
      Collection.fromPath(rootDir </> dir("db") </> file(longName)) must
        beRightDisjunction(Collection("db", longName))
    }

    "reject path longer than 120 characters" in {
      val longName = Stream.continually("B").take(118).mkString
      Collection.fromPath(rootDir </> dir("db") </> file(longName)) must beLeftDisjunction
    }

    "reject path that translates to more than 120 characters" in {
      val longName = "." + Stream.continually("C").take(116).mkString
      Collection.fromPath(rootDir </> dir("db") </> file(longName)) must beLeftDisjunction
    }

    "preserve space" in {
      Collection.fromPath(rootDir </> dir("db") </> dir("foo") </> file("bar baz")) must
        beRightDisjunction(Collection("db", "foo.bar baz"))
    }

    "reject path without db or collection" in {
      Collection.fromPath(rootDir) must beLeftDisjunction
    }

    "reject path with db but no collection" in {
      Collection.fromPath(rootDir </> dir("db")) must beLeftDisjunction
    }

    "escape space in db name" in {
      Collection.fromPath(rootDir </> dir("db 1") </> file("foo")) must
        beRightDisjunction(Collection("db+1", "foo"))
    }

    "escape leading dot in db name" in {
      Collection.fromPath(rootDir </> dir(".trash") </> file("foo")) must
        beRightDisjunction(Collection("~trash", "foo"))
    }

    "escape MongoDB-reserved chars in db name" in {
      Collection.fromPath(rootDir </> dir("db/\\\"") </> file("foo")) must
        beRightDisjunction(Collection("db%div%esc%quot", "foo"))
    }

    "escape Windows-only MongoDB-reserved chars in db name" in {
      Collection.fromPath(rootDir </> dir("db*<>:|?") </> file("foo")) must
        beRightDisjunction(Collection("db%mul%lt%gt%colon%bar%qmark", "foo"))
    }

    "escape escape characters in db name" in {
      Collection.fromPath(rootDir </> dir("db%+~") </> file("foo")) must
        beRightDisjunction(Collection("db%%%add%tilde", "foo"))
    }

    "fail with sequence of escapes exceeding maximum length" in {
      Collection.fromPath(rootDir </> dir("~:?~:?~:?~:") </> file("foo")) must beLeftDisjunction
    }

    "succeed with db name of exactly 64 bytes when encoded" in {
      val dbName = List.fill(64/4)("💩").mkString
      Collection.fromPath(rootDir </> dir(dbName) </> file("foo")) must beRightDisjunction
    }

    "fail with db name exceeding 64 bytes when encoded" in {
      val dbName = List.fill(64/4 + 1)("💩").mkString
      Collection.fromPath(rootDir </> dir(dbName) </> file("foo")) must beLeftDisjunction
    }

    "succeed with crazy char" in {
      Collection.fromPath(rootDir </> dir("*_") </> dir("_ⶡ\\݅ ᐠ") </> file("儨")) must
        beRightDisjunction
    }

    "never emit an invalid db name" ! prop { (db: SpecialStr, c: SpecialStr) =>
      val f = rootDir </> dir(db.str) </> file(c.str)
      val notTooLong = posixCodec.printPath(f).length < 30
      // NB: as long as the path is not too long, it should convert to something that's legal
      notTooLong ==> {
        Collection.fromPath(f).fold(
          err => scala.sys.error(err.toString),
          coll => {
            " ./\\*<>:|?".foreach { c => coll.databaseName.toList must not(contain(c)) }
          })
      }
    }.set(maxSize = 5)

    "round-trip" ! prop { f: AbsFileOf[SpecialStr] =>
      // NB: the path might be too long to convert
      val r = Collection.fromPath(f.path)
      (r.isRight) ==> {
        r.fold(
          err  => scala.sys.error(err.toString),
          coll => identicalPath(f.path, coll.asFile) must beTrue)
      }
    }
  }

  "Collection.asFile" should {

    "handle simple name" in {
      Collection("db", "foo").asFile must_==
        rootDir </> dir("db") </> file("foo")
    }

    "handle simple path" in {
      Collection("db", "foo.bar").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar")
    }

    "preserve space" in {
      Collection("db", "foo.bar baz").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar baz")
    }

    "unescape leading '.'" in {
      Collection("db", "\\.hidden").asFile must_==
        rootDir </> dir("db") </> file(".hidden")
    }

    "unescape '$'" in {
      Collection("db", "foo\\d").asFile must_==
        rootDir </> dir("db") </> file("foo$")
    }

    "unescape '\\'" in {
      Collection("db", "foo\\\\bar").asFile must_==
        rootDir </> dir("db") </> file("foo\\bar")
    }

    "unescape '.' with path separators" in {
      Collection("db", "foo.bar\\.baz").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar.baz")
    }

    "ignore slash" in {
      Collection("db", "foo/bar").asFile must_==
        rootDir </> dir("db") </> file("foo/bar")
    }

    "ignore unrecognized escape in database name" in {
      Collection("%foo", "bar").asFile must_==
        rootDir </> dir("%foo") </> file("bar")
    }

    "not explode on empty collection name" in {
      Collection("foo", "").asFile must_==
        rootDir </> dir("foo") </> file("")
    }
  }

  "dbName <-> dirName are inverses" ! prop { db: SpecialStr =>
    Collection.dbNameFromPath(rootDir </> dir(db.str))
      .map(Collection.dirNameFromDbName(_)) must beRightDisjunction(DirName(db.str))
  }.set(maxSize = 20)
}
