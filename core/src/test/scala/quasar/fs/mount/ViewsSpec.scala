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

import quasar._, LogicalPlan.{Free => LPFree, _}
import quasar.fp._
import quasar.fs._
import quasar.std.StdLib._, set._, structural._

import matryoshka._
import org.specs2.mutable._
import org.specs2.ScalaCheck
import pathy.{Path => PPath}, PPath._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

class ViewsSpec extends Specification with ScalaCheck with TreeMatchers {
  "lookup" should {
    "trivial read with relative path" in {
      val vs = Views(Map(
        (rootDir </> dir("foo") </> file("justZips")) ->
          Read(file("zips"))))

      vs.lookup(rootDir </> dir("foo") </> file("justZips")) must beSome(beTree(
        Read(rootDir[Sandboxed] </> dir("foo") </> file("zips"))))
    }
  }

  "rewrite" should {
    "no match" in {
      val vs = Views(Map())
      vs.rewrite(Read(rootDir </> file("zips"))) must beTree(
        Read(rootDir </> file("zips")))
    }

    "trivial read" in {
      val vs = Views(Map(
        (rootDir </> dir("view") </> file("justZips")) ->
          Read(rootDir </> file("zips"))))

      vs.rewrite(Read(rootDir </> dir("view") </> file("justZips"))) must beTree(
        Read(rootDir </> file("zips")))
    }

    "trivial read with relative path" in {
      val vs = Views(Map(
        (rootDir </> dir("foo") </> file("justZips")) ->
          Read(file("zips"))))

      vs.rewrite(Read(rootDir </> dir("foo") </> file("justZips"))) must beTree(
        Read(rootDir </> dir("foo") </> file("zips")))
    }

    "non-trivial" in {
      val inner =
        Let('tmp0, Read(rootDir </> file("zips")),
          Fix(MakeObjectN(
            Constant(Data.Str("city")) ->
              Fix(ObjectProject(LPFree('tmp0), Constant(Data.Str("city")))),
            Constant(Data.Str("state")) ->
              Fix(ObjectProject(LPFree('tmp0), Constant(Data.Str("state")))))))
      val outer =
        Fix(Take(
          Fix(Drop(
            Read(rootDir </> dir("view") </> file("simpleZips")),
            Constant(Data.Int(5)))),
          Constant(Data.Int(10))))
      val vs = Views(Map(
        (rootDir </> dir("view") </> file("simpleZips")) -> inner))

      vs.rewrite(outer) must beTree(
        Fix(Take(
          Fix(Drop(
            Let('tmp0, Read(rootDir </> file("zips")),
              Fix(MakeObjectN(
                Constant(Data.Str("city")) ->
                  Fix(ObjectProject(LPFree('tmp0), Constant(Data.Str("city")))),
                Constant(Data.Str("state")) ->
                  Fix(ObjectProject(LPFree('tmp0), Constant(Data.Str("state"))))))),
            Constant(Data.Int(5)))),
          Constant(Data.Int(10)))))
    }

    "multi-level" in {
      val vs = Views(Map(
        (rootDir </> dir("view") </> file("view1")) ->
          Read(rootDir </> file("zips")),
        (rootDir </> dir("view") </> file("view2")) ->
          Read(file("view1"))))

      vs.rewrite(Read(rootDir </> dir("view") </> file("view2"))) must beTree(
        Read(rootDir </> file("zips")))
    }


    // Several tests for edge cases with view references:

    "multiple references" in {
      // NB: joining a view to itself means two expanded reads. The main point is
      // that these references should not be mistaken for a circular reference.

      val vs = Views(Map(
        (rootDir </> dir("view") </> file("view1")) ->
          Read(rootDir </> file("zips"))))

      val q = Fix(InnerJoin(
        Read(rootDir </> dir("view") </> file("view1")),
        Read(rootDir </> dir("view") </> file("view1")),
        Constant(Data.Bool(true))))

      val exp = Fix(InnerJoin(
        Read(rootDir </> file("zips")),
        Read(rootDir </> file("zips")),
        Constant(Data.Bool(true))))

      vs.rewrite(q) must beTree(exp)
    }

    "self reference" in {
      // NB: resolves to a read on the underlying collection, allowing a view
      // to act like a filter or decorator for an existing collection.

      val p = rootDir </> dir("foo") </> file("bar")
      val q = Fix(Take(Read(p), Constant(Data.Int(10))))
      val vs = Views(Map(p -> q))

      vs.lookup(p) must beSome(q)
      vs.rewrite(Read(rootDir </> dir("foo") </> file("bar"))) must beTree(q)
    }

    "circular reference" in {
      // NB: this situation probably results from user error, but since this is
      // now the _only_ way the view definitions can be ill-formed, it seems
      // like a shame to introduce `\/` just to handle this case. Instead,
      // the inner reference is treated the same way as self-references, and
      // left un-expanded. That means the user will see an error when the query
      // is evaluated and there turns out to be no actual file called "view2".

      val vs = Views(Map(
        (rootDir </> dir("view") </> file("view1")) ->
          Fix(Drop(Read(file("view2")), Constant(Data.Int(5)))),
        (rootDir </> dir("view") </> file("view2")) ->
          Fix(Take(Read(file("view1")), Constant(Data.Int(10))))))

      vs.rewrite(Read(rootDir </> dir("view") </> file("view2"))) must beTree(
        Fix(Take(
          Fix(Drop(
            Read(rootDir </> dir("view") </> file("view2")),
            Constant(Data.Int(5)))),
          Constant(Data.Int(10)))))
    }
  }

  "ls" should {
    "be empty" ! prop { (dir: ADir) =>
      val views = Views(Map())
      views.ls(dir) must_== Set()
    }

    "list view under its parent dir" ! prop { (path: AFile) =>
      val views = Views(Map(path -> Read(rootDir </> file("foo"))))
      views.ls(fileParent(path)) must_== Set(fileName(path).right)
    }

    "list view parent under grand-parent dir" ! prop { (dir: ADir) =>
      (dir ≠ rootDir) ==> {
        val parent = parentDir(dir).get
        val views = Views(Map((dir </> file("view1")) -> Read(rootDir </> file("foo"))))
        views.ls(parent) must_== Set(dirName(dir).get.left)
      }
    }
  }
}
