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
import quasar._, fp._, fs._, LogicalPlan._
import quasar.effect.KeyValueStore
import quasar.std.IdentityLib.Squash
import quasar.std.StdLib._, set.{InnerJoin, Take, Drop}
import quasar.sql, sql._

import matryoshka.Fix
import monocle.{Lens => MLens}
import org.specs2.mutable
import org.specs2.ScalaCheck
import org.specs2.scalaz.DisjunctionMatchers._
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

object ViewMounterSpec {
  def viewConfig(q: String, vars: (String, String)*): (Expr, Variables) =
    (
      parse(Query(q)).toOption.get,
      Variables(Map(vars.map { case (n, v) =>
        quasar.VarName(n) -> quasar.VarValue(v) }: _*))
    )
}

class ViewMounterSpec extends mutable.Specification with ScalaCheck with TreeMatchers {
  import MountingError._
  import ViewMounterSpec._

  type F[A]      = Free[MountConfigsF, A]
  type CS[A]     = State[Map[APath, MountConfig], A]
  type Res[A]    = (Map[APath, MountConfig], A)

  def eval(m: Map[APath, MountConfig]): F ~> Res = new (F ~> Res) {
    def apply[A](fa: F[A]): Res[A] = {
      def mccs: MountConfigs ~> CS =
        KeyValueStore.toState[State[Map[APath, MountConfig], ?]](MLens.id[Map[APath, MountConfig]])
      hoistFree[MountConfigsF, CS](Coyoneda.liftTF[MountConfigs, CS](mccs)).apply(fa).run(m)
    }
  }

  "mounting views" >> {
    "fails with InvalidConfig when compilation fails" >> {
      val fnDNE = Fix(sql.InvokeFunctionF("DNE", List[sql.Expr]()))
      val f     = rootDir </> dir("mnt") </> file("dne")

      eval(Map.empty)(ViewMounter.mount[MountConfigsF](f, fnDNE, Variables.empty))
        ._2 must beLike {
          case -\/(InvalidConfig(_, _)) => ok
        }
    }

    "updates mounted views with compiled plan when compilation succeeds" >> {
      val selStar = Fix(sql.SelectF(
        sql.SelectAll,
        Nil,
        Some(sql.TableRelationAST[sql.Expr](rootDir </> dir("foo") </> file("bar"), None)),
        None, None, None))

      val f = rootDir </> dir("mnt") </> file("selectStar")

      eval(Map.empty)(ViewMounter.mount[MountConfigsF](f, selStar, Variables.empty))
        ._1.get(f) must beSome
    }
  }

  "unmounting views" >> {
    "removes plan from mounted views" >> {
      val f  = rootDir </> dir("mnt") </> file("foo")

      eval(
        Map(f -> MountConfig.viewConfig(viewConfig("select * from zips")))
        )(
        ViewMounter.unmount[MountConfigsF](f)
        )._1 must beEmpty
    }
  }

  "lookup" >> {
    "trivial read with relative path" >> {
      val f = rootDir[Sandboxed] </> dir("foo") </> file("justZips")
      val vc = viewConfig("select * from zips")
      val vs = Map[APath, MountConfig](f -> MountConfig.viewConfig(vc))

      eval(vs)(ViewMounter.lookup[MountConfigsF](f).run)._2 must beSome(vc)
    }
  }

  "rewrite" >> {
    "no match" >> {
      eval(Map())(ViewMounter.rewrite[MountConfigsF](Read(rootDir </> file("zips"))).run)
        ._2  must beRightDisjunction.like { case r => r must beTree(Read(rootDir </> file("zips"))) }
    }

    "trivial read" >> {
      val p = rootDir </> dir("view") </> file("justZips")
      val vs = Map[APath, MountConfig](
        p -> MountConfig.viewConfig(viewConfig("select * from `/zips`")))

      eval(vs)(ViewMounter.rewrite[MountConfigsF](Read(p)).run)
        ._2 must beRightDisjunction.like { case r => r must beTree(
          Fix(Squash(Read(rootDir </> file("zips"))))
        )}
    }

    "trivial read with relative path" >> {
      val p = rootDir </> dir("foo") </> file("justZips")
      val vs = Map[APath, MountConfig](
        p -> MountConfig.viewConfig(viewConfig("select * from zips")))

      eval(vs)(ViewMounter.rewrite[MountConfigsF](Read(p)).run)
        ._2 must beRightDisjunction.like { case r => r must beTree(
          Fix(Squash(Read(rootDir </> dir("foo") </> file("zips"))))
        )}
    }

    "non-trivial" >> {
      val inner = viewConfig("select city, state from `/zips` order by state")

      val p = rootDir </> dir("view") </> file("simpleZips")

      val outer =
        Fix(Take(
          Fix(Drop(
            Read(p),
            Constant(Data.Int(5)))),
          Constant(Data.Int(10))))

      val innerLP = (quasar.queryPlan _).tupled(inner).run.run._2.toOption.get

      val vs = Map[APath, MountConfig](
        p -> MountConfig.viewConfig(inner))

      eval(vs)(ViewMounter.rewrite[MountConfigsF](outer).run)
        ._2 must beRightDisjunction.like { case r => r must beTree(
          Fix(Take(
            Fix(Drop(
              innerLP,
              Constant(Data.Int(5)))),
            Constant(Data.Int(10)))))
        }
    }

    "multi-level" >> {
      val vs = Map[APath, MountConfig](
        (rootDir </> dir("view") </> file("view1")) ->
          MountConfig.viewConfig(viewConfig("select * from `/zips`")),
        (rootDir </> dir("view") </> file("view2")) ->
          MountConfig.viewConfig(viewConfig("select * from view1")))

      eval(vs)(ViewMounter.rewrite[MountConfigsF](Read(rootDir </> dir("view") </> file("view2"))).run)
        ._2 must beRightDisjunction.like { case r => r must beTree(
          Fix(Squash(Fix(Squash(Read(rootDir </> file("zips")))))))
        }
    }


    // Several tests for edge cases with view references:

    "multiple references" >> {
      // NB: joining a view to itself means two expanded reads. The main point is
      // that these references should not be mistaken for a circular reference.

      val vp = rootDir </> dir("view") </> file("view1")
      val zp = rootDir </> file("zips")

      val vs = Map[APath, MountConfig](
        vp -> MountConfig.viewConfig(viewConfig("select * from `/zips`")))

      val q = Fix(InnerJoin(
        Read(vp),
        Read(vp),
        Constant(Data.Bool(true))))

      val exp = Fix(InnerJoin(
        Fix(Squash(Read(zp))),
        Fix(Squash(Read(zp))),
        Constant(Data.Bool(true))))

      eval(vs)(ViewMounter.rewrite[MountConfigsF](q).run)
        ._2 must beRightDisjunction.like { case r => r must beTree(exp) }
    }

    "self reference" >> {
      // NB: resolves to a read on the underlying collection, allowing a view
      // to act like a filter or decorator for an existing collection.

      val p = rootDir </> dir("foo") </> file("bar")

      val q = viewConfig(s"select * from `${posixCodec.printPath(p)}` limit 10")

      val qlp = (quasar.queryPlan _).tupled(q).run.run._2.toOption.get

      val vs = Map[APath, MountConfig](p -> MountConfig.viewConfig(q))

      eval(vs)(ViewMounter.lookup[MountConfigsF](p).run)._2 must beSome(q)

      eval(vs)(ViewMounter.rewrite[MountConfigsF](Read(p)).run)
        ._2 must beRightDisjunction.like { case r => r must beTree(qlp) }
    }

    "circular reference" >> {
      // NB: this situation probably results from user error, but since this is
      // now the _only_ way the view definitions can be ill-formed, it seems
      // like a shame to introduce `\/` just to handle this case. Instead,
      // the inner reference is treated the same way as self-references, and
      // left un-expanded. That means the user will see an error when the query
      // is evaluated and there turns out to be no actual file called "view2".

      val v1p = rootDir </> dir("view") </> file("view1")
      val v2p = rootDir </> dir("view") </> file("view2")

      val vs = Map[APath, MountConfig](
        v1p -> MountConfig.viewConfig(
          viewConfig(s"select * from `${posixCodec.printPath(v2p)}` offset 5")),
        v2p -> MountConfig.viewConfig(
          viewConfig(s"select * from `${posixCodec.printPath(v1p)}` limit 10")))

      eval(vs)(ViewMounter.rewrite[MountConfigsF](Read(v2p)).run)
        ._2 must beRightDisjunction.like { case r => r must beTree(
           Fix(Take(
             Fix(Squash(Fix(Drop(
               Fix(Squash(Read(v2p))),
               Constant(Data.Int(5)))))),
             Constant(Data.Int(10))))
        )}

    }
  }

  "ls" >> {
    "be empty" ! prop { (dir: ADir) =>
      val vs = Map[APath, MountConfig]()

      eval(vs)(ViewMounter.ls[MountConfigsF](dir))
        ._2 must_== Set()
    }

    "list view under its parent dir" ! prop { (path: AFile) =>
      val vs = Map[APath, MountConfig](
        path -> MountConfig.viewConfig(viewConfig("select * from `/foo`")))

      eval(vs)(ViewMounter.ls[MountConfigsF](fileParent(path)))
        ._2 must_== Set(fileName(path).right)
    }

    "list view parent under grand-parent dir" ! prop { (dir: ADir) =>
      (dir ≠ rootDir) ==> {
        val parent = parentDir(dir).get
        val vs = Map[APath, MountConfig](
          (dir </> file("view1")) -> MountConfig.viewConfig(viewConfig("select * from `/foo`")))

        eval(vs)(ViewMounter.ls[MountConfigsF](parent))
          ._2 must_== Set(dirName(dir).get.left)
      }
    }

  }
}
