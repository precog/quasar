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

package quasar.api.services

import quasar.Predef._
import quasar.api._
import quasar.api.matchers._
import quasar.api.ApiErrorEntityDecoder._
import quasar.effect.KeyValueStore
import quasar.fp._
import quasar.fp.free._
import quasar.fs._, PathArbitrary._
import quasar.fs.mount.{MountRequest => MR, _}

import argonaut._, Argonaut._
import org.http4s._, Status._
import org.http4s.argonaut._
import org.specs2.specification.core.Fragments
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.scalaz.ScalazMatchers._
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class MountServiceSpec extends Specification with ScalaCheck with Http4s with PathUtils {
  import quasar.fs.mount.ViewMounterSpec._
  import posixCodec.printPath
  import PathError._

  type Eff[A]  = Coproduct[Task, Mounting, A]

  type Mounted = Set[MR]
  type TestSvc = Request => Free[Eff, (Response, Mounted)]

  val StubFs = FileSystemType("stub")
  val fooUri = ConnectionUri("foo")
  val barUri = ConnectionUri("foo")
  val invalidUri = ConnectionUri("invalid")

  val M = Mounting.Ops[Eff]

  def runTest[A](f: TestSvc => Free[Eff, A]): A = {
    type MEff[A] = Coproduct[Task, MountConfigs, A]

    (TaskRef(Set.empty[MR]) |@| TaskRef(Map.empty[APath, MountConfig]))
      .tupled.flatMap { case (mountedRef, configsRef) =>

      val mounter: Mounting ~> Free[MEff, ?] = Mounter[Task, MEff](
        {
          case MR.MountFileSystem(_, typ, `invalidUri`) =>
            MountingError.invalidConfig(
              MountConfig.fileSystemConfig(typ, invalidUri),
              "invalid connectionUri (simulated)".wrapNel
            ).left.point[Task]

          case mntReq =>
            mountedRef.modify(_ + mntReq).void map \/.right
        },
        mntReq => mountedRef.modify(_ - mntReq).void
      )

      val store: MountConfigs ~> Task = KeyValueStore.fromTaskRef(configsRef)
      val mt: MEff ~> Task = NaturalTransformation.refl[Task] :+: store
      val tf: Mounting ~> Task = foldMapNT(mt) compose mounter
      def eff: Eff ~> Task = NaturalTransformation.refl[Task] :+: tf

      val service = mount.service[Eff].toHttpService(liftMT[Task, ResponseT] compose eff)

      val testSvc: TestSvc =
        req => injectFT[Task, Eff] apply (service(req) flatMap (mountedRef.read strengthL _))

      f(testSvc) foldMap eff
    }.unsafePerformSync
  }

  def orFail[A](v: MountingError \/ A): Task[A] =
    Task.fromDisjunction(v.leftMap(e => new RuntimeException(e.shows)))

  def orFailF[A](v: MountingError \/ A): M.F[A] =
    free.lift(orFail(v)).into[Eff]

  def beMountNotFoundError(path: APath) =
    beApiErrorWithMessage(
      Status.NotFound withReason "Mount point not found.",
      "path" := path)

  def beInvalidConfigError(rsn: String) =
    equal(ApiError.apiError(
      Status.BadRequest withReason "Invalid mount configuration.",
      "reasons" := List(rsn)))

  "Mount Service" should {
    "GET" should {
      "succeed with correct filesystem path" ! prop { d: ADir =>
        !hasDot(d) ==> {
          runTest { service =>
            for {
              _    <- M.mountFileSystem(d, StubFs, ConnectionUri("foo"))
                        .run.flatMap(orFailF)
              r    <- service(Request(uri = pathUri(d)))
              (res, _) = r
              body <- lift(res.as[Json]).into[Eff]
            } yield {
              (body must_== Json("stub" -> Json("connectionUri" := "foo"))) and
              (res.status must_== Ok)
            }
          }
        }
      }

      "succeed with correct, problematic filesystem path" in {
        // NB: the trailing '%' is what breaks http4s
        val d = rootDir </> dir("a.b c/d\ne%f%")

        runTest { service =>
          for {
            _    <- M.mountFileSystem(d, StubFs, ConnectionUri("foo"))
                      .run.flatMap(orFailF)
            r    <- service(Request(uri = pathUri(d)))
            (res, _) = r
            body <- lift(res.as[Json]).into[Eff]
          } yield {
            (body must_== Json("stub" -> Json("connectionUri" := "foo"))) and
            (res.status must_== Ok)
          }
        }
      }

      "succeed with correct view path" ! prop { f: AFile =>
        !hasDot(f) ==> {
          runTest { service =>
            val cfg = viewConfig("select * from zips where pop > :cutoff", "cutoff" -> "1000")
            val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(cfg))

            for {
              _    <- M.mountView(f, cfg._1, cfg._2).run.flatMap(orFailF)

              r    <- service(Request(uri = pathUri(f)))
              (res, _) = r
              body <- lift(res.as[Json]).into[Eff]
            } yield {
              (body must_== cfgStr) and (res.status must_== Ok)
            }
          }
        }
      }

      "be 404 with missing mount (dir)" ! prop { d: APath =>
        runTest { service =>
          for {
            r   <- service(Request(uri = pathUri(d)))
            (res, _) = r
            err <- lift(res.as[ApiError]).into[Eff]
          } yield {
            err must beMountNotFoundError(d)
          }
        }
      }

      "be 404 with path type mismatch" ! prop { fp: AFile =>
        runTest { service =>
          val dp = fileParent(fp) </> dir(fileName(fp).value)

          for {
            _   <- M.mountFileSystem(dp, StubFs, ConnectionUri("foo"))
                     .run.flatMap(orFailF)
            r   <- service(Request(uri = pathUri(fp)))
            (res, _) = r
            err <- lift(res.as[ApiError]).into[Eff]
          } yield {
            err must beMountNotFoundError(fp)
          }
        }
      }
    }

    "MOVE" should {
      import org.http4s.Method.MOVE

      def destination(p: pathy.Path[_, _, Sandboxed]) = Header(Destination.name.value, UriPathCodec.printPath(p))

      "succeed with filesystem mount" ! prop { (srcHead: String, srcTail: RDir, dstHead: String, dstTail: RDir) =>
        // NB: distinct first segments means no possible conflict, but doesn't
        // hit every possible scenario.
        (srcHead != "" && dstHead != "" && srcHead != dstHead && !hasDot(rootDir </> dir(srcHead) </> srcTail)) ==> {
          runTest { service =>
            val src = rootDir </> dir(srcHead) </> srcTail
            val dst = rootDir </> dir(dstHead) </> dstTail
            for {
              _        <- M.mountFileSystem(src, StubFs, fooUri)
                            .run.flatMap(orFailF)

              r        <- service(Request(
                            method = MOVE,
                            uri = pathUri(src),
                            headers = Headers(destination(dst))))

              (res, mntd) = r
              body     <- lift(res.as[String]).into[Eff]

              srcAfter <- M.lookup(src).run
              dstAfter <- M.lookup(dst).run
            } yield {
              (body must_== s"moved ${printPath(src)} to ${printPath(dst)}") and
              (res.status must_== Ok)                                        and
              (mntd must_== Set(MR.mountFileSystem(dst, StubFs, fooUri)))    and
              (srcAfter must beNone)                                         and
              (dstAfter must beSome(MountConfig.fileSystemConfig(StubFs, fooUri)))
            }
          }
        }
      }

      "be 404 with missing source" ! prop { (src: ADir, dst: ADir) =>
        runTest { service =>
          for {
            r   <- service(Request(
                     method = MOVE,
                     uri = pathUri(src),
                     headers = Headers(destination(dst))))

            (res, mntd) = r
            err <- lift(res.as[ApiError]).into[Eff]
          } yield {
            (err must beApiErrorLike(pathNotFound(src))) and
            (mntd must beEmpty)
          }
        }
      }

      "be 400 with no specified Destination" ! prop { (src: ADir) =>
        !hasDot(src) ==> {
          runTest { service =>
            for {
              _   <- M.mountFileSystem(src, StubFs, fooUri)
                       .run.flatMap(orFailF)

              r   <- service(Request(
                        method = MOVE,
                        uri = pathUri(src)))

              (res, mntd) = r
              err <- lift(res.as[ApiError]).into[Eff]
            } yield {
              (err must beHeaderMissingError("Destination")) and
              (mntd must_== Set(MR.mountFileSystem(src, StubFs, fooUri)))
            }
          }
        }
      }

      "be 400 with relative path destination" ! prop { (src: ADir, dst: RDir) =>
        !hasDot(src) ==> {
          runTest { service =>
            for {
              _   <- M.mountFileSystem(src, StubFs, fooUri)
                       .run.flatMap(orFailF)

              r   <- service(Request(
                       method = MOVE,
                       uri = pathUri(src),
                       headers = Headers(destination(dst))))

              (res, mntd) = r
              err <- lift(res.as[ApiError]).into[Eff]
            } yield {
              (err must equal(ApiError.apiError(
                Status.BadRequest withReason "Expected an absolute directory.",
                "path" := dst))) and
              (mntd must_== Set(MR.mountFileSystem(src, StubFs, fooUri)))
            }
          }
        }
      }

      "be 400 with non-directory path destination" ! prop { (src: ADir, dst: AFile) =>
        !hasDot(src) ==> {
          runTest { service =>
            for {
              _   <- M.mountFileSystem(src, StubFs, fooUri)
                       .run.flatMap(orFailF)

              r   <- service(Request(
                       method = MOVE,
                       uri = pathUri(src),
                       headers = Headers(destination(dst))))

              (res, mntd) = r
              err <- lift(res.as[ApiError]).into[Eff]
            } yield {
              (err must equal(ApiError.apiError(
                Status.BadRequest withReason "Expected an absolute directory.",
                "path" := dst))) and
              (mntd must_== Set(MR.mountFileSystem(src, StubFs, fooUri)))
            }
          }
        }
      }
    }

    def xFileName(p: pathy.Path[_, _, Sandboxed]) = Header(XFileName.name.value, UriPathCodec.printPath(p))

    "Common" >> {
      import org.http4s.Method.POST
      import org.http4s.Method.PUT

      trait RequestBuilder {
        def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]): Free[Eff, Request]
      }

      def testBoth(test: RequestBuilder => Fragments) = {
        "POST" should {
          test(new RequestBuilder {
            def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]) =
              lift(Request(
                method = POST,
                uri = pathUri(parent),
                headers = Headers(xFileName(mount)))
              .withBody(body)).into[Eff]
            })
        }

        "PUT" should {
          test(new RequestBuilder {
            def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]) =
              lift(Request(
                method = PUT,
                uri = pathUri(parent </> mount))
              .withBody(body)).into[Eff]
          })
        }
      }

      testBoth { reqBuilder =>
        "succeed with filesystem path" ! prop { (parent: ADir, fsDir: RDir) =>
          !hasDot(parent </> fsDir) ==> {
            runTest { service =>
              for {
                req   <- reqBuilder(parent, fsDir, """{"stub": { "connectionUri": "foo" } }""")
                r     <- service(req)
                (res, mntd) = r
                body  <- lift(res.as[String]).into[Eff]
                dst   =  parent </> fsDir
                after <- M.lookup(dst).run
              } yield {
                (body must_== s"added ${printPath(dst)}")                   and
                (res.status must_== Ok)                                     and
                (mntd must_== Set(MR.mountFileSystem(dst, StubFs, fooUri))) and
                (after must beSome(MountConfig.fileSystemConfig(StubFs, fooUri)))
              }
            }
          }
        }

        "succeed with view path" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              val (expr, vars) = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(expr, vars))

              for {
                req   <- reqBuilder(parent, f, cfgStr)
                r     <- service(req)
                (res, mntd) = r
                body  <- lift(res.as[String]).into[Eff]
                dst   =  parent </> f
                after <- M.lookup(dst).run
              } yield {
                (body must_== s"added ${printPath(dst)}")         and
                (res.status must_== Ok)                           and
                (mntd must_== Set(MR.mountView(dst, expr, vars))) and
                (after must beSome(MountConfig.viewConfig(expr, vars)))
              }
            }
          }
        }

        "succeed with view under existing fs path" ! prop { (fs: ADir, viewSuffix: RFile) =>
          !hasDot(fs </> viewSuffix) ==> {
            runTest { service =>
              val (expr, vars) = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(expr, vars))

              val view = fs </> viewSuffix

              for {
                _         <- M.mountFileSystem(fs, StubFs, fooUri)
                               .run.flatMap(orFailF)

                req       <- reqBuilder(fs, viewSuffix, cfgStr)
                r         <- service(req)
                (res, mntd) = r
                body      <- lift(res.as[String]).into[Eff]

                afterFs   <- M.lookup(fs).run
                afterView <- M.lookup(view).run
              } yield {
                (body must_== s"added ${printPath(view)}") and
                (res.status must_== Ok)                    and
                (mntd must_== Set(
                  MR.mountFileSystem(fs, StubFs, fooUri),
                  MR.mountView(view, expr, vars)
                ))                                         and
                (afterFs must beSome)                      and
                (afterView must beSome(MountConfig.viewConfig(expr, vars)))
              }
            }
          }
        }

        "succeed with view 'above' existing fs path" ! prop { (d: ADir, view: RFile, fsSuffix: RDir) =>
          !hasDot(d </> view) ==> {
            runTest { service =>
              val (expr, vars) = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(expr, vars))

              val fs = d </> posixCodec.parseRelDir(posixCodec.printPath(view) + "/").flatMap(sandbox(currentDir, _)).get </> fsSuffix

              for {
                _     <- M.mountFileSystem(fs, StubFs, fooUri)
                           .run.flatMap(orFailF)

                req   <- reqBuilder(d, view, cfgStr)
                r     <- service(req)
                (res, mntd) = r
                body  <- lift(res.as[String]).into[Eff]
                vdst  =  d </> view
                after <- M.lookup(vdst).run
              } yield {
                (body must_== s"added ${printPath(vdst)}") and
                (res.status must_== Ok)                    and
                (mntd must_== Set(
                  MR.mountFileSystem(fs, StubFs, fooUri),
                  MR.mountView(vdst, expr, vars)
                ))                                         and
                (after must beSome(MountConfig.viewConfig(expr, vars)))
              }
            }
          }
        }

        "be 409 with fs above existing fs path" ! prop { (d: ADir, fs: RDir, fsSuffix: RDir) =>
          (!identicalPath(fsSuffix, currentDir)) ==> {
            runTest { service =>
              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.fileSystemConfig(StubFs, fooUri))
              val fs1 = d </> fs </> fsSuffix

              for {
                _     <- M.mountFileSystem(fs1, StubFs, fooUri)
                           .run.flatMap(orFailF)

                req   <- reqBuilder(d, fs, cfgStr)
                r     <- service(req)
                (res, mntd) = r
                jerr  <- lift(res.as[Json]).into[Eff]
                dst   =  d </> fs
                after <- M.lookup(dst).run
              } yield {
                (jerr must_== Json("error" := s"cannot mount at ${printPath(dst)} because existing mount below: ${printPath(fs1)}")) and
                (res.status must_== Conflict)                               and
                (mntd must_== Set(MR.mountFileSystem(fs1, StubFs, fooUri))) and
                (after must beNone)
              }
            }
          }
        }.pendingUntilFixed("test harness does not yet detect conflicts")

        "be 400 with fs config and file path in X-File-Name header" ! prop { (parent: ADir, fsFile: RFile) =>
          runTest { service =>
            for {
              req <- reqBuilder(parent, fsFile, """{ "stub": { "connectionUri": "foo" } }""")
              r   <- service(req)
              (res, mntd) = r
              err <- lift(res.as[ApiError]).into[Eff]
            } yield {
              (err must beApiErrorWithMessage(
                Status.BadRequest withReason "Incorrect path type.",
                "path" := (parent </> fsFile))) and
              (mntd must beEmpty)
            }
          }
        }

        "be 400 with view config and dir path in X-File-Name header" ! prop { (parent: ADir, viewDir: RDir) =>
          runTest { service =>
            val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
            val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(cfg))

            for {
              req <- reqBuilder(parent, viewDir, cfgStr)
              r   <- service(req)
              (res, mntd) = r
              err <- lift(res.as[ApiError]).into[Eff]
            } yield {
              (err must beApiErrorWithMessage(
                Status.BadRequest withReason "Incorrect path type.",
                "path" := (parent </> viewDir))) and
              (mntd must beEmpty)
            }
          }
        }

        "be 400 with invalid JSON" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              for {
                req <- reqBuilder(parent, f, "{")
                r   <- service(req)
                (res, mntd) = r
                err <- lift(res.as[ApiError]).into[Eff]
              } yield {
                (err must beApiErrorWithMessage(BadRequest withReason "Malformed input.")) and
                (mntd must beEmpty)
              }
            }
          }
        }

        "be 400 with invalid connection uri" ! prop { (parent: ADir, d: RDir) =>
          !hasDot(parent </> d) ==> {
            runTest { service =>
              for {
                req <- reqBuilder(parent, d, """{ "stub": { "connectionUri": "invalid" } }""")
                r   <- service(req)
                (res, mntd) = r
                err <- lift(res.as[ApiError]).into[Eff]
              } yield {
                (err must beInvalidConfigError("invalid connectionUri (simulated)")) and
                (mntd must beEmpty)
              }
            }
          }
        }

        "be 400 with invalid view URI" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              for {
                req <- reqBuilder(parent, f, """{ "view": { "connectionUri": "foo://bar" } }""")
                r   <- service(req)
                (res, mntd) = r
                err <- lift(res.as[ApiError]).into[Eff]
              } yield {
                (err must beApiErrorWithMessage(BadRequest)) and
                (mntd must beEmpty)
              }
            }
          }
        }
      }
    }

    "POST" should {
      import org.http4s.Method.POST

      "be 409 with existing filesystem path" ! prop { (parent: ADir, fsDir: RDir) =>
        runTest { service =>
          val mntPath = parent </> fsDir

          for {
            _     <- M.mountFileSystem(mntPath, StubFs, barUri)
                       .run.flatMap(orFailF)

            req   <- lift(Request(
                       method = POST,
                       uri = pathUri(parent),
                       headers = Headers(xFileName(fsDir)))
                     .withBody("""{ "stub": { "connectionUri": "foo" } }""")).into[Eff]

            r     <- service(req)
            (res, mntd) = r
            err   <- lift(res.as[ApiError]).into[Eff]

            after <- M.lookup(mntPath).run
          } yield {
            (err must beApiErrorLike(pathExists(mntPath)))                  and
            (mntd must_== Set(MR.mountFileSystem(mntPath, StubFs, barUri))) and
            (after must beSome(MountConfig.fileSystemConfig(StubFs, barUri)))
          }
        }
      }

      "be 400 with missing X-File-Name header" ! prop { (parent: ADir) =>
        !hasDot(parent) ==> {
          runTest { service =>
            for {
              req <- lift(Request(
                       method = POST,
                       uri = pathUri(parent))
                     .withBody("""{ "stub": { "connectionUri": "foo" } }""")).into[Eff]
              r   <- service(req)
              (res, mntd) = r
              err <- lift(res.as[ApiError]).into[Eff]
            } yield {
              (err must beHeaderMissingError("X-File-Name")) and
              (mntd must beEmpty)
            }
          }
        }
      }
    }

    "PUT" should {
      import org.http4s.Method.PUT

      "succeed with overwritten filesystem" ! prop { (fsDir: ADir) =>
        !hasDot(fsDir) ==> {
          runTest { service =>
            for {
              _     <- M.mountFileSystem(fsDir, StubFs, barUri)
                         .run.flatMap(orFailF)

              req   <- lift(Request(
                         method = PUT,
                         uri = pathUri(fsDir))
                       .withBody("""{ "stub": { "connectionUri": "foo" } }""")).into[Eff]

              r     <- service(req)
              (res, mntd) = r
              body  <- lift(res.as[String]).into[Eff]

              after <- M.lookup(fsDir).run
            } yield {
              (body must_== s"updated ${printPath(fsDir)}")                 and
              (res.status must_== Ok)                                       and
              (mntd must_== Set(MR.mountFileSystem(fsDir, StubFs, fooUri))) and
              (after must beSome(MountConfig.fileSystemConfig(StubFs, fooUri)))
            }
          }
        }
      }
    }

    "DELETE" should {
      import org.http4s.Method.DELETE

      "succeed with filesystem path" ! prop { (d: ADir) =>
        !hasDot(d) ==> {
          runTest { service =>
            for {
              _     <- M.mountFileSystem(d, StubFs, ConnectionUri("foo"))
                         .run.flatMap(orFailF)

              r     <- service(Request(
                         method = DELETE,
                         uri = pathUri(d)))
              (res, mntd) = r
              body  <- lift(res.as[String]).into[Eff]

              after <- M.lookup(d).run
            } yield {
              (body must_== s"deleted ${printPath(d)}") and
              (res.status must_== Ok)                   and
              (mntd must beEmpty)                       and
              (after must beNone)
            }
          }
        }
      }

      "succeed with view path" ! prop { (f: AFile) =>
        !hasDot(f) ==> {
          runTest { service =>
            val cfg = viewConfig("select * from zips where pop > :cutoff", "cutoff" -> "1000")

            for {
              _     <- M.mountView(f, cfg._1, cfg._2).run.flatMap(orFailF)

              r     <- service(Request(
                         method = DELETE,
                         uri = pathUri(f)))
              (res, mntd) = r
              body  <- lift(res.as[String]).into[Eff]

              after <- M.lookup(f).run
            } yield {
              (body must_== s"deleted ${printPath(f)}") and
              (res.status must_== Ok)                   and
              (mntd must beEmpty)                       and
              (after must beNone)
            }
          }
        }
      }

      "be 404 with missing path" ! prop { p: APath =>
        runTest { service =>
          for {
            r   <- service(Request(method = DELETE, uri = pathUri(p)))
            (res, mntd) = r
            err <- lift(res.as[ApiError]).into[Eff]
          } yield {
            (err must beApiErrorLike(pathNotFound(p))) and
            (mntd must beEmpty)
          }
        }
      }
    }
  }
}
