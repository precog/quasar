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
import quasar.fp._, PathyCodecJson._
import quasar.fp.free._
import quasar.fs._, PathArbitrary._
import quasar.fs.mount._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.specs2.specification.core.Fragments
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.scalaz.ScalazMatchers._
import pathy.Path, Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class MountServiceSpec extends Specification with ScalaCheck with Http4s with PathUtils {
  import quasar.fs.mount.ViewMounterSpec._
  import posixCodec.printPath
  import PathError._

  val StubFs = FileSystemType("stub")

  type Eff0[A] = Coproduct[Mounting, MountConfigs, A]
  type Eff[A]  = Coproduct[Task, Eff0, A]

  val M = Mounting.Ops[Eff]

  type HttpSvc = Request => M.F[Response]

  def runTest[R: org.specs2.execute.AsResult](f: HttpSvc => M.F[R]): R = {
    type MEff[A] = Coproduct[Task, MountConfigs, A]

    TaskRef(Map[APath, MountConfig]()).flatMap { ref =>

      val mounter: Mounting ~> Free[MEff, ?] = Mounter[Task, MEff](
        {
          case MountRequest.MountFileSystem(_, typ, uri @ ConnectionUri("invalid")) =>
            Task.now(MountingError.invalidConfig(MountConfig.fileSystemConfig(typ, uri),
              "invalid connectionUri (simulated)".wrapNel).left)
          case _ =>
            Task.now(().right)
        },
        κ(Task.now(())))

      val store: MountConfigs ~> Task = KeyValueStore.fromTaskRef(ref)

      val mt: MEff ~> Task = NaturalTransformation.refl[Task] :+: store

      val tf: Mounting ~> Task = foldMapNT(mt) compose mounter

      def eff: Eff ~> Task =
        NaturalTransformation.refl[Task] :+: tf :+: store

      val service = mount.service[Eff].toHttpService(liftMT[Task, ResponseT] compose eff)

      f(service.run andThen (free.lift(_).into[Eff])).foldMap(eff)
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
              _    <- M.mountFileSystem(d, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(uri = pathUri(d)))
            } yield {
              resp.as[Json].unsafePerformSync must_== Json("stub" -> Json("connectionUri" := "foo"))
              resp.status must_== Status.Ok
            }
          }
        }
      }

      "succeed with correct, problematic filesystem path" in {
        // NB: the trailing '%' is what breaks http4s
        val d = rootDir </> dir("a.b c/d\ne%f%")

        runTest { service =>
          for {
            _    <- M.mountFileSystem(d, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

            resp <- service(Request(uri = pathUri(d)))
          } yield {
            resp.as[Json].unsafePerformSync must_== Json("stub" -> Json("connectionUri" := "foo"))
            resp.status must_== Status.Ok
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

              resp <- service(Request(uri = pathUri(f)))
            } yield {
              resp.as[Json].unsafePerformSync must_== cfgStr
              resp.status must_== Status.Ok
            }
          }
        }
      }

      "be 404 with missing mount (dir)" ! prop { d: APath =>
        runTest { service =>
          for {
            resp <- service(Request(uri = pathUri(d)))
          } yield {
            resp.as[ApiError].unsafePerformSync must beMountNotFoundError(d)
          }
        }
      }

      "be 404 with path type mismatch" ! prop { fp: AFile =>
        runTest { service =>
          val dp = fileParent(fp) </> dir(fileName(fp).value)

          for {
            _    <- M.mountFileSystem(dp, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

            resp <- service(Request(uri = pathUri(fp)))
          } yield {
            resp.as[ApiError].unsafePerformSync must beMountNotFoundError(fp)
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
              _    <- M.mountFileSystem(src, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(
                method = MOVE,
                uri = pathUri(src),
                headers = Headers(destination(dst))))

              srcAfter <- M.lookup(src).run
              dstAfter <- M.lookup(dst).run
            } yield {
              resp.as[String].unsafePerformSync must_== s"moved ${printPath(src)} to ${printPath(dst)}"
              resp.status must_== Status.Ok

              srcAfter must beNone
              dstAfter must beSome(MountConfig.fileSystemConfig(StubFs, ConnectionUri("foo")))
            }
          }
        }
      }

      "be 404 with missing source" ! prop { (src: ADir, dst: ADir) =>
        runTest { service =>
          for {
            resp <- service(Request(
              method = MOVE,
              uri = pathUri(src),
              headers = Headers(destination(dst))))
          } yield {
            resp.as[ApiError].unsafePerformSync must beApiErrorLike(pathNotFound(src))
          }
        }
      }

      "be 400 with no specified Destination" ! prop { (src: ADir) =>
        !hasDot(src) ==> {
          runTest { service =>
            for {
              _    <- M.mountFileSystem(src, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(
                method = MOVE,
                uri = pathUri(src)))
            } yield {
              resp.as[ApiError].unsafePerformSync must beHeaderMissingError("Destination")
            }
          }
        }
      }

      "be 400 with relative path destination" ! prop { (src: ADir, dst: RDir) =>
        !hasDot(src) ==> {
          runTest { service =>
            for {
              _    <- M.mountFileSystem(src, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(
                method = MOVE,
                uri = pathUri(src),
                headers = Headers(destination(dst))))
            } yield {
              resp.as[ApiError].unsafePerformSync must equal(ApiError.apiError(
                Status.BadRequest withReason "Expected an absolute directory.",
                "path" := dst))
            }
          }
        }
      }

      "be 400 with non-directory path destination" ! prop { (src: ADir, dst: AFile) =>
        !hasDot(src) ==> {
          runTest { service =>
            for {
              _    <- M.mountFileSystem(src, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(
                method = MOVE,
                uri = pathUri(src),
                headers = Headers(destination(dst))))
            } yield {
              resp.as[ApiError].unsafePerformSync must equal(ApiError.apiError(
                Status.BadRequest withReason "Expected an absolute directory.",
                "path" := dst))
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
        def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]): Request
      }

      def testBoth(test: RequestBuilder => Fragments) = {
        "POST" should {
          test(new RequestBuilder {
            def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]) =
              Request(
                method = POST,
                uri = pathUri(parent),
                headers = Headers(xFileName(mount)))
              .withBody(body).unsafePerformSync
            })
        }

        "PUT" should {
          test(new RequestBuilder {
            def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]) =
              Request(
                method = PUT,
                uri = pathUri(parent </> mount))
              .withBody(body).unsafePerformSync
          })
        }
      }

      testBoth { reqBuilder =>
        "succeed with filesystem path" ! prop { (parent: ADir, fsDir: RDir) =>
          !hasDot(parent </> fsDir) ==> {
            runTest { service =>
              for {
                resp  <- service(reqBuilder(parent, fsDir, """{"stub": { "connectionUri": "foo" } }"""))

                after <- M.lookup(parent </> fsDir).run
              } yield {
                resp.as[String].unsafePerformSync must_== s"added ${printPath(parent </> fsDir)}"
                resp.status must_== Status.Ok

                after must beSome(MountConfig.fileSystemConfig(StubFs, ConnectionUri("foo")))
              }
            }
          }
        }

        "succeed with view path" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(cfg))

              for {
                resp  <- service(reqBuilder(parent, f, cfgStr))

                after <- M.lookup(parent </> f).run
              } yield {
                resp.as[String].unsafePerformSync must_== s"added ${printPath(parent </> f)}"
                resp.status must_== Status.Ok

                after must beSome(MountConfig.viewConfig(cfg))
              }
            }
          }
        }

        "succeed with view under existing fs path" ! prop { (fs: ADir, viewSuffix: RFile) =>
          !hasDot(fs </> viewSuffix) ==> {
            runTest { service =>
              val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(cfg))

              val view = fs </> viewSuffix

              for {
                _         <- M.mountFileSystem(fs, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

                resp      <- service(reqBuilder(fs, viewSuffix, cfgStr))

                afterFs   <- M.lookup(fs).run
                afterView <- M.lookup(view).run
              } yield {
                resp.as[String].unsafePerformSync must_== s"added ${printPath(view)}"
                resp.status must_== Status.Ok

                afterFs must beSome
                afterView must beSome(MountConfig.viewConfig(cfg))
              }
            }
          }
        }

        "succeed with view 'above' existing fs path" ! prop { (d: ADir, view: RFile, fsSuffix: RDir) =>
          !hasDot(d </> view) ==> {
            runTest { service =>
              val fsCfg = ()

              val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(cfg))

              val fs = d </> posixCodec.parseRelDir(posixCodec.printPath(view) + "/").flatMap(sandbox(currentDir, _)).get </> fsSuffix

              for {
                _     <- M.mountFileSystem(fs,StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

                resp  <- service(reqBuilder(d, view, cfgStr))

                after <- M.lookup(d </> view).run
              } yield {
                resp.as[String].unsafePerformSync must_== s"added ${printPath(d </> view)}"
                resp.status must_== Status.Ok

                after must beSome(MountConfig.viewConfig(cfg))
              }
            }
          }
        }

        "be 409 with fs above existing fs path" ! prop { (d: ADir, fs: RDir, fsSuffix: RDir) =>
          (!identicalPath(fsSuffix, currentDir)) ==> {
            runTest { service =>
              val cfg = (StubFs, ConnectionUri("foo"))

              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.fileSystemConfig(cfg))

              val fs1 = d </> fs </> fsSuffix

              for {
                _     <- M.mountFileSystem(fs1, cfg._1, cfg._2).run.flatMap(orFailF)

                resp  <- service(reqBuilder(d, fs, cfgStr))

                after <- M.lookup(d </> fs).run
              } yield {
                resp.as[Json].unsafePerformSync must_== Json("error" := s"cannot mount at ${printPath(d </> fs)} because existing mount below: ${printPath(fs1)}")
                resp.status must_== Status.Conflict

                after must beNone
              }
            }
          }
        }.pendingUntilFixed("test harness does not yet detect conflicts")

        "be 400 with fs config and file path in X-File-Name header" ! prop { (parent: ADir, fsFile: RFile) =>
          runTest { service =>
            val cfg = (StubFs, ConnectionUri("foo"))

            for {
              resp <- service(reqBuilder(parent, fsFile, """{ "stub": { "connectionUri": "foo" } }"""))
            } yield {
              resp.as[ApiError].unsafePerformSync must beApiErrorWithMessage(
                Status.BadRequest withReason "Incorrect path type.",
                "path" := (parent </> fsFile))
            }
          }
        }

        "be 400 with view config and dir path in X-File-Name header" ! prop { (parent: ADir, viewDir: RDir) =>
          runTest { service =>
            val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
            val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(cfg))

            for {
              resp <- service(reqBuilder(parent, viewDir, cfgStr))
            } yield {
              resp.as[ApiError].unsafePerformSync must beApiErrorWithMessage(
                Status.BadRequest withReason "Incorrect path type.",
                "path" := (parent </> viewDir))
            }
          }
        }

        "be 400 with unbound variable in view" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              val cfg = viewConfig("select * from zips where pop < :cutoff")
              val cfgStr = EncodeJson.of[MountConfig].encode(MountConfig.viewConfig(cfg))

              for {
                resp <- service(reqBuilder(parent, f, cfgStr))
              } yield {
                resp.as[ApiError].unsafePerformSync must
                  beInvalidConfigError("There is no binding for the variable :cutoff")
              }
            }
          }
        }

        "be 400 with invalid JSON" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              for {
                resp <- service(reqBuilder(parent, f, "{"))
              } yield {
                resp.as[ApiError].unsafePerformSync must beApiErrorWithMessage(
                  Status.BadRequest withReason "Malformed input.")
              }
            }
          }
        }

        "be 400 with invalid connection uri" ! prop { (parent: ADir, d: RDir) =>
          !hasDot(parent </> d) ==> {
            runTest { service =>
              for {
                resp <- service(reqBuilder(parent, d, """{ "stub": { "connectionUri": "invalid" } }"""))
              } yield {
                resp.as[ApiError].unsafePerformSync must beInvalidConfigError("invalid connectionUri (simulated)")
              }
            }
          }
        }

        "be 400 with invalid view URI" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              for {
                resp <- service(reqBuilder(parent, f, """{ "view": { "connectionUri": "foo://bar" } }"""))
              } yield {
                resp.as[ApiError].unsafePerformSync must beApiErrorWithMessage(Status.BadRequest)
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
          val previousCfg = (StubFs, ConnectionUri("bar"))
          val mntPath = parent </> fsDir

          for {
            _    <- M.mountFileSystem(mntPath, previousCfg._1, previousCfg._2).run.flatMap(orFailF)

            resp <- service(Request(
                      method = POST,
                      uri = pathUri(parent),
                      headers = Headers(xFileName(fsDir)))
                    .withBody("""{ "stub": { "connectionUri": "foo" } }""").unsafePerformSync)

            after <- M.lookup(mntPath).run
          } yield {
            resp.as[ApiError].unsafePerformSync must beApiErrorLike(pathExists(mntPath))
            after must beSome(MountConfig.fileSystemConfig(previousCfg))
          }
        }
      }

      "be 400 with missing X-File-Name header" ! prop { (parent: ADir) =>
        !hasDot(parent) ==> {
          runTest { service =>
            for {
              resp <- service(Request(
                        method = POST,
                        uri = pathUri(parent))
                      .withBody("""{ "stub": { "connectionUri": "foo" } }""").unsafePerformSync)
            } yield {
              resp.as[ApiError].unsafePerformSync must beHeaderMissingError("X-File-Name")
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
            val previousCfg = (StubFs, ConnectionUri("bar"))
            val cfg = (StubFs, ConnectionUri("foo"))

            for {
              _    <- M.mountFileSystem(fsDir, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(
                        method = PUT,
                        uri = pathUri(fsDir))
                      .withBody("""{ "stub": { "connectionUri": "foo" } }""").unsafePerformSync)

              after <- M.lookup(fsDir).run
            } yield {
              resp.as[String].unsafePerformSync must_== s"updated ${printPath(fsDir)}"
              resp.status must_== Status.Ok

              after must beSome(MountConfig.fileSystemConfig(cfg))
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
              _     <- M.mountFileSystem(d, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp  <- service(Request(
                        method = DELETE,
                        uri = pathUri(d)))

              after <- M.lookup(d).run
            } yield {
              resp.as[String].unsafePerformSync must_== s"deleted ${printPath(d)}"
              resp.status must_== Status.Ok

              after must beNone
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

              resp  <- service(Request(
                        method = DELETE,
                        uri = pathUri(f)))

              after <- M.lookup(f).run
            } yield {
              resp.status must_== Status.Ok
              resp.as[String].unsafePerformSync must_== s"deleted ${printPath(f)}"

              after must beNone
            }
          }
        }
      }

      "be 404 with missing path" ! prop { p: APath =>
        runTest { service =>
          for {
            resp <- service(Request(method = DELETE, uri = pathUri(p)))
          } yield {
            resp.as[ApiError].unsafePerformSync must beApiErrorLike(pathNotFound(p))
          }
        }
      }
    }
  }
}
