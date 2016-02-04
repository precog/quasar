package quasar.api.services

import quasar.Predef._
import quasar.{Variables}
import quasar.api._
import quasar.sql.{Expr}
import quasar.effect.{KeyValueStore}
import quasar.fp._
import quasar.fs.{Path => QPath, _}
import quasar.fs.mount._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import pathy.Path, Path._
import pathy.scalacheck._
import pathy.scalacheck.PathyArbitrary._

class MountServiceSpec extends Specification with ScalaCheck with Http4s with PathUtils {
  import posixCodec.printPath

  val StubFs = FileSystemType("stub")

  def pathUri(path: APath): Uri = Uri(path = UriPathCodec.printPath(path))

  def viewConfig(q: String, vars: (String, String)*): (Expr, Variables) =
    ((new quasar.sql.SQLParser).parse(quasar.sql.Query(q)).toOption.get,
      Variables(Map(vars.map { case (n, v) =>
        quasar.VarName(n) -> quasar.VarValue(v) }: _*)))

  type Eff[A] = Coproduct[Task, MountingF, A]

  val M = Mounting.Ops[Eff]

  type HttpSvc = Request => M.F[Response]

  def runTest[R: org.specs2.execute.AsResult](f: HttpSvc => M.F[R]): R = {
    type MEff[A] = Coproduct[Task, MountConfigsF, A]

    TaskRef(Map[APath, MountConfig2]()).map { ref =>

      val mounter: Mounting ~> Free[MEff, ?] = Mounter[Task, MEff](
        {
          case MountRequest.MountView(_, expr, Variables(vars)) if (vars.isEmpty) =>
            Task.now(MountingError.invalidConfig(MountConfig2.viewConfig(expr, Variables(vars)),
              "unbound variable (simulated)".wrapNel).left)
          case MountRequest.MountFileSystem(_, typ, uri @ ConnectionUri("invalid")) =>
            Task.now(MountingError.invalidConfig(MountConfig2.fileSystemConfig(typ, uri),
              "invalid connectionUri (simulated)".wrapNel).left)
          case _ =>
            Task.now(().right)
        },
        κ(Task.now(())))

      val store: MountConfigs ~> Task = KeyValueStore.fromTaskRef(ref)

      val mt: MEff ~> Task = free.interpret2[Task, MountConfigsF, Task](NaturalTransformation.refl, Coyoneda.liftTF(store))

      val tf: MountingF ~> Task = Coyoneda.liftTF(hoistFree(mt) compose mounter)

      val eff: Eff ~> Task =
        free.interpret2[Task, MountingF, Task](NaturalTransformation.refl, tf)

      val service = mount.service[Eff].toHttpService(liftMT[Task, ResponseT] compose eff)

      f(service.run andThen (free.lift(_).into[Eff])).foldMap(eff)
    }.run.run
  }

  def orFail[A](v: MountingError \/ A): Task[A] =
    Task.fromDisjunction(v.leftMap(e => new RuntimeException(e.shows)))
  def orFailF[A](v: MountingError \/ A): M.F[A] =
    free.lift(orFail(v)).into[Eff]

  "Mount Service" should {
    "GET" should {
      "succeed with correct filesystem path" ! prop { d: ADir =>
        !hasDot(d) ==> {
          runTest { service =>
            for {
              _    <- M.mountFileSystem(d, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(uri = pathUri(d)))
            } yield {
              resp.as[Json].run must_== Json("stub" -> Json("connectionUri" := "foo"))
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
            resp.as[Json].run must_== Json("stub" -> Json("connectionUri" := "foo"))
            resp.status must_== Status.Ok
          }
        }
      }

      "succeed with correct view path" ! prop { f: AFile =>
        !hasDot(f) ==> {
          runTest { service =>
            val cfg = viewConfig("select * from zips where pop > :cutoff", "cutoff" -> "1000")
            val cfgStr = EncodeJson.of[MountConfig2].encode(MountConfig2.viewConfig(cfg))

            for {
              _    <- M.mountView(f, cfg._1, cfg._2).run.flatMap(orFailF)

              resp <- service(Request(uri = pathUri(f)))
            } yield {
              resp.as[Json].run must_== cfgStr
              resp.status must_== Status.Ok
            }
          }
        }
      }

      // FIXME: use APath (see PathGen)
      // TODO: escaped paths do not survive being embedded in error messges
      "be 404 with missing mount (dir)" ! prop { d: AbsDirOf[AlphaCharacters] =>
        runTest { service =>
          for {
            resp <- service(Request(uri = pathUri(d.path)))
          } yield {
            resp.as[Json].run must_== Json("error" := s"There is no mount point at ${printPath(d.path)}")
            resp.status must_== Status.NotFound
          }
        }
      }

      // TODO: escaped paths do not survive being embedded in error messges
      "be 404 with path type mismatch" ! prop { fp: AbsFileOf[AlphaCharacters] =>
        runTest { service =>
          val dp = fileParent(fp.path) </> dir(fileName(fp.path).value)

          for {
            _    <- M.mountFileSystem(dp, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

            resp <- service(Request(uri = pathUri(fp.path)))
          } yield {
            resp.as[Json].run must_== Json("error" := s"There is no mount point at ${printPath(fp.path)}")
            resp.status must_== Status.NotFound
          }
        }
      }
    }

    "MOVE" should {
      import org.http4s.Method.MOVE

      def destination(p: pathy.Path[_, _, Sandboxed]) = Header(Destination.name.value, printPath(p))

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
              resp.as[String].run must_== s"moved ${printPath(src)} to ${printPath(dst)}"
              resp.status must_== Status.Ok

              srcAfter must beNone
              dstAfter must beSome(MountConfig2.fileSystemConfig(StubFs, ConnectionUri("foo")))
            }
          }
        }
      }

      // TODO: escaped paths do not survive being embedded in error messges
      "be 404 with missing source" ! prop { (src: AbsDirOf[AlphaCharacters], dst: ADir) =>
        runTest { service =>
          for {
            resp <- service(Request(
              method = MOVE,
              uri = pathUri(src.path),
              headers = Headers(destination(dst))))
          } yield {
            resp.as[Json].run must_== Json("error" := s"${printPath(src.path)} doesn't exist")
            resp.status must_== Status.NotFound
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
              resp.as[Json].run must_== Json("error" := s"The 'Destination' header must be specified")
              resp.status must_== Status.BadRequest
            }
          }
        }
      }

      // TODO: escaped paths do not survive being embedded in error messages
      "be 400 with relative path destination" ! prop { (src: ADir, dst: RelDirOf[AlphaCharacters]) =>
        !hasDot(src) ==> {
          runTest { service =>
            for {
              _    <- M.mountFileSystem(src, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(
                method = MOVE,
                uri = pathUri(src),
                headers = Headers(destination(dst.path))))
            } yield {
              resp.as[Json].run must_== Json("error" := s"Not an absolute directory path: ${printPath(dst.path)}")
              resp.status must_== Status.BadRequest
            }
          }
        }
      }

      // TODO: escaped paths do not survive being embedded in error messages
      "be 400 with non-directory path destination" ! prop { (src: ADir, dst: AbsFileOf[AlphaCharacters]) =>
        !hasDot(src) ==> {
          runTest { service =>
            for {
              _    <- M.mountFileSystem(src, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

              resp <- service(Request(
                method = MOVE,
                uri = pathUri(src),
                headers = Headers(destination(dst.path))))
            } yield {
              resp.as[Json].run must_== Json("error" := s"Not an absolute directory path: ${printPath(dst.path)}")
              resp.status must_== Status.BadRequest
            }
          }
        }
      }
    }

    def xFileName(p: pathy.Path[_, _, Sandboxed]) = Header(XFileName.name.value, printPath(p))

    "Common" >> {
      import org.http4s.Method.POST
      import org.http4s.Method.PUT

      trait RequestBuilder {
        def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]): Request
      }

      def testBoth(test: RequestBuilder => Unit) = {
        "POST" should {
          test(new RequestBuilder {
            def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]) =
              Request(
                method = POST,
                uri = pathUri(parent),
                headers = Headers(xFileName(mount)))
              .withBody(body).run
            })
        }

        "PUT" should {
          test(new RequestBuilder {
            def apply[B](parent: ADir, mount: RPath, body: B)(implicit B: EntityEncoder[B]) =
              Request(
                method = PUT,
                uri = pathUri(parent </> mount))
              .withBody(body).run
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
                resp.as[String].run must_== s"added ${printPath(parent </> fsDir)}"
                resp.status must_== Status.Ok

                after must beSome(MountConfig2.fileSystemConfig(StubFs, ConnectionUri("foo")))
              }
            }
          }
        }

        "succeed with view path" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig2].encode(MountConfig2.viewConfig(cfg))

              for {
                resp  <- service(reqBuilder(parent, f, cfgStr))

                after <- M.lookup(parent </> f).run
              } yield {
                resp.as[String].run must_== s"added ${printPath(parent </> f)}"
                resp.status must_== Status.Ok

                after must beSome(MountConfig2.viewConfig(cfg))
              }
            }
          }
        }

        "succeed with view under existing fs path" ! prop { (fs: ADir, viewSuffix: RFile) =>
          !hasDot(fs </> viewSuffix) ==> {
            runTest { service =>
              val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig2].encode(MountConfig2.viewConfig(cfg))

              val view = fs </> viewSuffix

              for {
                _         <- M.mountFileSystem(fs, StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

                resp      <- service(reqBuilder(fs, viewSuffix, cfgStr))

                afterFs   <- M.lookup(fs).run
                afterView <- M.lookup(view).run
              } yield {
                resp.as[String].run must_== s"added ${printPath(view)}"
                resp.status must_== Status.Ok

                afterFs must beSome
                afterView must beSome(MountConfig2.viewConfig(cfg))
              }
            }
          }
        }

        // TODO: escaped paths do not survive being embedded in error messages
        "succeed with view 'above' existing fs path" ! prop { (d: AbsDirOf[AlphaCharacters], view: RFile, fsSuffix: RDir) =>
          !hasDot(d.path </> view) ==> {
            runTest { service =>
              val fsCfg = ()

              val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
              val cfgStr = EncodeJson.of[MountConfig2].encode(MountConfig2.viewConfig(cfg))

              val fs = d.path </> posixCodec.parseRelDir(printPath(view) + "/").flatMap(sandbox(currentDir, _)).get </> fsSuffix

              for {
                _     <- M.mountFileSystem(fs,StubFs, ConnectionUri("foo")).run.flatMap(orFailF)

                resp  <- service(reqBuilder(d.path, view, cfgStr))

                after <- M.lookup(d.path </> view).run
              } yield {
                resp.as[String].run must_== s"added ${printPath(d.path </> view)}"
                resp.status must_== Status.Ok

                after must beSome(MountConfig2.viewConfig(cfg))
              }
            }
          }
        }

        // TODO: escaped paths do not survive being embedded in error messages
        "be 409 with fs above existing fs path" ! prop { (d: AbsDirOf[AlphaCharacters], fs: RelDirOf[AlphaCharacters], fsSuffix: RelDirOf[AlphaCharacters]) =>
          (!identicalPath(fsSuffix.path, currentDir)) ==> {
            runTest { service =>
              val cfg = (StubFs, ConnectionUri("foo"))

              val cfgStr = EncodeJson.of[MountConfig2].encode(MountConfig2.fileSystemConfig(cfg))

              val fs1 = d.path </> fs.path </> fsSuffix.path

              for {
                _     <- M.mountFileSystem(fs1, cfg._1, cfg._2).run.flatMap(orFailF)

                resp  <- service(reqBuilder(d.path, fs.path, cfgStr))

                after <- M.lookup(d.path </> fs.path).run
              } yield {
                resp.as[Json].run must_== Json("error" := s"cannot mount at ${printPath(d.path </> fs.path)} because existing mount below: ${printPath(fs1)}")
                resp.status must_== Status.Conflict

                after must beNone
              }
            }
          }
        }.pendingUntilFixed("test harness does not yet detect conflicts")

        // TODO: escaped paths do not survive being embedded in error messages
        "be 400 with fs config and file path in X-File-Name header" ! prop { (parent: AbsDirOf[AlphaCharacters], fsFile: RelFileOf[AlphaCharacters]) =>
          runTest { service =>
            val cfg = (StubFs, ConnectionUri("foo"))

            for {
              resp <- service(reqBuilder(parent.path, fsFile.path, """{ "stub": { "connectionUri": "foo" } }"""))
            } yield {
              resp.as[Json].run must_== Json("error" := s"wrong path type for mount: ${printPath(parent.path </> fsFile.path)}; directory path required")
              resp.status must_== Status.BadRequest
            }
          }
        }

        // TODO: escaped paths do not survive being embedded in error messages
        "be 400 with view config and dir path in X-File-Name header" ! prop { (parent: AbsDirOf[AlphaCharacters], viewDir: RelDirOf[AlphaCharacters]) =>
          runTest { service =>
            val cfg = viewConfig("select * from zips where pop < :cutoff", "cutoff" -> "1000")
            val cfgStr = EncodeJson.of[MountConfig2].encode(MountConfig2.viewConfig(cfg))

            for {
              resp <- service(reqBuilder(parent.path, viewDir.path, cfgStr))
            } yield {
              resp.as[Json].run must_== Json("error" := s"wrong path type for mount: ${printPath(parent.path </> viewDir.path)}; file path required")
              resp.status must_== Status.BadRequest
            }
          }
        }

        "be 400 with unbound variable in view" ! prop { (parent: ADir, f: RFile) =>
          !hasDot(parent </> f) ==> {
            runTest { service =>
              val cfg = viewConfig("select * from zips where pop < :cutoff")
              val cfgStr = EncodeJson.of[MountConfig2].encode(MountConfig2.viewConfig(cfg))

              for {
                resp <- service(reqBuilder(parent, f, cfgStr))
              } yield {
                resp.as[Json].run must_== Json("error" := s"unbound variable (simulated)")
                resp.status must_== Status.BadRequest
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
                resp.as[Json].run must_== Json("error" := "input error: JSON terminates unexpectedly.")
                resp.status must_== Status.BadRequest
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
                resp.as[Json].run must_== Json("error" := "invalid connectionUri (simulated)")
                resp.status must_== Status.BadRequest
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
                resp.as[Json].run must_== Json("error" := "unrecognized scheme: foo")
                resp.status must_== Status.BadRequest
              }
            }
          }
        }

        () // TODO: Remove after upgrading to specs2 3.x
      }
    }


    "POST" should {
      import org.http4s.Method.POST

      // TODO: escaped paths do not survive being embedded in error messages
      "be 409 with existing filesystem path" ! prop { (parent: AbsDirOf[AlphaCharacters], fsDir: RelDirOf[AlphaCharacters]) =>
        runTest { service =>
          val previousCfg = (StubFs, ConnectionUri("bar"))

          for {
            _    <- M.mountFileSystem(parent.path </> fsDir.path, previousCfg._1, previousCfg._2).run.flatMap(orFailF)

            resp <- service(Request(
                      method = POST,
                      uri = pathUri(parent.path),
                      headers = Headers(xFileName(fsDir.path)))
                    .withBody("""{ "stub": { "connectionUri": "foo" } }""").run)

            after <- M.lookup(parent.path </> fsDir.path).run
          } yield {
            resp.as[Json].run must_== Json("error" := s"${printPath(parent.path </> fsDir.path)} already exists")
            resp.status must_== Status.Conflict

            after must beSome(MountConfig2.fileSystemConfig(previousCfg))
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
                      .withBody("""{ "stub": { "connectionUri": "foo" } }""").run)
            } yield {
              resp.as[Json].run must_== Json("error" := "The 'X-File-Name' header must be specified")
              resp.status must_== Status.BadRequest
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
                      .withBody("""{ "stub": { "connectionUri": "foo" } }""").run)

              after <- M.lookup(fsDir).run
            } yield {
              resp.as[String].run must_== s"updated ${printPath(fsDir)}"
              resp.status must_== Status.Ok

              after must beSome(MountConfig2.fileSystemConfig(cfg))
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
              resp.as[String].run must_== s"deleted ${printPath(d)}"
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
              resp.as[String].run must_== s"deleted ${printPath(f)}"

              after must beNone
            }
          }
        }
      }

      // FIXME: need Arbitrary[APath]
      // TODO: escaped paths do not survive being embedded in error messages
      "be 404 with missing path" ! prop { (p: AbsDirOf[AlphaCharacters]) =>
        runTest { service =>
          for {
            resp <- service(Request(
                      method = DELETE,
                      uri = pathUri(p.path)))
          } yield {
            resp.as[Json].run must_== Json("error" := s"${printPath(p.path)} doesn't exist")
            resp.status must_== Status.NotFound
          }
        }
      }
    }
  }
}
