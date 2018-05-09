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

package quasar.api.services

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.{Data, RepresentableData, Variables}
import quasar.DataGenerators._
import quasar.RepresentableDataGenerators._
import quasar.api._,
  ApiErrorEntityDecoder._, PathUtils._, MessageFormat.JsonContentType, MessageFormatGen._
import quasar.api.matchers._
import quasar.api.MessageFormatGen._
import quasar.contrib.pathy._, PathArbitrary._
import quasar.csv.CsvParser
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._, MountConfigArbitrary._
import MountConfig.{ModuleConfig, ViewConfig, viewConfig0}
import quasar.fs.mount.cache.ViewCache
import quasar.main.CoreEffIO
import quasar.sql._
import quasar.time.DateGenerators._

import java.time.{Duration, Instant}

import argonaut.{Json, EncodeJson}
import argonaut.Argonaut._
import argonaut.ArgonautScalaz._
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.{NonNegative, Negative, Positive => RPositive}
import eu.timepit.refined.scalacheck.numeric._
import eu.timepit.refined.scalaz._
import org.http4s._
import org.http4s.argonaut._
import org.http4s.headers._
import org.http4s.server.middleware.GZip
import org.specs2.specification.core.Fragment
import org.specs2.execute.AsResult
import org.specs2.matcher.MatchResult
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import pathy.scalacheck.PathyArbitrary._
import scalaz.{Failure => _, Zip =>_, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.stream.Process
import scodec.bits._
import shapeless.tag.@@

class DataServiceSpec extends quasar.Qspec with FileSystemFixture with Http4s {
  import Fixture._, InMemory._, JsonPrecision._, JsonFormat._
  import FileSystemFixture.{ReadWriteT, ReadWrites, amendWrites}
  import PathError.pathNotFound
  import VCacheFixture._

  def service(mem: InMemState, mounts: MountingsConfig = MountingsConfig.empty): Service[Request, Response] =
    serviceRef(mem, mounts)._1

  def serviceRef(mem: InMemState, mounts: MountingsConfig = MountingsConfig.empty): (Service[Request, Response], Task[(InMemState, Map[APath, MountConfig])]) = {
    val (inter, ref) = inMemFSWebInspect(mem, mounts).unsafePerformSync
    val svc = HeaderParam(GZip(data.service[CoreEffIO].toHttpService(inter))).orNotFound
    (svc, ref)
  }

  def restrict[M[_], S[_], T[_]](f: T ~> M)(implicit S: S :<: T) =
    f compose injectNT[S, T]

  def serviceErrs(mem: InMemState, writeErrors: FileSystemError*): HttpService = {
    type RW[A] = ReadWriteT[FailedResponseOr, A]
    val fs0: Task[CoreEffIO ~> FailedResponseOr] = inMemFSWeb(mem)
    val g: Task[WriteFile ~> RW] = fs0 ∘ (i => amendWrites(restrict[FailedResponseOr, WriteFile, CoreEffIO](i)))
    val f: Task[CoreEffIO ~> RW] = fs0 ∘ (liftMT[FailedResponseOr, ReadWriteT] compose _)
    val fsErrs: Task[CoreEffIO ~> FailedResponseOr] =
      (f ⊛ g)((fʹ, gʹ) =>
        evalNT[FailedResponseOr, ReadWrites]((Nil, List(writeErrors.toVector))) compose free.transformIn(gʹ, fʹ))

    data.service[CoreEffIO].toHttpService(fsErrs.unsafePerformSync)
  }

  val csv = MediaType.`text/csv`

  trait StateChange
  object Unchanged extends StateChange
  case class Changed(data: FileMap = Map.empty, mounts: Map[APath, MountConfig] = Map.empty) extends StateChange

  def isExpectedResponse(data: Vector[Data], response: Response, format: MessageFormat) = {
    val expectedBody: Process[Task, String] = format.encode(Process.emitAll(data))
    response.as[String].unsafePerformSync must_=== expectedBody.runLog.unsafePerformSync.mkString("")
    response.status must_=== Status.Ok
    response.contentType must_=== Some(`Content-Type`(format.mediaType, Charset.`UTF-8`))
  }

  "Data Service" should {
    "GET" >> {
      "return 404 NotFound if file does not exist" >> prop { file: AFile =>
        val response = service(InMemState.empty)(Request(uri = pathUri(file))).unsafePerformSync
        response.status must_= Status.NotFound
        response.as[Json].unsafePerformSync must_= Json(
          "error" := Json(
            "status" := "Path not found.",
            "detail" := Json(
              "path" := posixCodec.printPath(file))))
      }
      "respond with file data" >> {
        "in correct format" >> {
          "readable and line delimited json by default" >> prop { filesystem: SingleFileMemState =>
            val response = service(filesystem.state)(Request(uri = pathUri(filesystem.file))).unsafePerformSync
            isExpectedResponse(filesystem.contents, response, MessageFormat.Default)
          }
          "in any supported format if specified" >> {
            "in the content-type header" >> {
              def testProp(format: MessageFormat) = prop { filesystem: SingleFileMemState =>
                test(format, filesystem)
              }
              def test(format: MessageFormat, filesystem: SingleFileMemState) = {
                val request = Request(
                  uri = pathUri(filesystem.file),
                  headers = Headers(Accept(format.mediaType)))
                val response = service(filesystem.state)(request).unsafePerformSync
                isExpectedResponse(filesystem.contents, response, format)
              }
              "json" >> {
                s"readable and line delimited (${jsonReadableLine.mediaType.renderString})" >> {
                  testProp(jsonReadableLine)
                }
                s"precise and line delimited (${jsonPreciseLine.mediaType.renderString})" >> {
                  testProp(jsonPreciseLine)
                }
                s"readable and in a single json array (${jsonReadableArray.mediaType.renderString})" >> {
                  testProp(jsonReadableArray)
                }
                s"precise and in a single json array (${jsonPreciseArray.mediaType.renderString})" >> {
                  testProp(jsonPreciseArray)
                }
              }
              "csv" >> prop { (filesystem: SingleFileMemState, format: MessageFormat.Csv) =>
                test(format, filesystem)
              }
              "or a more complicated proposition" >> prop { filesystem: SingleFileMemState =>
                val request = Request(
                  uri = pathUri(filesystem.file),
                  headers = Headers(Header("Accept", "application/ldjson;q=0.9;mode=readable,application/json;boundary=NL;mode=precise")))
                val response = service(filesystem.state)(request).unsafePerformSync
                isExpectedResponse(filesystem.contents, response, JsonContentType(Precise, LineDelimited))
              }
            }
            "in the request-headers" >> prop { filesystem: SingleFileMemState =>
              val contentType = JsonContentType(Precise, LineDelimited)
              val request = Request(
                uri = pathUri(filesystem.file).+?("request-headers", s"""{"Accept": "application/ldjson; mode=precise" }"""))
              val response = service(filesystem.state)(request).unsafePerformSync
              isExpectedResponse(filesystem.contents, response, JsonContentType(Precise, LineDelimited))
            }
          }
        }
        "with gziped encoding when specified" >> prop { filesystem: SingleFileMemState =>
          val request = Request(
            uri = pathUri(filesystem.file),
            headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
          val response = service(filesystem.state)(request).unsafePerformSync
          response.headers.get(headers.`Content-Encoding`) must_=== Some(`Content-Encoding`(ContentCoding.gzip))
          response.status must_=== Status.Ok
        }
        "support disposition" >> prop { filesystem: SingleFileMemState =>
          val disposition = `Content-Disposition`("attachement", Map("filename" -> "data.json"))
          val request = Request(
            uri = pathUri(filesystem.file),
            headers = Headers(Accept(jsonReadableLine.mediaType.withExtensions(Map("disposition" -> disposition.value)))))
          val response = service(filesystem.state)(request).unsafePerformSync
          isExpectedResponse(filesystem.contents, response, MessageFormat.Default)
          response.headers.get(`Content-Disposition`) must_=== Some(disposition)
        }
        "support disposition non-ascii filename" >> prop { filesystem: SingleFileMemState =>
          val disposition = `Content-Disposition`("attachement", Map("filename*" -> "UTF-8''Na%C3%AFve%20file.txt"))
          val request = Request(
            uri = pathUri(filesystem.file),
            headers = Headers(Accept(jsonReadableLine.mediaType.withExtensions(Map("disposition" -> disposition.value)))))
          val response = service(filesystem.state)(request).unsafePerformSync
          isExpectedResponse(filesystem.contents, response, MessageFormat.Default)
          response.headers.get(`Content-Disposition`) must_=== Some(disposition)
        }
        "support offset and limit" >> {
          "return expected result if user supplies valid values" >> prop {
            (filesystem: SingleFileMemState, offset: Int @@ NonNegative, limit: Int @@ RPositive, format: MessageFormat) =>
              val request = Request(
                uri = pathUri(filesystem.file).+?("offset", offset.toString).+?("limit", limit.toString),
                headers = Headers(Accept(format.mediaType)))
              val response = service(filesystem.state)(request).unsafePerformSync
              isExpectedResponse(filesystem.contents.drop(offset).take(limit), response, format)
          }
          "return 400 if provided with" >> {
            "a non-positive limit (0 is invalid)" >> prop { (path: AbsFile[Sandboxed], offset: Natural, limit: Int) =>
              (limit < 1) ==> {
                val request = Request(
                  uri = pathUri(path).+?("offset", offset.shows).+?("limit", limit.shows))
                val response = service(InMemState.empty)(request).unsafePerformSync
                response.as[ApiError].unsafePerformSync must beApiErrorWithMessage(
                  Status.BadRequest withReason "Invalid query parameter.")
              }
            }
            "a negative offset" >> prop { (path: AbsFile[Sandboxed], offset: Long @@ Negative, limit: Positive) =>
              val request = Request(
                uri = pathUri(path).+?("offset", offset.shows).+?("limit", limit.shows))
              val response = service(InMemState.empty)(request).unsafePerformSync
              response.as[ApiError].unsafePerformSync must beApiErrorWithMessage(
                Status.BadRequest withReason "Invalid query parameter.")
            }
            "if provided with multiple limits?" >> prop { (path: AbsFile[Sandboxed], offset: Natural, limit1: Positive, limit2: Positive, otherLimits: List[Positive]) =>
              val limits = limit1 :: limit2 :: otherLimits
              val request = Request(
                uri = pathUri(path).+?("offset", offset.shows).+?("limit", limits.map(_.shows)))
              val response = service(InMemState.empty)(request).unsafePerformSync
              response.status must_=== Status.BadRequest
              response.as[Json].unsafePerformSync must_=== Json("error" := s"Two limits were provided, only supply one limit")
            }.pendingUntilFixed("SD-1082")
            "if provided with multiple offsets?" >> prop { (path: AbsFile[Sandboxed], limit: Positive, offsets: List[Natural]) =>
              (offsets.length >= 2) ==> {
                val request = Request(
                  uri = pathUri(path).+?("offset", offsets.map(_.shows)).+?("limit", limit.shows))
                val response = service(InMemState.empty)(request).unsafePerformSync
                response.status must_=== Status.BadRequest
                response.as[Json].unsafePerformSync must_=== Json("error" := s"Two limits were provided, only supply one limit")
                todo // Confirm this is the expected behavior because http4s defaults to just grabbing the first one
                     // and going against that default behavior would be more work
              }
            }.pendingUntilFixed("SD-1082")
            "an unparsable limit" >> prop { path: AbsFile[Sandboxed] =>
              val request = Request(uri = pathUri(path).+?("limit", "a"))
              val response = service(InMemState.empty)(request).unsafePerformSync
              response.as[ApiError].unsafePerformSync must beApiErrorWithMessage(
                Status.BadRequest withReason "Invalid query parameter.")
            }
            "if provided with both an invalid offset and limit" >> prop { (path: AbsFile[Sandboxed], limit: Int, offset: Long @@ Negative) =>
              (limit < 1) ==> {
                val request = Request(uri = pathUri(path).+?("limit", limit.shows).+?("offset", offset.shows))
                val response = service(InMemState.empty)(request).unsafePerformSync
                response.status must_=== Status.BadRequest
                response.as[Json].unsafePerformSync must_=== Json("error" := s"invalid limit: $limit (must be >= 1), invalid offset: $offset (must be >= 0)")
              }
            }.pendingUntilFixed("SD-1083")
          }
        }
        "support very large data set" >> {
          val sampleFile = rootDir[Sandboxed] </> dir("foo") </> file("bar")
          def fileSystemWithSampleFile(data: Vector[Data]) = InMemState fromFiles Map(sampleFile -> data)
          // NB: defer constructing this large object until the actual test that uses it, so it becomes garbage sooner.
          def bigData = (0 until 100*1000).map(n => Data.Obj(ListMap("n" -> Data.Int(n)))).toVector
          "plain text" >> {
            val data = bigData
            val request = Request(uri = pathUri(sampleFile))
            val response = service(fileSystemWithSampleFile(data))(request).unsafePerformSync
            isExpectedResponse(data, response, MessageFormat.Default)
          }
          "gziped" >> {
            val data = bigData
            val request = Request(
              uri = pathUri(sampleFile),
              headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
            val response = service(fileSystemWithSampleFile(data))(request).unsafePerformSync
            response.status must_=== Status.Ok
          }
          "zipped json" >> {
            val disposition = `Content-Disposition`("attachment", Map("filename*" -> "UTF-8''foo.zip"))
            val data = Vector(Data.Obj("a" -> Data.Str("bar"), "b" -> Data.Bool(true)))
            val request = Request(
              uri = pathUri(sampleFile),
              headers = Headers(Header("Accept", "application/zip,application/json;disposition=\"attachment;filename*=UTF-8''foo.zip\"")))
            val response = service(fileSystemWithSampleFile(data))(request).unsafePerformSync
            val zipfile = response.as[ByteVector].unsafePerformSync
            val zipMagicByte: ByteVector = hex"504b" // zip file magic byte

            (response.headers.get(`Content-Disposition`.name) must_=== Some(disposition)) and
              (response.contentType must_=== Some(`Content-Type`(MediaType.`application/zip`))) and
              (response.status must_=== Status.Ok) and
              (zipfile.take(2) must_=== zipMagicByte) and
              (Zip.unzipFiles(Process.emit(zipfile)).run.unsafePerformSync.map(_.keys) must_=== \/-(Set(currentDir </> file(".quasar-metadata.json"), currentDir </>file("bar.json"))))

          }
          "download single zipped file and then re-upload zipped file" >> {
            val baseDir = rootDir[Sandboxed]
            val sampleFile = baseDir </> file("foo.json")
            val data = Vector(Data.Obj("a" -> Data.Str("foo"), "b" -> Data.Bool(true)))
            val initialContent = Map(sampleFile -> data)

            val downloadRequest = Request(
                  uri = pathUri(sampleFile),
                  headers = Headers(Header("Accept", "application/zip,application/json;disposition=\"attachment;filename*=UTF-8''foo.zip\"")))
            val originalService = service(InMemState.fromFiles(initialContent))
            val zippedResponse = originalService(downloadRequest).flatMap(_.as[ByteVector]).unsafePerformSync

            val uploadRequest = Request(uri = pathUri(baseDir), method = Method.PUT)
                                  .withBody(zippedResponse).unsafePerformSync
                                  .withContentType(`Content-Type`(MediaType.`application/zip`).some)
            val (emptyService, getState) = serviceRef(InMemState.empty)
            val uploadResponse = emptyService(uploadRequest).unsafePerformSync

            uploadResponse.as[String].unsafePerformSync must_=== ""
            uploadResponse.status must_=== Status.Ok
            getState.unsafePerformSync._1.contents must_=== initialContent
          }
          "zipped csv" >> {
            val disposition = `Content-Disposition`("attachment", Map("filename*" -> "UTF-8''foo.zip"))
            val data = Vector(Data.Str("a,b\n1,2"))
            val request = Request(
              uri = pathUri(sampleFile),
              headers = Headers(
                Header("Accept", "application/zip,text/csv;columnDelimiter=\",\";quoteChar=\"\\\"\";escapeChar=\"\\\"\";disposition=\"attachment;filename*=UTF-8''foo.zip\"")))
            val response = service(fileSystemWithSampleFile(data))(request).unsafePerformSync
            val zipfile = response.as[ByteVector].unsafePerformSync
            val zipMagicByte: ByteVector = hex"504b" // zip file magic byte

            response.headers.get(`Content-Disposition`.name) must_=== Some(disposition)
            response.contentType must_=== Some(`Content-Type`(MediaType.`application/zip`))
            response.status must_=== Status.Ok
            zipfile.take(2) must_=== zipMagicByte
            Zip.unzipFiles(Process.emit(zipfile)).run.unsafePerformSync.map(_.keys) must_=== \/-(Set(currentDir </> file(".quasar-metadata.json"), currentDir </>file("bar.csv")))
          }
          "zipped via request headers" >> {
            val data = Vector(Data.Obj("a" -> Data.Str("bar"), "b" -> Data.Bool(true)))
            val request = Request(uri = pathUri(sampleFile).+?("request-headers", s"""{"Accept-Encoding":"gzip","Accept":"application/zip,application/json"}"""))
            val response = service(fileSystemWithSampleFile(data))(request).unsafePerformSync
            val zipfile = response.as[ByteVector].unsafePerformSync
            val zipMagicByte: ByteVector = hex"504b" // zip file magic byte

            response.contentType must_=== Some(`Content-Type`(MediaType.`application/zip`))
            response.status must_=== Status.Ok
            zipfile.take(2) must_=== zipMagicByte
            Zip.unzipFiles(Process.emit(zipfile)).run.unsafePerformSync.map(_.keys) must_=== \/-(Set(currentDir </> file(".quasar-metadata.json"), currentDir </>file("bar.json")))
          }
        }
      }

      "if file is a view" >> {
        "download data as if it were an ordinary file" >> prop { (viewFile: AFile, data: Vector[Data]) =>
          val file0 = rootDir </> file("data")
          val view = MountConfig.viewConfig0(sqlB"select * from `/data`")
          val mounts = Map((viewFile: APath) -> view)
          val service0 = service(InMemState.fromFiles(Map(file0 -> data)), MountingsConfig(mounts))
          val response = service0(Request(uri = pathUri(viewFile))).unsafePerformSync
          isExpectedResponse(data, response, MessageFormat.Default)
        }
        // Note: I don't think this is the ideal response but it's a little tricky to
        // implement a different response and it's been behaving like this for quite some
        // time so I think it's okay to keep it this way until a client argues otherwise
        "if view points to non-existent file, respond with a 404 NotFound with the missing file" >> prop { viewFile: AFile =>
          val view = MountConfig.viewConfig0(sqlB"select * from `/foo`")
          val mounts = Map((viewFile: APath) -> view)
          val response = service(emptyMem, MountingsConfig(mounts))(Request(uri = pathUri(viewFile))).unsafePerformSync
          (response.as[Json].unsafePerformSync must_= Json(
            "error" := Json(
              "status" := "Path not found.",
              "detail" := Json(
                "path" := "/foo")))) and
          (response.status must_=== Status.NotFound)
        }
      }

      "respond with view cache data" >> {
        "fresh" >> prop {
            (f: AFile, g: AFile, now: Instant, lastUpdate: Instant, maxAgeSecs: Int @@ RPositive) => {
            val maxAge = Duration.ofSeconds(maxAgeSecs.toLong)
            lastUpdate.isBefore(Instant.MAX.minus(maxAge)) && now.isBefore(lastUpdate.plus(maxAge)) && f ≠ g
          } ==> {
            val expr = sqlB"""select { "α": 7 }"""
            val viewCache = ViewCache(
              MountConfig.ViewConfig(expr, Variables.empty), lastUpdate.some, None, 0, None, None,
              maxAgeSecs.toLong, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, g, None)
            val mounts = Map[APath, MountConfig](f -> viewConfig0(expr))
            val memState = InMemState.fromFiles(Map(g -> Vector(Data.Obj("α" -> Data._int(7)))))

            val (respA, respB, vc) = evalViewTest(now, mounts, memState) { (it, ir) =>
              (for {
                _ <- vcache.put(f, viewCache)
                a <- data.service[ViewEff].apply(Request(uri = pathUri(f)))
                b <- data.service[ViewEff].apply(Request(uri = pathUri(g)))
                c <- vcache.get(f).run
              } yield (a.toHttpResponse(ir), b.toHttpResponse(ir), c)).foldMap(it)
            }.unsafePerformSync

            respA.status must_= Status.Ok
            respB.status must_= Status.Ok
            respA.headers.get(Expires.name) ∘ (_.value) must_= None
            respA.as[String].unsafePerformSync must_= respB.as[String].unsafePerformSync
            vc ∘ (_.cacheReads) must_= (viewCache.cacheReads ⊹ 1).some
          }
        }

        "stale" >> prop {
            (f: AFile, g: AFile, now: Instant, lastUpdate: Instant, maxAgeSecs: Int @@ RPositive) => {
            val maxAge = Duration.ofSeconds(maxAgeSecs.toLong)
              lastUpdate.isBefore(Instant.MAX.minus(maxAge)) && now.isAfter(lastUpdate.plus(maxAge)) && f ≠ g
          } ==> {
            val expr = sqlB"""select { "α": 7 }"""
            val viewCache = ViewCache(
              MountConfig.ViewConfig(expr, Variables.empty), lastUpdate.some, None, 0, None, None,
              maxAgeSecs.toLong, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, g, None)
            val mounts = Map[APath, MountConfig](f -> viewConfig0(expr))
            val memState = InMemState.fromFiles(Map(g -> Vector(Data.Obj("α" -> Data._int(7)))))

            val (respA, respB, vc) = evalViewTest(now, mounts, memState) { (it, ir) =>
              (for {
                _ <- vcache.put(f, viewCache)
                a <- data.service[ViewEff].apply(Request(uri = pathUri(f)))
                b <- data.service[ViewEff].apply(Request(uri = pathUri(g)))
                c <- vcache.get(f).run
              } yield (a.toHttpResponse(ir), b.toHttpResponse(ir), c)).foldMap(it)
            }.unsafePerformSync

            respA.status must_= Status.Ok
            respB.status must_= Status.Ok
            respA.headers.get(Warning) ∘ (_.value) must_= None
            respA.headers.get(Expires) ∘ (_.value) must_= None
            respA.as[String].unsafePerformSync must_= respB.as[String].unsafePerformSync
            vc ∘ (_.cacheReads) must_= (viewCache.cacheReads ⊹ 1).some
           }
        }
      }

      "download directory as zip archive" >> {
        "if directory contains only data files" >> prop { filesystem: NonEmptyDir =>
          val disposition = `Content-Disposition`("attachment", Map("filename" -> "foo.zip"))
          val requestMediaType = MediaType.`text/csv`.withExtensions(Map("disposition" -> disposition.value))
          val request = Request(
            uri = pathUri(filesystem.dir),
            headers = Headers(Accept(requestMediaType)))
          val response = service(filesystem.state)(request).unsafePerformSync
          response.status must_=== Status.Ok
          response.contentType must_=== Some(`Content-Type`(MediaType.`application/zip`))
          response.headers.get(`Content-Disposition`) must_=== Some(disposition)
          // Just want to make sure streaming of the zipped bytes doesn't fail
          // even though we aren't actually checking the validity of the bytes
          response.as[ByteVector].void.unsafePerformSync must_=== (())
        }.set(minTestsOk = 10).flakyTest("scalacheck: Gave up after only 2 passed tests. 12 tests were discarded.")  // NB: this test is slow because NonEmptyDir instances are still relatively large
        "if directory contains modules, they are simply ignored" >> prop { (filesystem: NonEmptyDir, moduleConfig: MountConfig.ModuleConfig) =>
          val disposition = `Content-Disposition`("attachment", Map("filename" -> "foo.zip"))
          val requestMediaType = MediaType.`text/csv`.withExtensions(Map("disposition" -> disposition.value))
          val request = Request(
            uri = pathUri(filesystem.dir),
            headers = Headers(Accept(requestMediaType)))
          val mounts = MountingsConfig(Map[APath, MountConfig]((filesystem.dir </> dir("module")) -> moduleConfig))
          val response = service(filesystem.state, mounts)(request).unsafePerformSync
          response.status must_=== Status.Ok
          response.contentType must_=== Some(`Content-Type`(MediaType.`application/zip`))
          response.headers.get(`Content-Disposition`) must_=== Some(disposition)
          // Just want to make sure streaming of the zipped bytes doesn't fail
          // even though we aren't actually checking the validity of the bytes
          response.as[ByteVector].void.unsafePerformSync must_=== (())
        }
        "if directory contains views, they are downloaded as files" >> prop { (filesystem: NonEmptyDir, data: Vector[Data]) =>
          val outsideFile = rootDir </> file("data")
          val viewConfig = MountConfig.viewConfig0(sqlB"select * from `/data`")
          val disposition = `Content-Disposition`("attachment", Map("filename" -> "foo.zip"))
          val requestMediaType = MediaType.`text/csv`.withExtensions(Map("disposition" -> disposition.value))
          val request = Request(
            uri = pathUri(filesystem.dir),
            headers = Headers(Accept(requestMediaType)))
          val mounts = MountingsConfig(Map[APath, MountConfig]((filesystem.dir </> file("view")) -> viewConfig))
          val response = service(filesystem.state.addFile(outsideFile, data), mounts)(request).unsafePerformSync
          response.status must_=== Status.Ok
          response.contentType must_=== Some(`Content-Type`(MediaType.`application/zip`))
          response.headers.get(`Content-Disposition`) must_=== Some(disposition)
          // Just want to make sure streaming of the zipped bytes doesn't fail
          // even though we aren't actually checking the validity of the bytes
          response.as[ByteVector].void.unsafePerformSync must_=== (())
        }
        "if directory contains views referencing missing files" >> prop { filesystem: NonEmptyDir =>
          val viewConfig = MountConfig.viewConfig0(sqlB"select * from `/missing`")
          val disposition = `Content-Disposition`("attachment", Map("filename" -> "foo.zip"))
          val requestMediaType = MediaType.`text/csv`.withExtensions(Map("disposition" -> disposition.value))
          val request = Request(
            uri = pathUri(filesystem.dir),
            headers = Headers(Accept(requestMediaType)))
          val mounts = MountingsConfig(Map[APath, MountConfig]((filesystem.dir </> file("view")) -> viewConfig))
          val response = service(filesystem.state, mounts)(request).unsafePerformSync
          response.status must_=== Status.NotFound
        }.pendingUntilFixed("Currently will only fail once streaming has started, but requires substantial change to `ReadFile` API to change behavior")
      }
      "what happens if user specifies a Path that is a directory but without the appropriate headers?" >> todo
      "description of the function if the file is a module function" in todo
      "description of the module if the directory is a module" in todo
    }
    "POST and PUT" >> {
      def testBoth[A](test: org.http4s.Method => Fragment) = {
        "POST" should {
          test(org.http4s.Method.POST)
        }
        "PUT" should {
          test(org.http4s.Method.PUT)
        }
      }
      testBoth { method =>
        "fail when media-type is" >> {
          "not supported" >> prop { file: Path[Abs, File, Sandboxed] =>
            val request = Request(
              uri = pathUri(file),
              method = method).withBody("zip code: 34561 and zip code: 78932").unsafePerformSync
            val response = service(emptyMem)(request).unsafePerformSync
            response.as[ApiError].unsafePerformSync must beApiErrorLike[DecodeFailure](
              MediaTypeMismatch(MediaType.`text/plain`, MessageFormat.supportedMediaTypes))
          }
          "not supplied" >> prop { file: Path[Abs, File, Sandboxed] =>
            val request = Request(
              uri = pathUri(file),
              method = method).withBody("{\"a\": 1}\n{\"b\": \"12:34:56\"}").unsafePerformSync.replaceAllHeaders(Headers.empty)
            val response = service(emptyMem)(request).unsafePerformSync
            response.as[ApiError].unsafePerformSync must beApiErrorLike[DecodeFailure](
              MediaTypeMissing(MessageFormat.supportedMediaTypes))
          }
        }
        "be 400 with" >> {
          def be400[A: EntityDecoder](
            file: AFile,
            reqBody: String,
            expectBody: A => MatchResult[scala.Any],
            mediaType: MediaType = jsonReadableLine.mediaType
          ) = {
            val request = Request(
              uri = pathUri(file),             // We do it this way because withBody sets the content-type
              method = method
            ).withBody(reqBody).unsafePerformSync.replaceAllHeaders(`Content-Type`(mediaType, Charset.`UTF-8`))
            val (service, ref) = serviceRef(emptyMem)
            val response = service(request).unsafePerformSync
            expectBody(response.as[A].unsafePerformSync)
            ref.unsafePerformSync must_===((emptyMem, Map.empty))
          }
          "invalid body" >> {
            "no body" >> prop { file: AFile =>
              be400(
                file,
                reqBody = "",
                expectBody = (_: ApiError) must equal(ApiError.fromStatus(
                  Status.BadRequest withReason "Request has no body."))
              )
            }.set(minTestsOk = 5) // NB: seems like the parser is slow
            "invalid JSON" >> prop { file: AFile =>
              be400(
                file,
                reqBody = "{",
                expectBody = (_: ApiError) must equal(ApiError.apiError(
                  Status.BadRequest withReason "Malformed upload data.",
                  "errors" := List("parse error: JSON terminates unexpectedly. in the following line: {")))
              )
            }.set(minTestsOk = 5)  // NB: seems like the parser is slow
            "invalid CSV" >> {
              "empty (no headers)" >> prop { file: AFile =>
                be400(
                  file,
                  reqBody = "",
                  expectBody = (_: ApiError) must equal(ApiError.fromStatus(
                    Status.BadRequest withReason "Request has no body.")),
                  mediaType = csv
                )
              }.set(minTestsOk = 5)  // NB: seems like the parser is slow
              "if broken (after the tenth data line)" >> prop { file: AFile =>
                val brokenBody = "\"a\",\"b\"\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n\",\n"
                  be400(
                    file,
                    reqBody = brokenBody,
                    expectBody = (_: ApiError) must equal(ApiError.apiError(
                      Status.BadRequest withReason "Malformed upload data.",
                      "errors" := List("parse error: Malformed Input!: Some(\",\n)"))),
                    mediaType = csv)
              }.set(minTestsOk = 5)  // NB: seems like the parser is slow
            }
          }
          // TODO: Consider spliting this into a case of Root (depth == 0) and missing dir (depth > 1)
          "if path is invalid (parent directory does not exist)" >> prop { (file: AFile, json: Json) =>
            Path.depth(file) != 1 ==> {
              be400(file, reqBody = json.spaces4, (_: Json) must_=== Json("error" := s"Invalid path: ${posixCodec.printPath(file)}"))
            }
          }.pendingUntilFixed("What do we want here, create it or not?")
          "produce two errors with partially invalid JSON" >> prop { file: AFile =>
            val twoErrorJson = """{"a": 1}
                                 |"unmatched
                                 |{"b": 2}
                                 |}
                                 |{"c": 3}""".stripMargin
            be400(
              file,
              reqBody = twoErrorJson,
              expectBody = (_: ApiError) must equal(ApiError.apiError(
                Status.BadRequest withReason "Malformed upload data.",
                "errors" := List(
                  "parse error: JSON terminates unexpectedly. in the following line: \"unmatched",
                  "parse error: Unexpected content found: } in the following line: }"))))
          }
        }
        "accept valid data" >> {
          def accept[A: EntityEncoder](body: A, expected: List[Data], mediaType: Option[MediaType] = None) =
            prop { (fileName: FileName, preExistingContent: Vector[Data]) =>
              val sampleFile = rootDir[Sandboxed] </> file1(fileName)
              val request = Request(uri = pathUri(sampleFile), method = method).withBody(body).unsafePerformSync
              val overrideContentType = mediaType.cata(mt => request.withContentType(`Content-Type`(mt).some), request)
              val (service, ref) = serviceRef(InMemState.fromFiles(Map(sampleFile -> preExistingContent)))
              val response = service(overrideContentType).unsafePerformSync
              response.as[String].unsafePerformSync must_=== ""
              response.status must_=== Status.Ok
              val expectedWithPreExisting =
                // PUT has override semantics
                if (method == Method.PUT) Map(sampleFile -> expected.toVector)
                // POST has append semantics
                else /*method == Method.POST*/ Map(sampleFile -> (preExistingContent ++ expected.toVector))
              ref.unsafePerformSync._1.contents must_=== expectedWithPreExisting
            }
          val expectedData = List(
            Data.Obj(ListMap("a" -> Data.Int(1))),
            Data.Obj(ListMap("b" -> Data.LocalTime(java.time.LocalTime.parse("12:34:56")))))
          "Json" >> {
            val line1 = Json("a" := 1)
            val preciseLine2 = Json("b" := Json("$localtime" := "12:34:56"))
            val readableLine2 = Json("b" := "12:34:56")
            "when formatted with one json object per line" >> {
              "Precise" >> {
                accept(List(line1, preciseLine2).map(PreciseJson(_)), expectedData)
              }
              "Readable" >> {
                accept(List(line1, readableLine2).map(ReadableJson(_)), expectedData)
              }
            }
            "when formatted as a single json array" >> {
              "Precise" >> {
                accept(PreciseJson(Json.array(line1, preciseLine2)), expectedData)
              }
              "Readable" >> {
                accept(ReadableJson(Json.array(line1, readableLine2)), expectedData)
              }
            }
            "when having multiple lines containing arrays" >> {
              val arbitraryValue = 3
              def replicate[A](a: A) = Applicative[Id].replicateM[A](arbitraryValue, a)
              "Precise" >> {
                accept(PreciseJson(Json.array(replicate(Json.array(line1, preciseLine2)): _*)), replicate(Data.Arr(expectedData)))
              }
              "Readable" >> {
                accept(ReadableJson(Json.array(replicate(Json.array(line1, readableLine2)): _*)), replicate(Data.Arr(expectedData)))
              }
            }
          }
          "CSV" >> {
            "standard" >> {
              accept(Csv("a,b\n1,\n,12:34:56"), expectedData)
            }
            "weird" >> {
              val specialCsvMediaType = MessageFormat.Csv(CsvParser.Format('|', '\'', '"', "\n"), None).mediaType
              val weirdData = List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Str("[1|2|3]"))))
              accept(Csv("a|b\n1|\n|'[1|2|3]'\n"), weirdData, Some(specialCsvMediaType))
            }.flakyTest
          }
        }
        "be 500 when error during writing" >> prop {
          (fileName: FileName, body: NonEmptyList[ReadableJson], failureMsg: String) =>
            val destination = rootDir[Sandboxed] </> file1(fileName)
            val request = Request(uri = pathUri(destination), method = method).withBody(body.list.toList)
            def service: HttpService = serviceErrs(
              InMemState.empty,
              FileSystemError.writeFailed(Data.Int(4), "anything but 4"))
            val response = request.flatMap(service.orNotFound(_)).unsafePerformSync
            response.status must_=== Status.InternalServerError
            response.as[String].unsafePerformSync must contain("anything but 4")
        }
      }
    }
    "PUT" >> {
      "download and re-upload directory" >> prop { (baseDir: ADir, rFile: RFile, content: NonEmptyList[RepresentableData]) =>
        val initialContent = Map((baseDir </> rFile) -> content.map(_.data).toVector)

        // NB: only Precise format preserves all possible Data
        val mediaType = jsonPreciseLine.mediaType

        val downloadRequest = Request(uri = pathUri(baseDir), headers = Headers(Accept(mediaType)))
        val (originalService, _) = serviceRef(InMemState.fromFiles(initialContent))
        val zippedResponse = originalService(downloadRequest).flatMap(_.as[ByteVector]).unsafePerformSync

        val uploadRequest = Request(uri = pathUri(baseDir), method = Method.PUT)
                              .withBody(zippedResponse).unsafePerformSync
                              .withContentType(`Content-Type`(MediaType.`application/zip`).some)
        val (emptyService, getState) = serviceRef(InMemState.empty)
        val uploadResponse = emptyService(uploadRequest).unsafePerformSync

        uploadResponse.as[String].unsafePerformSync must_=== ""
        uploadResponse.status must_=== Status.Ok
        getState.unsafePerformSync._1.contents must_=== initialContent
      }

      def utf8Bytes(str: String): Process[Task, ByteVector] =
        Process.eval(ByteVector.encodeUtf8(str).disjunction.fold(
          err => Task.fail(new RuntimeException(err.toString)),
          Task.now))

      "upload mixed content" >> prop { (baseDir: ADir, files: Map[RFile, MessageFormat]) =>
        files.size > 1 ==> {

          val content = Vector(Data.Obj("a" -> Data.Str("foo"), "b" -> Data.Bool(true)))

            val metadata = ArchiveMetadata(files.map { case (p, fmt) =>
            p -> FileMetadata(`Content-Type`(fmt.mediaType))
          })

          val metaBytes = utf8Bytes(EncodeJson.of[ArchiveMetadata].encode(metadata).pretty(minspace))

          val body = Zip.zipFiles[Task](
            files.map { case (p, fmt) =>
              p -> fmt.encode[Task](Process.emitAll(content))
                .flatMap(utf8Bytes)
            } + (ArchiveMetadata.HiddenFile -> metaBytes))

          val contentMap = files.map { case (p, _) => (baseDir </> p) -> content }

          val (service, ref) = serviceRef(InMemState.empty)
          (for {
            request <- Request(uri = pathUri(baseDir), method = Method.PUT)
              .withBody(body)
              .map(_.withContentType(`Content-Type`(MediaType.`application/zip`).some))
            response <- service(request)
            body <- response.as[String]
            state <- ref
          } yield {
            body must_=== ""
            response.status must_=== Status.Ok
            state._1.contents must_=== contentMap
          }).unsafePerformSync
        }
      }
      "fail upload with no metadata" >> prop { (baseDir: ADir, files: Map[RFile, MessageFormat]) =>
        val content = Vector(Data.Obj("a" -> Data.Str("foo"), "b" -> Data.Bool(true)))

        val body = Zip.zipFiles[Task](
          files.map { case (p, fmt) =>
            p -> fmt.encode[Task](Process.emitAll(content))
                  .flatMap(str => utf8Bytes(str))
          })

        val (service, ref) = serviceRef(InMemState.empty)
        (for {
          request <- Request(uri = pathUri(baseDir), method = Method.PUT)
                        .withBody(body)
                        .map(_.withContentType(`Content-Type`(MediaType.`application/zip`).some))
          response <- service(request)
          error    <- response.as[ApiError]
          state    <- ref
        } yield {
          response.status must_=== Status.BadRequest
          error must beApiErrorLike[DecodeFailure](
            InvalidMessageBodyFailure("metadata not found: " + posixCodec.printPath(ArchiveMetadata.HiddenFile)))
          state._1.contents must_=== Map.empty
        }).unsafePerformSync
      }
    }
    "MOVE" >> {
      def testMove[A: EntityDecoder, R: AsResult](
          from: APath,
          to: APath,
          state: InMemState,
          mounts: Map[APath, MountConfig],
          body: A => R,
          status: Status,
          newState: StateChange) = {
        // TODO: Consider if it's possible to invent syntax Move(...)
        val request = Request(
          uri = pathUri(from),
          headers = Headers(Header("Destination", UriPathCodec.printPath(to))),
          method = Method.MOVE)
        val (service, ref) = serviceRef(state, MountingsConfig(mounts))
        val response = service(request).unsafePerformSync
        response.status must_=== status
        body(response.as[A].unsafePerformSync)
        val expectedNewState = newState match {
          case Unchanged => (state.contents, mounts)
          case Changed(newContents, newMounts) => (newContents, newMounts)
        }
        val (actualNewState, newMounts) = ref.unsafePerformSync
        (actualNewState.contents must_= expectedNewState._1) and
        (newMounts must_= expectedNewState._2)
      }
      "be 400 for missing Destination header" >> prop { path: AFile =>
        val request = Request(uri = pathUri(path), method = Method.MOVE)
        val response = service(emptyMem)(request).unsafePerformSync
        response.as[ApiError].unsafePerformSync must beHeaderMissingError("Destination")
      }
      "be 404 for missing source file" >> prop { (file: AFile, destFile: AFile) =>
        file ≠ destFile ==> {
          testMove(
            from = file,
            to = destFile,
            state = emptyMem,
            mounts = Map.empty,
            status = Status.NotFound,
            body = (_: ApiError) must beApiErrorLike(pathNotFound(file)),
            newState = Unchanged)
        }
      }
      "be 400 if attempting to move a dir into a file" >> prop {(fs: NonEmptyDir, file: AFile) =>
        testMove(
          from = fs.dir,
          to = file,
          state = fs.state,
          mounts = Map.empty,
          status = Status.BadRequest,
          body = (_: ApiError) must beApiErrorWithMessage(
            Status.BadRequest withReason "Illegal move.",
            "srcPath" := fs.dir,
            "dstPath" := file),
          newState = Unchanged)
      }.set(minTestsOk = 10)  // NB: this test is slow because NonEmptyDir instances are still relatively large
      "be 400 if attempting to move a file into a dir" >> prop {(fs: SingleFileMemState, dir: ADir) =>
        testMove(
          from = fs.file,
          to = dir,
          state = fs.state,
          mounts = Map.empty,
          status = Status.BadRequest,
          body = (_: ApiError) must beApiErrorWithMessage(
            Status.BadRequest withReason "Illegal move.",
            "srcPath" := fs.file,
            "dstPath" := dir),
          newState = Unchanged)
      }
      "be 201 with file" >> prop {(fs: SingleFileMemState, file: AFile) =>
        (fs.file ≠ file) ==>
          testMove(
            from = fs.file,
            to = file,
            state = fs.state,
            mounts = Map.empty,
            status = Status.Created,
            body = (str: String) => str must_= "",
            newState = Changed(data = Map(file -> fs.contents)))
      }
      "be 201 with view file" >> prop {(src: AFile, dst: AFile, viewConfig: ViewConfig) =>
        (src ≠ dst) ==>
          testMove(
            from = src,
            to = dst,
            state = emptyMem,
            mounts = Map(src -> viewConfig),
            status = Status.Created,
            body = (str: String) => str must_= "",
            newState = Changed(mounts = Map(dst -> viewConfig)))
      }
      "be 201 with module directory" >> prop {(src: ADir, dst: ADir, moduleConfig: ModuleConfig) =>
        (src ≠ dst) ==>
          testMove(
            from = src,
            to = dst,
            state = emptyMem,
            mounts = Map(src -> moduleConfig),
            status = Status.Created,
            body = (str: String) => str must_= "",
            newState = Changed(mounts = Map(dst -> moduleConfig)))
      }
      "be 201 with dir containing data files, a view and a module" >> prop {(fs: NonEmptyDir, dir: ADir, moduleConfig: ModuleConfig, moduleDir: RDir, viewConfig: ViewConfig, viewFile: RFile) =>
        (fs.dir ≠ dir) ==>
          testMove(
            from = fs.dir,
            to = dir,
            state = fs.state,
            mounts = Map((fs.dir </> moduleDir) -> moduleConfig, (fs.dir </> viewFile) -> viewConfig),
            status = Status.Created,
            body = (str: String) => str must_=== "",
            newState = Changed(
              data = fs.filesInDir.map{ case (relFile,data) => (dir </> relFile, data)}.list.toList.toMap,
              mounts = Map((dir </> moduleDir) -> moduleConfig, (dir </> viewFile) -> viewConfig)))
      }.set(minTestsOk = 10)  // NB: this test is slow because NonEmptyDir instances are still relatively large

      "be 400 with file to same location" >> prop {(fs: SingleFileMemState) =>
        testMove(
          from = fs.file,
          to = fs.file,
          state = fs.state,
          mounts = Map.empty,
          status = Status.BadRequest,
          body = (_: ApiError) must_=== ApiError.fromMsg(
            Status.BadRequest withReason "Destination is same path as source",
            s"Destination is same path as source",
            "path" := fs.file),
          newState = Unchanged)
      }
      "be 400 with dir to same location" >> prop {(fs: NonEmptyDir) =>
        testMove(
          from = fs.dir,
          to = fs.dir,
          state = fs.state,
          mounts = Map.empty,
          status = Status.BadRequest,
          body = { err: ApiError =>
            (err.status.reason must_=== "Destination is same path as source") and
            (err.detail("path") must beSome)
          },
          newState = Unchanged)
      }.set(minTestsOk = 10).flakyTest("Gave up after only 1 passed tests. 10 tests were discarded.")
      // NB: this test is slow because NonEmptyDir instances are still relatively large
    }
    "COPY" >> {
      def testCopy[A: EntityDecoder, R: AsResult](
        from: APath,
        to: APath,
        state: InMemState,
        mounts: Map[APath, MountConfig],
        body: A => R,
        status: Status,
        newState: StateChange) = {
        // TODO: Consider if it's possible to invent syntax Copy(...)
        val request = Request(
          uri = pathUri(from),
          headers = Headers(Header("Destination", UriPathCodec.printPath(to))),
          method = Method.COPY)
        val (service, ref) = serviceRef(state, MountingsConfig(mounts))
        val response = service(request).unsafePerformSync
        response.status must_=== status
        body(response.as[A].unsafePerformSync)
        val expectedNewContents = newState match {
          case Unchanged => (state.contents, mounts)
          case c: Changed => (c.data, c.mounts)
        }
        val (newState0, newMounts) = ref.unsafePerformSync
        (newState0.contents must_= expectedNewContents._1) and
          (newMounts must_= expectedNewContents._2)
      }
      "be 400 for missing Destination header" >> prop { path: AFile =>
        val request = Request(uri = pathUri(path), method = Method.COPY)
        val response = service(emptyMem)(request).unsafePerformSync
        response.as[ApiError].unsafePerformSync must beHeaderMissingError("Destination")
      }
      "be 404 for missing source file" >> prop { (file: AFile, destFile: AFile) =>
        file ≠ destFile ==> {
          testCopy(
            from = file,
            to = destFile,
            state = emptyMem,
            mounts = Map.empty,
            status = Status.NotFound,
            body = (_: ApiError) must beApiErrorLike(pathNotFound(file)),
            newState = Unchanged)
        }
      }
      "be 400 if attempting to copy a dir into a file" >> prop {(fs: NonEmptyDir, file: AFile) =>
        testCopy(
          from = fs.dir,
          to = file,
          state = fs.state,
          mounts = Map.empty,
          status = Status.BadRequest,
          body = (_: ApiError) must beApiErrorWithMessage(
            Status.BadRequest withReason "Illegal move.",
            "srcPath" := fs.dir,
            "dstPath" := file),
          newState = Unchanged)
      }.set(minTestsOk = 10)  // NB: this test is slow because NonEmptyDir instances are still relatively large
      "be 400 if attempting to copy a file into a dir" >> prop {(fs: SingleFileMemState, dir: ADir) =>
        testCopy(
          from = fs.file,
          to = dir,
          state = fs.state,
          mounts = Map.empty,
          status = Status.BadRequest,
          body = (_: ApiError) must beApiErrorWithMessage(
            Status.BadRequest withReason "Illegal move.",
            "srcPath" := fs.file,
            "dstPath" := dir),
          newState = Unchanged)
      }
      "be 201 with file" >> prop {(fs: SingleFileMemState, file: AFile) =>
        (fs.file ≠ file) ==>
          testCopy(
            from = fs.file,
            to = file,
            state = fs.state,
            mounts = Map.empty,
            status = Status.Created,
            body = (str: String) => str must_=== "",
            newState = Changed(data = Map(file -> fs.contents, fs.file -> fs.contents)))
      }
      "be 201 with view file" >> prop {(src: AFile, dst: AFile, viewConfig: ViewConfig) =>
        (src ≠ dst) ==>
          testCopy(
            from = src,
            to = dst,
            state = InMemState.empty,
            mounts = Map(src -> viewConfig),
            status = Status.Created,
            body = (str: String) => str must_= "",
            newState = Changed(mounts = Map(
              src -> viewConfig,
              dst -> viewConfig)))
      }
      "be 201 with module directory" >> prop {(src: ADir, dst: ADir, moduleConfig: ModuleConfig) =>
        (src ≠ dst) ==>
          testCopy(
            from = src,
            to = dst,
            state = InMemState.empty,
            mounts = Map(src -> moduleConfig),
            status = Status.Created,
            body = (str: String) => str must_= "",
            newState = Changed(mounts = Map(
              src -> moduleConfig,
              dst -> moduleConfig)))
      }
      "be 201 with dir containing data files, a view and a module" >> prop {(fs: NonEmptyDir, dst: ADir, viewFile: RFile, viewConfig: ViewConfig, moduleDir: RDir, moduleConfig: ModuleConfig) =>
        (fs.dir ≠ dst) ==>
          testCopy(
            from = fs.dir,
            to = dst,
            state = fs.state,
            mounts = Map((fs.dir </> viewFile) -> viewConfig, (fs.dir </> moduleDir) -> moduleConfig),
            status = Status.Created,
            body = (str: String) => str must_= "",
            newState = Changed(
              data = fs.filesInDir.map{ case (relFile,data) => (dst </> relFile, data)}.list.toList.toMap ++ fs.state.contents,
              mounts = Map((fs.dir </> viewFile) -> viewConfig, (fs.dir </> moduleDir) -> moduleConfig, (dst </> viewFile) -> viewConfig, (dst </> moduleDir) -> moduleConfig)))
      }.set(minTestsOk = 10)  // NB: this test is slow because NonEmptyDir instances are still relatively large
      "be 400 with file to same location" >> prop {(fs: SingleFileMemState) =>
        testCopy(
          from = fs.file,
          to = fs.file,
          state = fs.state,
          mounts = Map.empty,
          status = Status.BadRequest,
          body = (_: ApiError) must_=== ApiError.fromMsg(
            Status.BadRequest withReason "Destination is same path as source",
            s"Destination is same path as source",
            "path" := fs.file),
          newState = Unchanged)
      }
      "be 400 with dir to same location" >> prop {(fs: NonEmptyDir) =>
        testCopy(
          from = fs.dir,
          to = fs.dir,
          state = fs.state,
          mounts = Map.empty,
          status = Status.BadRequest,
          body = { err: ApiError =>
            (err.status.reason must_=== "Destination is same path as source") and
              (err.detail("path") must beSome)
          },
          newState = Unchanged)
      }.set(minTestsOk = 10).flakyTest("Gave up after only 1 passed tests. 10 tests were discarded.")
      // NB: this test is slow because NonEmptyDir instances are still relatively large
    }
    "DELETE" >> {
      "be 204 with existing file" >> prop { filesystem: SingleFileMemState =>
        val request = Request(uri = pathUri(filesystem.file), method = Method.DELETE)
        val (service, ref) = serviceRef(filesystem.state)
        val response = service(request).unsafePerformSync
        response.status must_=== Status.NoContent
        ref.unsafePerformSync._1.contents must_=== Map() // The filesystem no longer contains that file
      }
      "be 204 with existing dir" >> prop { filesystem: NonEmptyDir =>
        val request = Request(uri = pathUri(filesystem.dir), method = Method.DELETE)
        val (service, ref) = serviceRef(filesystem.state)
        val response = service(request).unsafePerformSync
        response.status must_=== Status.NoContent
        ref.unsafePerformSync._1.contents must_=== Map() // The filesystem no longer contains that folder
      }.set(minTestsOk = 10)  // NB: this test is slow because NonEmptyDir instances are still relatively large
       .flakyTest("scalacheck: 'Gave up after only 1 passed tests. 11 tests were discarded.'")
      "be 404 with missing file" >> prop { file: AbsFile[Sandboxed] =>
        val request = Request(uri = pathUri(file), method = Method.DELETE)
        val response = service(emptyMem)(request).unsafePerformSync
        response.as[ApiError].unsafePerformSync must beApiErrorLike(pathNotFound(file))
      }
      "be 404 with missing dir" >> prop { dir: AbsDir[Sandboxed] =>
        val request = Request(uri = pathUri(dir), method = Method.DELETE)
        val response = service(emptyMem)(request).unsafePerformSync
        response.as[ApiError].unsafePerformSync must beApiErrorLike(pathNotFound(dir))
      }
    }
  }
}
