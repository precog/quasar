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

import scala.Predef.$conforms
import quasar.Predef._
import quasar.Data
import quasar.DataArbitrary._
import quasar.api._,
  ApiErrorEntityDecoder._, MessageFormat.JsonContentType, MessageFormatGen._
import quasar.api.matchers._
import quasar.contrib.pathy._, PathArbitrary._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._

import argonaut.Json
import argonaut.Argonaut._
import org.http4s._
import org.http4s.headers._
import org.http4s.server.middleware.GZip
import org.specs2.specification.core.Fragment
import org.specs2.execute.AsResult
import org.specs2.matcher.MatchResult
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import pathy.scalacheck.PathyArbitrary._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.stream.Process

import quasar.api.MessageFormatGen._

import eu.timepit.refined.numeric.{NonNegative, Negative, Positive => RPositive}
import eu.timepit.refined.auto._
import eu.timepit.refined.scalacheck.numeric._
import shapeless.tag.@@
import quasar.api.PathUtils._

class DataServiceSpec extends quasar.Qspec with FileSystemFixture with Http4s {
  import Fixture._, InMemory._, JsonPrecision._, JsonFormat._
  import FileSystemFixture.{ReadWriteT, ReadWrites, amendWrites}
  import PathError.{pathExists, pathNotFound}

  type Eff0[A] = Coproduct[FileSystemFailure, FileSystem, A]
  type Eff[A]  = Coproduct[Task, Eff0, A]
  type EffM[A] = Free[Eff, A]

  def effRespOr(fs: FileSystem ~> Task): Eff ~> ResponseOr =
    liftMT[Task, ResponseT]              :+:
    failureResponseOr[FileSystemError]   :+:
    (liftMT[Task, ResponseT] compose fs)

  def service(mem: InMemState): HttpService =
    HttpService.lift(req => runFs(mem) flatMap (fs =>
      data.service[Eff].toHttpService(effRespOr(fs)).apply(req)))

  def serviceRef(mem: InMemState): (HttpService, Task[InMemState]) = {
    val (inter, ref) = runInspect(mem).unsafePerformSync
    val svc = HttpService.lift(req =>
      data.service[Eff]
        .toHttpService(effRespOr(inter compose fileSystem))
        .apply(req))

    (svc, ref)
  }

  def restrict[M[_], S[_], T[_]](f: T ~> M)(implicit S: S :<: T) =
    f compose injectNT[S, T]

  def serviceErrs(mem: InMemState, writeErrors: FileSystemError*): HttpService = {
    type RW[A] = ReadWriteT[ResponseOr, A]
    HttpService.lift(req => runFs(mem) flatMap { fs =>
      val fs0: Eff ~> ResponseOr = effRespOr(fs)
      val g: WriteFile ~> RW = amendWrites(restrict[ResponseOr, WriteFile, Eff](fs0))
      val f: Eff ~> RW = liftMT[ResponseOr, ReadWriteT] compose fs0

      val fsErrs: Eff ~> ResponseOr =
        evalNT[ResponseOr, ReadWrites]((Nil, List(writeErrors.toVector))) compose free.transformIn(g, f)

      data.service[Eff].toHttpService(fsErrs).apply(req)
    })
  }

  val csv = MediaType.`text/csv`

  "Data Service" should {
    "GET" >> {
      "respond with empty response" >> {
        "if file does not exist" >> prop { file: AFile =>
          val response = service(InMemState.empty)(Request(uri = pathUri(file))).unsafePerformSync
          response.status must_= Status.Ok
          response.as[String].unsafePerformSync must_= ""
        }
      }
      "respond with file data" >> {
        def isExpectedResponse(data: Vector[Data], response: Response, format: MessageFormat) = {
          val expectedBody: Process[Task, String] = format.encode(Process.emitAll(data))
          response.as[String].unsafePerformSync must_== expectedBody.runLog.unsafePerformSync.mkString("")
          response.status must_== Status.Ok
          response.contentType must_== Some(`Content-Type`(format.mediaType, Charset.`UTF-8`))
        }
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
              val response = HeaderParam(service(filesystem.state))(request).unsafePerformSync
              isExpectedResponse(filesystem.contents, response, JsonContentType(Precise, LineDelimited))
            }
          }
        }
        "with gziped encoding when specified" >> prop { filesystem: SingleFileMemState =>
          val request = Request(
            uri = pathUri(filesystem.file),
            headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
          val response = GZip(service(filesystem.state))(request).unsafePerformSync
          response.headers.get(headers.`Content-Encoding`) must_== Some(`Content-Encoding`(ContentCoding.gzip))
          response.status must_== Status.Ok
        }
        "support disposition" >> prop { filesystem: SingleFileMemState =>
          val disposition = `Content-Disposition`("attachement", Map("filename" -> "data.json"))
          val request = Request(
            uri = pathUri(filesystem.file),
            headers = Headers(Accept(jsonReadableLine.copy(disposition = Some(disposition)).mediaType)))
          val response = service(filesystem.state)(request).unsafePerformSync
          response.headers.get(`Content-Disposition`.name) must_== Some(disposition)
        }
        "support disposition non-ascii filename" >> prop { filesystem: SingleFileMemState =>
          val disposition = `Content-Disposition`("attachement", Map("filename*" -> "UTF-8''Na%C3%AFve%20file.txt"))
          val request = Request(
            uri = pathUri(filesystem.file),
            headers = Headers(Accept(jsonReadableLine.copy(disposition = Some(disposition)).mediaType)))
          val response = service(filesystem.state)(request).unsafePerformSync
          response.headers.get(`Content-Disposition`.name) must_== Some(disposition)
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
              response.status must_== Status.BadRequest
              response.as[Json].unsafePerformSync must_== Json("error" := s"Two limits were provided, only supply one limit")
            }.pendingUntilFixed("SD-1082")
            "if provided with multiple offsets?" >> prop { (path: AbsFile[Sandboxed], limit: Positive, offsets: List[Natural]) =>
              (offsets.length >= 2) ==> {
                val request = Request(
                  uri = pathUri(path).+?("offset", offsets.map(_.shows)).+?("limit", limit.shows))
                val response = service(InMemState.empty)(request).unsafePerformSync
                response.status must_== Status.BadRequest
                response.as[Json].unsafePerformSync must_== Json("error" := s"Two limits were provided, only supply one limit")
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
                response.status must_== Status.BadRequest
                response.as[Json].unsafePerformSync must_== Json("error" := s"invalid limit: $limit (must be >= 1), invalid offset: $offset (must be >= 0)")
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
            isExpectedResponse(data, response, MessageFormat.Default)
          }
        }
      }
      "download as zipped directory" >> prop { filesystem: NonEmptyDir =>
        val disposition = `Content-Disposition`("attachment", Map("filename" -> "foo.zip"))
        val requestMediaType = MediaType.`text/csv`.withExtensions(Map("disposition" -> disposition.value))
        val request = Request(
          uri = pathUri(filesystem.dir),
          headers = Headers(Accept(requestMediaType)))
        val response = service(filesystem.state)(request).unsafePerformSync
        response.status must_== Status.Ok
        response.contentType must_== Some(`Content-Type`(MediaType.`application/zip`))
        response.headers.get(`Content-Disposition`) must_== Some(disposition)
      }.set(minTestsOk = 10).flakyTest("scalacheck: Gave up after only 2 passed tests. 12 tests were discarded.")  // NB: this test is slow because NonEmptyDir instances are still relatively large
      "what happens if user specifies a Path that is a directory but without the appropriate headers?" >> todo
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
            ref.unsafePerformSync must_== emptyMem
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
              be400(file, reqBody = json.spaces4, (_: Json) must_== Json("error" := s"Invalid path: ${posixCodec.printPath(file)}"))
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
          def accept[A: EntityEncoder](body: A, expected: List[Data]) =
            prop { (fileName: FileName, preExistingContent: Vector[Data]) =>
              val sampleFile = rootDir[Sandboxed] </> file1(fileName)
              val request = Request(uri = pathUri(sampleFile), method = method).withBody(body).unsafePerformSync
              val (service, ref) = serviceRef(InMemState.fromFiles(Map(sampleFile -> preExistingContent)))
              val response = service(request).unsafePerformSync
              response.as[String].unsafePerformSync must_== ""
              response.status must_== Status.Ok
              val expectedWithPreExisting =
                // PUT has override semantics
                if (method == Method.PUT) Map(sampleFile -> expected.toVector)
                // POST has append semantics
                else /*method == Method.POST*/ Map(sampleFile -> (preExistingContent ++ expected.toVector))
              ref.unsafePerformSync.contents must_== expectedWithPreExisting
            }
          val expectedData = List(
            Data.Obj(ListMap("a" -> Data.Int(1))),
            Data.Obj(ListMap("b" -> Data.Time(java.time.LocalTime.parse("12:34:56")))))
          "Json" >> {
            val line1 = Json("a" := 1)
            val preciseLine2 = Json("b" := Json("$time" := "12:34:56"))
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
              val weirdData = List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Str("[1|2|3]"))))
              accept(Csv("a|b\n1|\n|'[1|2|3]'\n"), weirdData)
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
            val response = request.flatMap(service(_)).unsafePerformSync
            response.status must_== Status.InternalServerError
            response.as[String].unsafePerformSync must contain("anything but 4")
        }
      }
    }
    "MOVE" >> {
      trait StateChange
      object Unchanged extends StateChange
      case class Changed(newContents: FileMap) extends StateChange

      def testMove[A: EntityDecoder, R: AsResult](
          from: APath,
          to: APath,
          state: InMemState,
          body: A => R,
          status: Status,
          newState: StateChange) = {
        // TODO: Consider if it's possible to invent syntax Move(...)
        val request = Request(
          uri = pathUri(from),
          headers = Headers(Header("Destination", UriPathCodec.printPath(to))),
          method = Method.MOVE)
        val (service, ref) = serviceRef(state)
        val response = service(request).unsafePerformSync
        response.status must_=== status
        body(response.as[A].unsafePerformSync)
        val expectedNewContents = newState match {
          case Unchanged => state.contents
          case Changed(newContents) => newContents
        }
        ref.unsafePerformSync.contents must_=== expectedNewContents
      }
      "be 400 for missing Destination header" >> prop { path: AbsFile[Sandboxed] =>
        val request = Request(uri = pathUri(path), method = Method.MOVE)
        val response = service(emptyMem)(request).unsafePerformSync
        response.as[ApiError].unsafePerformSync must beHeaderMissingError("Destination")
      }
      "be 404 for missing source file" >> prop { (file: AFile, destFile: AFile) =>
        testMove(
          from = file,
          to = destFile,
          state = emptyMem,
          status = Status.NotFound,
          body = (_: ApiError) must beApiErrorLike(pathNotFound(file)),
          newState = Unchanged)
      }
      "be 400 if attempting to move a dir into a file" >> prop {(fs: NonEmptyDir, file: AFile) =>
        testMove(
          from = fs.dir,
          to = file,
          state = fs.state,
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
            status = Status.Created,
            body = (str: String) => str must_== "",
            newState = Changed(Map(file -> fs.contents)))
      }
      "be 201 with dir" >> prop {(fs: NonEmptyDir, dir: ADir) =>
        (fs.dir ≠ dir) ==>
          testMove(
            from = fs.dir,
            to = dir,
            state = fs.state,
            status = Status.Created,
            body = (str: String) => str must_== "",
            newState = Changed(fs.filesInDir.map{ case (relFile,data) => (dir </> relFile, data)}.list.toList.toMap))
      }.set(minTestsOk = 10)  // NB: this test is slow because NonEmptyDir instances are still relatively large
      "be 409 with file to same location" >> prop {(fs: SingleFileMemState) =>
        testMove(
          from = fs.file,
          to = fs.file,
          state = fs.state,
          status = Status.Conflict,
          body = (_: ApiError) must beApiErrorLike(pathExists(fs.file)),
          newState = Unchanged)
      }
      "be 409 with dir to same location" >> prop {(fs: NonEmptyDir) =>
        testMove(
          from = fs.dir,
          to = fs.dir,
          state = fs.state,
          status = Status.Conflict,
          body = { err: ApiError =>
            (err.status.reason must_== "Path exists.") and
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
        response.status must_== Status.NoContent
        ref.unsafePerformSync.contents must_== Map() // The filesystem no longer contains that file
      }
      "be 204 with existing dir" >> prop { filesystem: NonEmptyDir =>
        val request = Request(uri = pathUri(filesystem.dir), method = Method.DELETE)
        val (service, ref) = serviceRef(filesystem.state)
        val response = service(request).unsafePerformSync
        response.status must_== Status.NoContent
        ref.unsafePerformSync.contents must_== Map() // The filesystem no longer contains that folder
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
