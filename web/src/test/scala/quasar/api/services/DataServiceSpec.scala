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

package quasar
package api
package services

import org.http4s.Uri.Authority
import org.http4s.server.middleware.GZip
import org.specs2.ScalaCheck
import org.specs2.execute.AsResult
import org.specs2.mutable.Specification
import pathy.scalacheck.AbsFileOf
import quasar.Data
import quasar.Predef._
import quasar.api.MessageFormat.JsonContentType
import JsonPrecision._
import JsonFormat._
import quasar.DataCodec

import argonaut.Json
import argonaut.Argonaut._
import org.http4s._
import org.http4s.headers._
import org.http4s.server._
import quasar.fp.numeric._
import quasar.fp.numeric.scalacheck._
import quasar.fs.{Path => _, _}
import pathy.Path
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz.scalacheck.ScalazArbitrary._
import quasar.fs.NumericArbitrary._
import quasar.DataArbitrary._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

import quasar.api.MessageFormatGen._

import org.scalacheck.{Arbitrary, Gen}

import eu.timepit.refined.numeric.Negative
import shapeless.tag.@@

import Fixture._

class DataServiceSpec extends Specification with ScalaCheck with FileSystemFixture with Http4s {
  import InMemory._

  def service(mem: InMemState): HttpService =
    data.service[FileSystem](runFs(mem).run)

  def serviceRef(mem: InMemState): (HttpService, Task[InMemState]) = {
    val (inter, ref) = runInspect(mem).run
    (data.service[FileSystem](inter compose fileSystem), ref)
  }

  implicit val arbFileName: Arbitrary[FileName] = Arbitrary(Gen.alphaStr.filter(_.nonEmpty).map(FileName(_)))

  val csv = MediaType.`text/csv`

  import posixCodec.printPath

  "Data Service" should {
    "GET" >> {
      "respond with NotFound" >> {
        "if file does not exist" ! prop { file: AbsFileOf[AlphaCharacters] =>
          val pathString = printPath(file.path)
          val response = service(InMemState.empty)(Request(uri = Uri(path = pathString))).run
          response.status must_== Status.NotFound
          response.as[Json].run must_== Json("error" := s"$pathString doesn't exist")
        }
      }
      "respond with file data" >> {
        def isExpectedResponse(data: Vector[Data], response: Response, format: MessageFormat) = {
          val expectedBody: Process[Task, String] = format.encode(Process.emitAll(data))
          response.as[String].run must_== expectedBody.runLog.run.mkString("")
          response.status must_== Status.Ok
          response.contentType must_== Some(`Content-Type`(format.mediaType, Charset.`UTF-8`))
        }
        "in correct format" >> {
          "readable and line delimited json by default" ! prop { filesystem: SingleFileMemState =>
            val response = service(filesystem.state)(Request(uri = Uri(path = filesystem.path))).run
            isExpectedResponse(filesystem.contents, response, MessageFormat.Default)
          }
          "in any supported format if specified" >> {
            "in the content-type header" >> {
              def testProp(format: MessageFormat) = prop { filesystem: SingleFileMemState => test(format, filesystem) }
              def test(format: MessageFormat, filesystem: SingleFileMemState) = {
                val request = Request(
                  uri = Uri(path = filesystem.path),
                  headers = Headers(Accept(format.mediaType)))
                val response = service(filesystem.state)(request).run
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
              "csv" ! prop { (filesystem: SingleFileMemState, format: MessageFormat.Csv) => test(format, filesystem) }
              "or a more complicated proposition" ! prop { filesystem: SingleFileMemState =>
                val request = Request(
                  uri = Uri(path = filesystem.path),
                  headers = Headers(Header("Accept", "application/ldjson;q=0.9;mode=readable,application/json;boundary=NL;mode=precise")))
                val response = service(filesystem.state)(request).run
                isExpectedResponse(filesystem.contents, response, JsonContentType(Precise, LineDelimited))
              }
            }
            "in the request-headers" ! prop { filesystem: SingleFileMemState =>
              val contentType = JsonContentType(Precise, LineDelimited)
              val request = Request(
                uri = Uri(path = filesystem.path).+?("request-headers", s"""{"Accept": "application/ldjson; mode=precise" }"""))
              val response = HeaderParam(service(filesystem.state))(request).run
              isExpectedResponse(filesystem.contents, response, JsonContentType(Precise, LineDelimited))
            }
          }
        }
        "with gziped encoding when specified" ! prop { filesystem: SingleFileMemState =>
          val request = Request(
            uri = Uri(path = filesystem.path),
            headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
          val response = GZip(service(filesystem.state))(request).run
          response.headers.get(headers.`Content-Encoding`) must_== Some(`Content-Encoding`(ContentCoding.gzip))
          response.status must_== Status.Ok
        }
        "support disposition" ! prop { filesystem: SingleFileMemState =>
          val disposition = `Content-Disposition`("attachement", Map("filename" -> "data.json"))
          val request = Request(
            uri = Uri(path = filesystem.path),
            headers = Headers(Accept(jsonReadableLine.copy(disposition = Some(disposition)).mediaType)))
          val response = service(filesystem.state)(request).run
          response.headers.get(`Content-Disposition`) must_== Some(disposition)
        }
        "support offset and limit" >> {
          "return expected result if user supplies valid values" ! prop {
            (filesystem: SingleFileMemState, offset: Natural, limit: Positive, format: MessageFormat) =>
              val request = Request(
                uri = Uri(path = filesystem.path).+?("offset", offset.shows).+?("limit", limit.shows),
                headers = Headers(Accept(format.mediaType)))
              val response = service(filesystem.state)(request).run
              isExpectedResponse(filesystem.contents.drop(offset.toInt).take(limit.toInt), response, format)
          }
          "return 400 if provided with" >> {
            "a non-positive limit (0 is invalid)" ! prop { (path: AbsFile[Sandboxed], offset: Natural, limit: Int) =>
              (limit < 1) ==> {
                val request = Request(
                  uri = Uri(path = printPath(path)).+?("offset", offset.shows).+?("limit", limit.shows))
                val response = service(InMemState.empty)(request).run
                response.status must_== Status.BadRequest
                response.as[String].run must_== s"invalid limit: $limit (must be >= 1)"
              }
            }
            "a negative offset" ! prop { (path: AbsFile[Sandboxed], offset: Long @@ Negative, limit: Positive) =>
              val request = Request(
                uri = Uri(path = printPath(path)).+?("offset", offset.shows).+?("limit", limit.shows))
              val response = service(InMemState.empty)(request).run
              response.status must_== Status.BadRequest
              response.as[String].run must_== s"invalid offset: $offset (must be >= 0)"
            }
            "if provided with multiple limits?" ! prop { (path: AbsFile[Sandboxed], offset: Natural, limit1: Positive, limit2: Positive, otherLimits: List[Positive]) =>
              val limits = limit1 :: limit2 :: otherLimits
              val request = Request(
                uri = Uri(path = printPath(path)).+?("offset", offset.shows).+?("limit", limits.map(_.shows)))
              val response = service(InMemState.empty)(request).run
              response.status must_== Status.BadRequest
              response.as[String].run must_== s"Two limits were provided, only supply one limit"
            }.pendingUntilFixed("SD-1082")
            "if provided with multiple offsets?" ! prop { (path: AbsFile[Sandboxed], limit: Positive, offsets: List[Natural]) =>
              (offsets.length >= 2) ==> {
                val request = Request(
                  uri = Uri(path = printPath(path)).+?("offset", offsets.map(_.shows)).+?("limit", limit.shows))
                val response = service(InMemState.empty)(request).run
                response.status must_== Status.BadRequest
                response.as[String].run must_== s"Two offsets were provided, only supply one offset"
                todo // Confirm this is the expected behavior because http4s defaults to just grabbing the first one
                     // and going against that default behavior would be more work
              }
            }.pendingUntilFixed("SD-1082")
            "an unparsable limit" ! prop { path: AbsFile[Sandboxed] =>
              val request = Request(
                uri = Uri(path = printPath(path)).+?("limit", "a"))
              val response = service(InMemState.empty)(request).run
              response.status must_== Status.BadRequest
              response.as[String].run must_== s"""invalid limit: Query decoding Long failed (For input string: "a")"""
            }
            "if provided with both an invalid offset and limit" ! prop { (path: AbsFile[Sandboxed], limit: Int, offset: Long @@ Negative) =>
              (limit < 1) ==> {
                val request = Request(
                  uri = Uri(path = printPath(path)).+?("limit", limit.shows).+?("offset", offset.shows))
                val response = service(InMemState.empty)(request).run
                response.status must_== Status.BadRequest
                response.as[String].run must_== s"invalid limit: $limit (must be >= 1), invalid offset: $offset (must be >= 0)"
              }
            }.pendingUntilFixed("SD-1083")
          }
        }
        "support very large data set" >> {
          val sampleFile = rootDir[Sandboxed] </> dir("foo") </> file("bar")
          val samplePath: String = printPath(sampleFile)
          def fileSystemWithSampleFile(data: Vector[Data]) = InMemState fromFiles Map(sampleFile -> data)
          val data = (0 until 100*1000).map(n => Data.Obj(ListMap("n" -> Data.Int(n)))).toVector
          "plain text" >> {
            val request = Request(
              uri = Uri(path = samplePath))
            val response = service(fileSystemWithSampleFile(data))(request).run
            isExpectedResponse(data, response, MessageFormat.Default)
          }
          "gziped" >> {
            val request = Request(
              uri = Uri(path = samplePath),
              headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
            val response = service(fileSystemWithSampleFile(data))(request).run
            isExpectedResponse(data, response, MessageFormat.Default)
          }
        }
      }
      "using disposition to download as zipped directory" ! prop { filesystem: NonEmptyDir =>
        val dirPath = printPath(filesystem.dir)
        val disposition = `Content-Disposition`("attachement", Map("filename" -> "foo.zip"))
        val requestMediaType = MediaType.`text/csv`.withExtensions(Map("disposition" -> disposition.value))
        val request = Request(
          uri = Uri(path = dirPath),
          headers = Headers(Accept(requestMediaType)))
        val response = service(filesystem.state)(request).run
        response.status must_== Status.Ok
        response.contentType must_== Some(`Content-Type`(MediaType.`application/zip`))
        response.headers.get(`Content-Disposition`) must_== Some(disposition)
      }.set(minTestsOk = 1) // This test is relatively slow
      "what happens if user specifies a Path that is a directory but without the appropriate headers?" >> todo
    }
    "POST and PUT" >> {
      def testBoth[A](test: org.http4s.Method => Unit) = {
        "POST" should {
          test(org.http4s.Method.POST)
        }
        "PUT" should {
          test(org.http4s.Method.PUT)
        }
      }
      testBoth { method =>
        "be 415 if media-type is missing" ! prop { file: Path[Abs,File,Sandboxed] =>
          val path = printPath(file)
          val request = Request(
            uri = Uri(path = path),
            method = method).withBody("{\"a\": 1}\n{\"b\": \"12:34:56\"}").run
          val response = service(emptyMem)(request).run
          response.status must_== Status.UnsupportedMediaType
          response.as[String].run must_== "No media-type is specified in Content-Type header"
        }
        "be 400 with" >> {
          def be400[A: EntityDecoder](body: String, expectedBody: A, mediaType: MediaType = jsonReadableLine.mediaType) = {
            prop { file: AFile =>
              val path = printPath(file)
              val request = Request(
                uri = Uri(path = path),             // We do it this way becuase withBody sets the content-type
                method = method).withBody(body).run.replaceAllHeaders(`Content-Type`(mediaType, Charset.`UTF-8`))
              val (service, ref) = serviceRef(emptyMem)
              val response = service(request).run
              response.as[A].run must_== expectedBody
              response.status must_== Status.BadRequest
              ref.run must_== emptyMem
            }
          }
          "invalid body" >> {
            "no body" ! be400(body = "", expectedBody = "Request has no body")
            "invalid JSON" ! be400(
              body = "{",
              expectedBody = Json("error" := "some uploaded value(s) could not be processed",
                                  "details" := Json.array(jString("parse error: JSON terminates unexpectedly. in the following line: {")))
            )
            "invalid CSV" >> {
              "empty (no headers)" ! be400(
                body = "",
                expectedBody = "Request has no body",
                mediaType = csv
              )
              "if broken (after the tenth data line)" ! {
                val brokenBody = "\"a\",\"b\"\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n\",\n"
                be400(
                  body = brokenBody,
                  expectedBody = Json("error" := "some uploaded value(s) could not be processed",
                    "details" := Json.array(jString("parse error: Malformed Input!: Some(\",\n)"))),
                  mediaType = csv)
              }
            }
          }
          // TODO: Consider spliting this into a case of Root (depth == 0) and missing dir (depth > 1)
          "if path is invalid (parent directory does not exist)" ! prop { (file: AFile, json: Json) =>
            Path.depth(file) != 1 ==> {
              be400(body = json.spaces4, s"Invalid path: ${printPath(file)}")
            }
          }.pendingUntilFixed("What do we want here, create it or not?")
          "produce two errors with partially invalid JSON" ! prop { path: Path[Abs,File,Sandboxed] =>
            val twoErrorJson = """{"a": 1}
                                 |"unmatched
                                 |{"b": 2}
                                 |}
                                 |{"c": 3}""".stripMargin
            val expectedResponse = Json("error" := "some uploaded value(s) could not be processed",
              "details" := Json.array(
                jString("parse error: JSON terminates unexpectedly. in the following line: \"unmatched"),
                jString("parse error: Unexpected content found: } in the following line: }")
              ))
            be400(twoErrorJson, expectedResponse)
          }
        }
        "accept valid data" >> {
          def accept[A: EntityEncoder](body: A, expected: List[Data]) =
            prop { (fileName: FileName, preExistingContent: Vector[Data]) =>
              val sampleFile = rootDir[Sandboxed] </> file1(fileName)
              val path = printPath(sampleFile)
              val request = Request(uri = Uri(path = path), method = method).withBody(body).run
              val (service, ref) = serviceRef(InMemState.fromFiles(Map(sampleFile -> preExistingContent)))
              val response = service(request).run
              response.as[String].run must_== ""
              response.status must_== Status.Ok
              val expectedWithPreExisting =
                // PUT has override semantics
                if (method == Method.PUT) Map(sampleFile -> expected.toVector)
                // POST has append semantics
                else /*method == Method.POST*/ Map(sampleFile -> (preExistingContent ++ expected.toVector))
              ref.run.contents must_== expectedWithPreExisting
            }
          val expectedData = List(
            Data.Obj(ListMap("a" -> Data.Int(1))),
            Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))
          "Json" >> {
            val line1 = Json("a" := 1)
            val preciseLine2 = Json("b" := Json("$time" := "12:34:56"))
            val readableLine2 = Json("b" := "12:34:56")
            "when formatted with one json object per line" >> {
              "Precise" ! {
                accept(List(line1, preciseLine2).map(PreciseJson(_)), expectedData)
              }.pendingUntilFixed("SD-1066")
              "Readable" ! {
                accept(List(line1, readableLine2).map(ReadableJson(_)), expectedData)
              }
            }
            "when formatted as a single json array" >> {
              "Precise" ! {
                accept(PreciseJson(Json.array(line1, preciseLine2)), expectedData)
              }.pendingUntilFixed("SD-1066")
              "Readable" ! {
                accept(ReadableJson(Json.array(line1, readableLine2)), expectedData)
              }
            }
            "when having multiple lines containing arrays" >> {
              val arbitraryValue = 3
              def replicate[A](a: A) = Applicative[Id].replicateM[A](arbitraryValue, a)
              "Precise" ! {
                accept(PreciseJson(Json.array(replicate(Json.array(line1, preciseLine2)): _*)), replicate(Data.Arr(expectedData)))
              }.pendingUntilFixed("SD-1066")
              "Readable" ! {
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
            }
          }
        }
        "be 500 with server side error" ! prop {
          (fileName: FileName, body: NonEmptyList[ReadableJson], failureMsg: String) =>
            import quasar.api.Server._
            val port = quasar.api.Server.anyAvailablePort.run
            val destination = rootDir[Sandboxed] </> file1(fileName)
            val request = Request(
              uri = Uri(path = printPath(destination), authority = Some(Authority(port = Some(port)))),
              method = method).withBody(body.list).run
            val failInter = new (FileSystem ~> Task) {
              def apply[A](a: FileSystem[A]): Task[Nothing] = Task.fail(new RuntimeException(failureMsg))
            }
            val service = data.service(failInter)
            val serverBlueprint = ServerBlueprint(port, scala.concurrent.duration.Duration.Inf,ListMap("" -> service))
            val (server, _) = startServer(serverBlueprint,true).run
            val client = org.http4s.client.blaze.defaultClient
            val response = client(request).onFinish(_ => server.shutdown.void).run
            response.status must_== Status.InternalServerError
            response.as[String].run must_== ""
        }
        () // Required for testBoth (not required with specs2 3.x)
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
          uri = Uri(path = printPath(from)),
          headers = Headers(Header("Destination", printPath(to))),
          method = Method.MOVE)
        val (service, ref) = serviceRef(state)
        val response = service(request).run
        response.status must_== status
        body(response.as[A].run)
        val expectedNewContents = newState match {
          case Unchanged => state.contents
          case Changed(newContents) => newContents
        }
        ref.run.contents must_== expectedNewContents
      }
      "be 400 for missing Destination header" ! prop { path: AbsFile[Sandboxed] =>
        val pathString = printPath(path)
        val request = Request(uri = Uri(path = pathString), method = Method.MOVE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.BadRequest
        response.as[Json].run must_== Json("error" := "The 'Destination' header must be specified")
      }
      "be 404 for missing source file" ! prop { (file: AbsFileOf[AlphaCharacters], destFile: AbsFileOf[AlphaCharacters]) =>
        testMove(
          from = file.path,
          to = destFile.path,
          state = emptyMem,
          status = Status.NotFound,
          body = (json: Json) => json must_== Json("error" := s"${printPath(file.path)} doesn't exist"),
          newState = Unchanged)
      }
      "be 400 if attempting to move a dir into a file" ! prop {(fs: NonEmptyDir, file: AbsFile[Sandboxed]) =>
        testMove(
          from = fs.dir,
          to = file,
          state = fs.state,
          status = Status.BadRequest,
          body = (str: String) => str must_== "Cannot move directory into a file",
          newState = Unchanged)
      }
      "be 400 if attempting to move a file into a dir" ! prop {(fs: SingleFileMemState, dir: AbsDir[Sandboxed]) =>
        testMove(
          from = fs.file,
          to = dir,
          state = fs.state,
          status = Status.BadRequest,
          body = (str: String) => str must_== "Cannot move a file into a directory, must specify destination precisely",
          newState = Unchanged)
      }
      "be 201 with file" ! prop {(fs: SingleFileMemState, file: AFile) =>
        (fs.file ≠ file) ==>
          testMove(
            from = fs.file,
            to = file,
            state = fs.state,
            status = Status.Created,
            body = (str: String) => str must_== "",
            newState = Changed(Map(file -> fs.contents)))
      }
      "be 201 with dir" ! prop {(fs: NonEmptyDir, dir: AbsDir[Sandboxed]) =>
        (fs.dir ≠ dir) ==>
          testMove(
            from = fs.dir,
            to = dir,
            state = fs.state,
            status = Status.Created,
            body = (str: String) => str must_== "",
            newState = Changed(fs.filesInDir.map{ case (relFile,data) => (dir </> relFile.path, data)}.list.toMap))
      }
      "be 409 with file to same location" ! prop {(fs: SingleFileMemState) =>
        testMove(
          from = fs.file,
          to = fs.file,
          state = fs.state,
          status = Status.Conflict,
          body = (json: Json) => json must_== Json("error" := s"${printPath(fs.file)} already exists"),
          newState = Unchanged)
      }
      "be 409 with dir to same location" ! prop {(fs: NonEmptyDir) =>
        testMove(
          from = fs.dir,
          to = fs.dir,
          state = fs.state,
          status = Status.Conflict,
          body = (json: Json) => json must beOneOf(
            fs.filesInDir.list.map { case (p, _) =>
              Json("error" := s"${printPath(fs.dir </> p.path)} already exists")
            }: _*),
          newState = Unchanged)
      }
    }
    "DELETE" >> {
      "be 200 with existing file" ! prop { filesystem: SingleFileMemState =>
        val request = Request(uri = Uri(path = filesystem.path), method = Method.DELETE)
        val (service, ref) = serviceRef(filesystem.state)
        val response = service(request).run
        response.status must_== Status.Ok
        ref.run.contents must_== Map() // The filesystem no longer contains that file
      }
      "be 200 with existing dir" ! prop { filesystem: NonEmptyDir =>
        val dirPath = printPath(filesystem.dir)
        val request = Request(uri = Uri(path = dirPath), method = Method.DELETE)
        val (service, ref) = serviceRef(filesystem.state)
        val response = service(request).run
        response.status must_== Status.Ok
        ref.run.contents must_== Map() // The filesystem no longer contains that folder
      }
      "be 404 with missing file" ! prop { file: AbsFile[Sandboxed] =>
        val path = printPath(file)
        val request = Request(uri = Uri(path = path), method = Method.DELETE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.NotFound
      }
      "be 404 with missing dir" ! prop { dir: AbsDir[Sandboxed] =>
        val dirPath = printPath(dir)
        val request = Request(uri = Uri(path = dirPath), method = Method.DELETE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.NotFound
      }
    }
  }
}
