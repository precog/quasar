package quasar.api

import quasar._
import quasar.Predef._
import quasar.recursionschemes.Fix

import quasar.config._
import quasar.fs._
import quasar.specs2._

import scalaz._, Scalaz._
import scalaz.concurrent._

import scalaz.stream.text.utf8Decode

import argonaut._, Argonaut._

import org.http4s._
import org.http4s.Uri
import org.http4s.Method._
import org.http4s.Status._
import org.http4s.headers._
import org.http4s.MediaType._
import org.http4s.client.{Client, blaze}
import org.http4s.util.UrlCodingUtils.urlEncode
import org.http4s.argonaut.jsonOf
import org.http4s.Method

import org.specs2.mutable._
import org.specs2.time.NoTimeConversions

import Utils._

class ApiSpecs extends Specification with PendingWithAccurateCoverage with NoTimeConversions with BackendStub {
  sequential  // The tests around restarting the server do not pass if run in parallel.
              // TODO: Explore why that is and/or find a way to extract them so the rest of the tests can run in parallel
  args.report(showtimes = true)

  "OPTIONS" should {
    def queryFs(base: Uri) = base / "query" / "fs" / ""

    "advertise GET and POST for /query path" in {
      withoutBackend { (client, baseUri) =>
        cors(
          Request(method = OPTIONS, uri = queryFs(baseUri)), client, Set(GET, POST)
        )
      }
    }
    
    "advertise GET, PUT, POST, DELETE, and MOVE for /data path" in {
      withoutBackend { (client, baseUri) =>
        cors(
          Request(method = OPTIONS, uri = queryFs(baseUri)),
          client,
          Set(GET, PUT, POST, DELETE, MOVE)
        )
      }
    }

    "advertise Destination header for /query path and method POST" in {
      withoutBackend { (client, baseUri) =>
        corsHeaders(queryFs(baseUri), client, POST)(Destination)
      }
    }
    
    "advertise Destination header for /data path and method MOVE" in {
      withoutBackend { (client, baseUri) =>
        corsHeaders(queryFs(baseUri), client, MOVE)(Destination)
      }
    }
  }

  "/metadata/fs" should {
    def metadataFs(base: Uri) = base / "metadata" / "fs" / "" // Note: trailing slash required

    "return no filesystems" in {
      withoutBackend { (client, baseUri) =>
        expectFs(metadataFs(baseUri), client)(Nil).run
      }
    }
    
    "be NotFound with missing backend" in {
      withoutBackend { (client, baseUri) =>
        notFoundUri(metadataFs(baseUri) / "missing", client)("").run
      }
    }
    "return empty for null fs" in {
      withBackends { (client, baseUri) =>
        expectFs(metadataFs(baseUri) / "empty" / "", client)(Nil).run
      }
    }
    
    "return empty for missing path" in {
      withBackends { (client, baseUri) =>
        expectFs(metadataFs(baseUri) / "foo" / "baz" / "", client)(Nil).run
      }
    }

    "find stubbed files" in {
      withBackends { (client, baseUri) =>
        expectFs(metadataFs(baseUri) / "foo" / "", client)(List(
          Mount("a file","file"), 
          Mount("bar","file"), 
          Mount("dir","directory"), 
          Mount("empty","file"), 
          Mount("quoting","file"), 
          Mount("tmp","directory"))
        ).run
      }
    }
    
    "find intermediate directory" in {
      withBackends { (client, baseUri) =>
        expectFs(metadataFs(baseUri) / "non" / "", client)(
          List(Mount("root","directory"))
        ).run
      }
    }

    "find nested mount" in {
      withBackends { (client, baseUri) =>
        expectFs(metadataFs(baseUri) / "non" / "root" / "", client)(
          List(Mount("mounting","mount"))
        ).run
      }
    }
    
    "be NotFound for file with same name as existing directory (minus the trailing slash)" in {
      withBackends { (client, baseUri) =>
        notFoundUri(metadataFs(baseUri) / "foo", client)("").run
      }
    }
      
    "be empty for file" in {
      withBackends { (client, baseUri) =>
        client(Request(uri = metadataFs(baseUri) / "foo" / "bar")).
          flatMap(_.as[String]).
          flatMap(v => Task.now(_root_.argonaut.Parse.parse(v))).
          run ==== 
        \/-(Json.obj())
      }
    }

    "also contain CORS headers" in {
      withoutBackend { (client, baseUri) =>
        cors(Request(uri = metadataFs(baseUri)), client, Set(GET, POST))
      }
    }
  }
  
  "/data/fs" should {   
    def dataFs(base: Uri) = base / "data" / "fs"

    "GET" should {
      "be NotFound for missing backend" in {
        withoutBackend { (client, baseUri) =>
          notFoundUri(dataFs(baseUri) / "missing", client)(ErrorMessage("./missing: doesn't exist")).run
        }
      }
      
      "be NotFound for missing file" in {
        withBackends { (client, baseUri) =>
          notFoundUri(dataFs(baseUri) / "empty" / "anything", client)(ErrorMessage("./anything: no backend")).run
        }
      }

      "read entire file readably by default" in {
        withBackends { (client, baseUri) =>
          parseMultilineJson(dataFs(baseUri) / "foo" / "bar", client).run ==== ((List(
            Json("a" := 1),
            Json("b" := 2),
            Json("c" := List(3))
          ).right,
            Some(readableContentType)
          ))
        }
      }
      
      "read empty file" in {
        withBackends { (client, baseUri) =>
          parseMultilineJson(dataFs(baseUri) / "foo" / "empty", client).run ==== 
            ((Nil.right, Some(readableContentType)))
        }
      }

      "read entire file precisely when specified via headers" in {
        withBackends { (client, baseUri) =>
          parseMultilineJson2(Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(Header("Accept", "application/ldjson;mode=precise"))
          ), client).run ==== ((
            List(Json("a" := 1), Json("b" := 2), Json("c" := Json("$set" := List(3)))).right,
            Some(preciseContentType)
          ))
        }
      }
      
      "read entire file precisely when specified via uri query parameter request-headers" in {
        withBackends { (client, baseUri) =>
          parseMultilineJson2(Request(
            uri = (dataFs(baseUri) / "foo" / "bar").withQueryParam(
              "request-headers", """{"Accept": "application/ldjson; mode=precise" }"""
            )
          ), client).run ==== ((
            List(Json("a" := 1), Json("b" := 2), Json("c" := Json("$set" := List(3)))).right,
            Some(preciseContentType)
          ))
        }
      }

      "read entire file precisely with complex Accept" in {
        withBackends { (client, baseUri) =>
          parseMultilineJson2(Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(Header("Accept", "application/ldjson;q=0.9;mode=readable,application/json;boundary=NL;mode=precise"))
          ), client).run ==== ((
            List(Json("a" := 1), Json("b" := 2), Json("c" := Json("$set" := List(3)))).right,
            Some(preciseContentType)
          ))
        }
      }
      
      "read entire file in JSON array when specified" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = dataFs(baseUri) / "foo" / "bar", 
            headers = Headers(Header("Accept", "application/json"))
          )).flatMap(response => response.as[String]).run ====
            """[
               |{ "a": 1 },
               |{ "b": 2 },
               |{ "c": [ 3 ] }
               |]
               |""".stripMargin.replace("\n", "\r\n")
        }
      }
      
      "read entire file with gzip encoding" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = dataFs(baseUri) / "foo" / "bar", 
            headers = Headers(Header("Accept-Encoding", "gzip"))
          )).map{response => 
            (response.headers.get(`Content-Encoding`), response.status)
          }.run ==== ((Some(`Content-Encoding`(ContentCoding.gzip)), Status.Ok))
        }
      }

      "read entire file (with space)" in {
        withBackends { (client, baseUri) =>
          parseMultilineJson(dataFs(baseUri) / "foo" / "a file", client).run ==== 
            ((List(Json("1" := "ok")).right,Some(readableContentType)))
        }
      }
    
      "read entire file as CSV" in {
        withBackends { (client, baseUri) =>
          expectCsv(dataFs(baseUri) / "foo" / "bar", client)(List("a,b,c[0]", "1,,", ",2,", ",,3"))
        }
      }

      "read entire file as CSV with quoting" in {
        withBackends { (client, baseUri) =>
          expectCsv(dataFs(baseUri) / "foo" / "quoting", client)(List("a,b", "\"\"\"Hey\"\"\",\"a, b, c\""))
        }
      }

      "read entire file as CSV with alternative delimiters" in {
        val mt = List(
          csvContentType,
          "columnDelimiter=\"\t\"",
          "rowDelimiter=\";\"",
          "quoteChar=\"'\"",  // NB: probably doesn't need quoting, but http4s renders it that way
          "escapeChar=\"\\\\\"").mkString("; ")

        withBackends { (client, baseUri) =>
          expectCsv2(Request(
            uri = (dataFs(baseUri) / "foo" / "bar"),
            headers = Headers(Header("Accept", mt))
          ), client)(List("a\tb\tc[0];1\t\t;\t2\t;\t\t3;"), "Content-Type: " + mt + charsetParam)
        }
      }
      
      "read entire file as CSV with standard delimiters specified" in {
        val mt = List(
          csvContentType,
          "columnDelimiter=\",\"",
          "rowDelimiter=\"\\\\r\\\\n\"",
          "quoteChar=\"\"",
          "escapeChar=\"\\\"\"").mkString("; ")

        withBackends { (client, baseUri) =>
          expectCsv2(Request(
            uri = (dataFs(baseUri) / "foo" / "bar"),
            headers = Headers(Header("Accept", mt))
          ), client)(List("a,b,c[0]", "1,,", ",2,", ",,3"), csvResponseContentType)
        }
      }
      
      "read with disposition" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = (dataFs(baseUri) / "foo" / "bar"),
            headers = Headers(Header("Accept", "application/ldjson; disposition=\"attachment; filename=data.json\""))
          )).map(_.headers.get(`Content-Disposition`).map(_.value.toString)).run ====
            Some("attachment; filename=\"data.json\"")
        }
      }

      "read partial file with offset and limit" in {
        withBackends { (client, baseUri) =>
          parseMultilineJson(
            (dataFs(baseUri) / "foo" / "bar").
              withQueryParam("offset", "1").
              withQueryParam("limit", "1"), 
            client
          ).run ==== ((List(Json("b" := 2)).right,Some(readableContentType)))
        }
      }

      "download zipped directory" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = (dataFs(baseUri) / "foo" / ""),
            headers = Headers(Header("Accept", "text/csv; disposition=\"attachment; filename=foo.zip\""))
          )).map(response => (
            response.status,
            response.headers.get(`Content-Type`).map(_.value),
            response.headers.get(`Content-Disposition`).map(_.value.toString)
          )).run ====
            ((Status.Ok, Some("application/zip"), Some("attachment; filename=\"foo.zip\"")))
        }
      }

      "be BadRequest with negative offset" in {
        withBackends { (client, baseUri) =>
          val offset = -10
          badRequestUri(
            (dataFs(baseUri) / "foo" / "bar").
              withQueryParam("offset", offset).
              withQueryParam("limit", "10"),
            client
          )(ErrorMessage(s"invalid offset: $offset (must be >= 0)")).run
        }
      }

      "be BadRequest with negative limit" in {
        withBackends { (client, baseUri) =>
          val limit = "-10"
          badRequestUri(
            (dataFs(baseUri) / "foo" / "bar").
              withQueryParam("offset", 10).
              withQueryParam("limit", limit),
            client
          )(ErrorMessage(s"invalid limit: $limit (must be >= 1)")).run
        }
      }

      "be BadRequest with unparsable limit" in {
        withBackends { (client, baseUri) =>
          val limit = "a"
          badRequestUri(
            (dataFs(baseUri) / "foo" / "bar").
              withQueryParam("offset", 10).
              withQueryParam("limit", limit),
            client
          )(ErrorMessage(s"???")).run
        }
      }.pendingUntilFixed("#773")

      "read very large data (generated with Process.range)" in {
        tag("slow")
        withBackends { (client, baseUri) =>
          countLines(dataFs(baseUri) / "large" / "range", client) ==== 100*1000
        }
      }

      "read very large data (generated with Process.range, gzipped)" in {
        tag("slow")
        withBackends { (client, baseUri) =>
          size(dataFs(baseUri) / "large" / "range", client) must beBetween(200*1000, 250*1000)
        }
      }

      "read very large data (generated with Process.resource)" in {
        tag("slow")
        withBackends { (client, baseUri) =>
          countLines(dataFs(baseUri) / "large" / "resource", client) ==== 100*1000
        }
      }

      "read very large data (generated with Process.resource, gzipped)" in {
        tag("slow")
        withBackends { (client, baseUri) =>
          size(dataFs(baseUri) / "large" / "resource", client) must beBetween(200*1000, 250*1000)
        }
      }
    }
    
    "PUT" should {
      "be NotFound for missing backend" in {
        withoutBackend { (client, baseUri) =>
          val body ="""|{"a": 1}
                       |{"b": 2}""".stripMargin
          Request(
            uri = dataFs(baseUri) / "missing",
            method = PUT
          ).withBody(body).flatMap(request => 
            notFound(request, client)(ErrorMessage("./missing: doesn't exist"))
          ).run
        }
      }

      "be BadRequest with no body" in {
        withBackends { (client, baseUri) =>
          badRequest(Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            method = PUT
          ), client)(ErrorMessage("some uploaded value(s) could not be processed")).run
        }
      }

      "be BadRequest with invalid JSON" in {
        withBackends { (client, baseUri) =>
          Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            method = PUT
          ).withBody("{").flatMap(request => 
            badRequest(request, client)(ErrorMessage("some uploaded value(s) could not be processed"))
          ).run
        }
      }

      "accept valid (Precise) JSON" in {
        mockTest { (client, baseUri, mock) =>
          asR[String](
            body = """|{"a": 1}
                      |{"b": "12:34:56"}""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            response ==== "" and
            status ==== Ok and
            mock.actions ==== List(
              Mock.Action.Save(
                Path("./bar"),
                List(
                  Data.Obj(ListMap("a" -> Data.Int(1))),
                  Data.Obj(ListMap("b" -> Data.Str("12:34:56"))))))
          }.run
        }
      }

      "accept valid (Readable) JSON" in {
        mockTest { (client, baseUri, mock) =>
          asR[String](
            body = """|{"a": 1}
                      |{"b": "12:34:56"}""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(Header("Content-Type", readableContentType)),
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            response ==== "" and
            status ==== Ok and
            mock.actions ==== List(
              Mock.Action.Save(
                Path("./bar"),
                List(
                  Data.Obj(ListMap("a" -> Data.Int(1))),
                  Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))))
          }.run                
        }
      }


      "accept valid (standard) CSV" in {
        mockTest { (client, baseUri, mock) =>
          asR[String](
            body = """|a,b
                      |1,
                      |,12:34:56""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(`Content-Type`(`text/csv`)),
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            response ==== "" and
            status ==== Ok and
            mock.actions ==== List(
              Mock.Action.Save(
                Path("./bar"),
                List(
                  Data.Obj(ListMap("a" -> Data.Int(1))),
                  Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))))
          }.run
        }
      }

      "accept valid (weird) CSV" in {
        mockTest { (client, baseUri, mock) =>
          asR[String](
            body = """|a|b
                      |1|
                      ||'[1|2|3]'
                      |""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(`Content-Type`(`text/csv`)),
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            response ==== "" and
            status ==== Ok and
            mock.actions ==== List(
              Mock.Action.Save(
                Path("./bar"),
                List(
                  Data.Obj(ListMap("a" -> Data.Int(1))),
                  Data.Obj(ListMap("b" -> Data.Str("[1|2|3]"))))))
          }.run
        }
      }

      "be BadRequest with empty CSV (no headers)" in {
        mockTest { (client, baseUri, mock) =>
           asR[ErrorMessage](
            body = "",
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(`Content-Type`(`text/csv`)),
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("some uploaded value(s) could not be processed") and
            status ==== BadRequest and
            mock.actions ==== List()
          }.run
        }
      }

      "be BadRequest with broken CSV (after the tenth data line)" in {
        mockTest { (client, baseUri, mock) =>
           asR[ErrorMessage](
            body = "\"a\",\"b\"\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n\",\n", // NB: missing quote char _after_ the tenth data row
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(`Content-Type`(`text/csv`)),
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("some uploaded value(s) could not be processed") and
            status ==== BadRequest and
            mock.actions ==== List()
          }.run
        }
      }

      "be BadRequest with simulated path error" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
           asR[ErrorMessage](
            body = """{"a": 1}""",
            dataFs(baseUri) / "foo" / "pathError",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("simulated (client) error") and
            status ==== BadRequest and
            configs.toList ==== List()
          }.run
        }
      }
      
      "be InternalServerError with simulated error on a particular value" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{"a": 1}""",
            at = dataFs(baseUri) / "foo" / "valueError",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("simulated (value) error; value: Str()") and
            status ==== InternalServerError and
            configs.toList ==== List()
          }.run
        }
      }
    }
    
    "POST" should {
      "be NotFound for missing backend" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """|{"a": 1}
                      |{"b": 2}""".stripMargin,
            at = dataFs(baseUri) / "missing",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("./missing: doesn't exist") and
            status ==== NotFound and
            configs.toList ==== List()
          }.run
        }
      }

      "be BadRequest with no body" in {
        withBackends { (client, baseUri) =>
          badRequest(Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            method = POST
          ), client)(ErrorMessage("some uploaded value(s) could not be processed")).run
        }
      }

      "be BadRequest with invalid JSON" in {
        withBackends { (client, baseUri) =>
          asR[ErrorMessage](
            body = "{",
            at = dataFs(baseUri) / "foo" / "bar",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("some uploaded value(s) could not be processed") and
            status ==== BadRequest
          }.run
        }
      }

      
      "produce two errors with partially invalid JSON" in {
        withBackends { (client, baseUri) =>
          asR[ErrorMessage2](
            body = """|{"a": 1}
                      |"unmatched
                      |{"b": 2}
                      |}
                      |{"c": 3}""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage2(
              error = "some uploaded value(s) could not be processed",
              details = List(
                ErrorDetail("JSON terminates unexpectedly.; value: Str(parse error: \"unmatched)"),
                ErrorDetail("Unexpected content found: }; value: Str(parse error: })")
              )
            ) and
            status ==== BadRequest
          }.run
        }
      }

      "accept valid (Precise) JSON" in {
        mockTest { (client, baseUri, mock) =>
          asR[String](
            body = """|{"a": 1}
                      |{"b": "12:34:56"}""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== "" and
            status ==== Ok and
            mock.actions ==== List(
              Mock.Action.Append(
                Path("./bar"),
                List(
                  Data.Obj(ListMap("a" -> Data.Int(1))),
                  Data.Obj(ListMap("b" -> Data.Str("12:34:56"))))))
          }.run
        }
      }

      "accept valid (Readable) JSON" in {
        mockTest { (client, baseUri, mock) =>
          asR[String](
            body = """|{"a": 1}
                      |{"b": "12:34:56"}""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(Header("Content-Type", readableContentType)),
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== "" and
            status ==== Ok
            mock.actions ==== List(
              Mock.Action.Append(
                Path("./bar"),
                List(
                  Data.Obj(ListMap("a" -> Data.Int(1))),
                  Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))))
          }.run
        }
      }

      "accept valid (standard) CSV" in {
        mockTest { (client, baseUri, mock) =>
          asR[String](
            body = """|a,b
                      |1,
                      |,12:34:56""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(`Content-Type`(`text/csv`)),
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== "" and
            status ==== Ok
            mock.actions ==== List(
              Mock.Action.Append(
                Path("./bar"),
                List(
                  Data.Obj(ListMap("a" -> Data.Int(1))),
                  Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))))
          }.run
        }
      }

      "accept valid (weird) CSV" in {
        mockTest { (client, baseUri, mock) =>
          asR[String](
            body = """|a|b
                      |1|
                      ||'[1|2|3]'""".stripMargin,
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(`Content-Type`(`text/csv`)),
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== "" and
            status ==== Ok
            mock.actions ==== List(
              Mock.Action.Append(
                Path("./bar"),
                List(
                  Data.Obj(ListMap("a" -> Data.Int(1))),
                  Data.Obj(ListMap("b" -> Data.Str("[1|2|3]"))))))
          }.run
        }
      }
      
      "be BadRequest with empty CSV (no headers)" in {
        mockTest { (client, baseUri, mock) =>
          asR[ErrorMessage](
            body = "",
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(`Content-Type`(`text/csv`)),
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("some uploaded value(s) could not be processed") and
            status ==== BadRequest and
            mock.actions ==== List()
          }.run
        }
      }

      "be BadRequest with broken CSV (after the tenth data line)" in {
        mockTest { (client, baseUri, mock) =>
          asR[ErrorMessage](
            body = "\"a\",\"b\"\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n\",\n", // NB: missing quote char _after_ the tenth data row
            at = dataFs(baseUri) / "foo" / "bar",
            headers = Headers(`Content-Type`(`text/csv`)),
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("some uploaded value(s) could not be processed") and
            status ==== BadRequest and
            mock.actions ==== List()
          }.run
        }
      }

      "be BadRequest with simulated path error" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
           asR[ErrorMessage](
            body = """{"a": 1}""",
            at = dataFs(baseUri) / "foo" / "pathError",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("simulated (client) error") and
            status ==== BadRequest and
            configs.toList ==== List()
          }.run
        }
      }
      
      "be InternalServerError with simulated error on a particular value" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{"a": 1}""",
            at = dataFs(baseUri) / "foo" / "valueError",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            response ==== ErrorMessage("some uploaded value(s) could not be processed") and
            status ==== InternalServerError and
            configs.toList ==== List()
          }.run
        }
      }
    }

    "MOVE" should {
      "be BadRequest for missing src backend" in {
        withoutBackend { (client, baseUri) =>
          badRequest(Request(
            uri = dataFs(baseUri) / "foo",
            method = MOVE
          ), client)(ErrorMessage("The 'Destination' header must be specified")).run
        }
      }

      "be NotFound for missing source file" in {
        withBackends { (client, baseUri) =>
          notFound(Request(
            uri = dataFs(baseUri) / "missing" / "a",
            method = MOVE,
            headers = Headers(Header("Destination", "/foo/bar"))
          ), client)(ErrorMessage("./missing/a: doesn't exist")).run
        }
      }
      
      "be NotFound for missing dst backend" in {
        withBackends { (client, baseUri) =>
          notFound(Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            method = MOVE,
            headers = Headers(Header("Destination", "/missing/a"))
          ), client)(ErrorMessage("./missing/a: doesn't exist")).run
        }
      }

      "be Created for file" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            method = MOVE,
            headers = Headers(Header("Destination", "/foo/baz"))
          )).map(_.status ==== Created).run
        }
      }

      "be Created for dir" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = dataFs(baseUri) / "foo" / "dir" / "",
            method = MOVE,
            headers = Headers(Header("Destination", "/foo/dir2/"))
          )).map(_.status ==== Created).run
        }
      }

      "be NotImplemented for src and dst not in same backend" in {
        withBackends { (client, baseUri) =>
          requestAs[ErrorMessage](Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            method = MOVE,
            headers = Headers(Header("Destination", "/empty/a"))
          ), client)(Status.NotImplemented, ErrorMessage("src and dst path not in the same backend")).run
        }
      }
    }

    "DELETE" should {
      "be NotFound for missing backend" in {
        withoutBackend { (client, baseUri) =>
          notFound(Request(
            uri = dataFs(baseUri) / "missing",
            method = DELETE
          ), client)(ErrorMessage("./missing: doesn't exist")).run
        }
      }

      "be Ok with existing file" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = dataFs(baseUri) / "foo" / "bar",
            method = DELETE
          )).map(_.status ==== Ok).run
        }
      }

      "be Ok with existing dir" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = dataFs(baseUri) / "foo" / "dir" / "",
            method = DELETE
          )).map(_.status ==== Ok).run
        }
      }

      "be Ok with missing file (idempotency)" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = dataFs(baseUri) / "foo" / "missing",
            method = DELETE
          )).map(_.status ==== Ok).run
        }
      }

      "be Ok with missing dir (idempotency)" in {
        withBackends { (client, baseUri) =>
          client(Request(
            uri = dataFs(baseUri) / "foo" / "missingDir" / "",
            method = DELETE
          )).map(_.status ==== Ok).run
        }
      }
    }
  }
 
  "/query/fs" should {
    def queryFs(base: Uri) = base / "query" / "fs" / ""
    
    "GET" should {
      "be NotFound for missing backend" in {
        withoutBackend { (client, baseUri) =>
          notFound(Request(
            uri = (queryFs(baseUri) / "missing").withQueryParam("q", "select * from bar")
          ), client)(ErrorMessage("???")).run
        }
      }.pendingUntilFixed("#771")

      "be BadRequest for missing query" in {
        withBackends { (client, baseUri) =>
          badRequest(Request(
            uri = queryFs(baseUri) / "foo" / ""
          ), client)(ErrorMessage("The request must contain a query")).run
        }
      }

      "execute simple query" in {
        withBackends { (client, baseUri) =>
          parseMultilineJson2(Request(
            uri = (queryFs(baseUri) / "foo" / "").withQueryParam("q", "select * from bar")
          ), client).run ==== ((List(Json("0" := "ok")).right, Some(readableContentType)))
        }
      }
      "be BadRequest for query error" in {
        withBackends { (client, baseUri) =>
          badRequest(Request(
            uri = (queryFs(baseUri) / "foo" / "").withQueryParam("q", "select date where")
          ), client)(ErrorMessage("keyword 'case' expected; `where'")).run
        }
      }
    }

    "POST" should {
      "be NotFound with missing backend" in {
        withoutBackend { (client, baseUri) =>
          asR[String](
            body = "select * from bar",
            at = queryFs(baseUri) / "missing" / "bar",
            method = POST,
            headers = Headers(Header("Destination", "/tmp/gen0")),
            client = client
          ).map{ case (status, response) =>
            status ==== NotFound and
            response ==== "???"
          }.run
        }
      }.pendingUntilFixed("#771")

      "be BadRequest with missing query" in {
        withBackends { (client, baseUri) =>
          badRequest(Request(
            uri = queryFs(baseUri) / "foo" / "",
            method = POST,
            headers = Headers(Header("Destination", "/foo/tmp/gen0"))
          ), client)(ErrorMessage("The body of the POST must contain a query")).run
        }
      }

      "be BadRequest with missing Destination header" in {
        withBackends { (client, baseUri) =>
          asR[ErrorMessage](
            body = "select * from bar",
            at = queryFs(baseUri) / "foo" / "",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("The 'Destination' header must be specified")
          }.run
        }
      }

      "execute simple query" in {
        withBackends { (client, baseUri) =>
          asR[String](
            body = "select * from bar",
            at = queryFs(baseUri) / "foo" / "",
            method = POST,
            headers = Headers(Header("Destination", "/foo/tmp/gen0")),
            client = client
          ).map{ case (status, response) =>
            (status ==== Ok) and
            ((for {
              json   <- Parse.parse(response).toOption
              out    <- json.field("out")
              outStr <- out.string
            } yield outStr) must beSome("/foo/tmp/gen0"))
          }.run
        }
      }

      "be BadRequest for query error" in {
        withBackends { (client, baseUri) =>
          asR[ErrorMessage](
            body = "select date where",
            at = queryFs(baseUri) / "foo" / "",
            method = POST,
            headers = Headers(Header("Destination", "tmp0")),
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("keyword 'case' expected; `where'")
          }.run
        }
      }
    }
  }

  "/compile/fs" should {
    def compileFs(base: Uri) = base / "compile" / "fs"

    "GET" should {
      "be NotFound with missing backend" in {
        withoutBackend { (client, baseUri) =>
          notFoundUri(compileFs(baseUri).withQueryParam("q", "select * from bar"), client)(ErrorMessage("???")).run
        }
      }.pendingUntilFixed("#771")

      "be BadRequest with missing query" in {
        withBackends { (client, baseUri) =>
          badRequestUri(compileFs(baseUri) / "foo" / "", client)(ErrorMessage("The request must contain a query")).run
        }
      }

      "plan simple query" in {
        withBackends { (client, baseUri) =>
          requestAsUri(
            (compileFs(baseUri) / "foo" / "").withQueryParam("q", "select * from bar"),
            client
          )(Ok,
            """|Stub
               |Plan(logical: Squash(Read(Path("bar"))))""".stripMargin
          ).run
        }
      }

      "be BadRequest for query error" in {
        withBackends { (client, baseUri) =>
          badRequestUri(
            (compileFs(baseUri) / "foo" / "").withQueryParam("q", "select date where"),
            client
          )(ErrorMessage("keyword 'case' expected; `where'")).run
        }
      }
    }

    "POST" should {
      "be NotFound with missing backend" in {
        withoutBackend { (client, baseUri) =>
          asR[ErrorMessage](
            body = "select * from bar",
            at = compileFs(baseUri) / "missing" / "",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("???")
          }.run
        }
      }.pendingUntilFixed("#771")

      "be BadRequest with missing query" in {
        withBackends { (client, baseUri) =>
          badRequest(Request(
            uri = compileFs(baseUri) / "foo" / "",
            method = POST
          ), client)(ErrorMessage("The body of the POST must contain a query")).run
        }
      }

      "plan simple query" in {
        withBackends { (client, baseUri) =>
          asR[String](
            body = "select * from bar",
            at = compileFs(baseUri) / "foo" / "",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            status ==== Ok and
            response ==== """|Stub
                             |Plan(logical: Squash(Read(Path("bar"))))""".stripMargin
          }.run
        }
      }

      "be BadRequest for query error" in {
        withBackends { (client, baseUri) =>
          asR[ErrorMessage](
            body = "select date where",
            at = compileFs(baseUri) / "foo" / "",
            method = POST,
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("keyword 'case' expected; `where'")
          }.run
        }
      }
    }
  }

  "/mount/fs" should {
    def mountFs(base: Uri) = base / "mount" / "fs"
   
    "GET" should {
      "be NotFound with missing mount" in {
        withoutBackend { (client, baseUri) =>
          notFoundUri(mountFs(baseUri) / "missing" / "", client)("There is no mount point at /missing/").run
        }
      }

      "succeed with correct path" in {
        withoutBackend { (client, baseUri) =>
          parseMultilineJson(mountFs(baseUri) / "foo" / "", client).run ==== ((
            List(Json("mongodb" := Json("connectionUri" := "mongodb://localhost/foo"))).right,
            Some(`application/json`.renderString)
          ))
        }
      }

      "be NotFound with missing trailing slash" in {
        withoutBackend { (client, baseUri) =>
          notFoundUri(mountFs(baseUri) / "foo", client)("There is no mount point at /foo").run
        }
      }
    }
    
    "MOVE" should {
      "succeed with valid paths" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          (for {
            response <- client(Request(
              uri = mountFs(baseUri) / "foo" / "",
              method = MOVE,
              headers = Headers(Header("Destination", "/foo2/"))
            ))
            body <- response.as[String]
          } yield {
            body ==== "moved /foo/ to /foo2/" and
            configs.toList ==== List(Config(SDServerConfig(baseUri.port), Map(
              Path("/foo2/") -> MongoDbConfig("mongodb://localhost/foo"),
              Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"))))
          }).run
        }
      }

      "be NotFound with missing source" in {
        withoutBackend { (client, baseUri) =>
          notFound(Request(
            uri = mountFs(baseUri) / "missing" / "",
            method = MOVE,
            headers = Headers(Header("Destination", "/foo/"))
          ), client)(ErrorMessage("There is no mount point at /missing/")).run
        }
      }

      "be BadRequest with missing 'Destination' header" in {
        withoutBackend { (client, baseUri) =>
          badRequest(Request(
            uri = mountFs(baseUri) / "foo" / "",
            method = MOVE
          ), client)(ErrorMessage("The 'Destination' header must be specified")).run
        }
      }

      "be BadRequest with relative path" in {
        withoutBackend { (client, baseUri) =>
          badRequest(Request(
            uri = mountFs(baseUri) / "foo" / "",
            method = MOVE,
            headers = Headers(Header("Destination", "foo2/"))
          ), client)(ErrorMessage("Not an absolute path: ./foo2/")).run
        }
      }

      "be BadRequest with non-directory path for MongoDB mount" in {
        withoutBackend { (client, baseUri) =>
          badRequest(Request(
            uri = mountFs(baseUri) / "foo" / "",
            method = MOVE,
            headers = Headers(Header("Destination", "/foo2"))
          ), client)(ErrorMessage("Not a directory path: /foo2")).run
        }
      }
    }
    
    "POST" should {
      "succeed with valid MongoDB config" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[String](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""",
            at = mountFs(baseUri),
            method = POST,
            headers = Headers(Header("X-File-Name", "local/")),
            client = client
          ).map{ case (status, response) =>
            status ==== Ok and
            response ==== "added /local/" and
            configs.toList ==== List(Config(SDServerConfig(baseUri.port), Map(
              Path("/foo/") -> MongoDbConfig("mongodb://localhost/foo"),
              Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"),
              Path("/local/") -> MongoDbConfig("mongodb://localhost/test"))))
          }.run
        }
      }

      "be Conflict with existing path" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/foo2" } }""",
            at = mountFs(baseUri),
            method = POST,
            headers = Headers(Header("X-File-Name", "foo/")),
            client = client
          ).map{ case (status, response) =>
            status ==== Conflict and
            response ==== ErrorMessage("Can't add a mount point above the existing mount point at /foo/") and
            configs.toList ==== Nil
          }.run
        }
      }

      "be Conflict for conflicting mount above" in {
        mockTest { (client, baseUri, mock) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/root" } }""",
            at = mountFs(baseUri) / "non" / "",
            method = POST,
            headers = Headers(Header("X-File-Name", "root/")),
            client = client
          ).map{ case (status, response) =>
            status ==== Conflict and
            response ==== ErrorMessage("Can't add a mount point above the existing mount point at /non/root/mounting/") and
            mock.actions ==== Nil
          }.run
        }
      }

      "be Conflict for conflicting mount below" in {
        mockTest { (client, baseUri, mock) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/root" } }""",
            at = mountFs(baseUri) / "foo" / "",
            method = POST,
            headers = Headers(Header("X-File-Name", "nope/")),
            client = client
          ).map{ case (status, response) =>
            status ==== Conflict and
            response ==== ErrorMessage("Can't add a mount point below the existing mount point at /foo/") and
            mock.actions ==== Nil
          }.run
        }
      }

      "be BadRequest with missing file-name header" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""",
            at = mountFs(baseUri),
            method = POST,
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("The 'X-File-Name' header must be specified") and
            configs.toList ==== Nil
          }.run
        }
      }

      "be BadRequest with invalid MongoDB path (no trailing slash)" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""",
            at = mountFs(baseUri),
            method = POST,
            headers = Headers(Header("X-File-Name", "local")),
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("Not a directory path: /local") and
            configs.toList ==== Nil
          }.run
        }
      }

      "be BadRequest with invalid JSON" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{ "mongodb":""",
            at = mountFs(baseUri),
            method = POST,
            headers = Headers(Header("X-File-Name", "local/")),
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("input error: JSON terminates unexpectedly.") and
            configs.toList ==== Nil
          }.run
        }
      }

      "be BadRequest with invalid MongoDB URI (extra slash)" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost:8080//test" } }""",
            at = mountFs(baseUri),
            method = POST,
            headers = Headers(Header("X-File-Name", "local/")),
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("invalid connection URI: mongodb://localhost:8080//test") and
            configs.toList ==== Nil
          }.run
        }
      }
    }
    
    "PUT" should {
      "succeed with valid MongoDB config" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[String](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""",
            at = mountFs(baseUri) / "local" / "",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            status ==== Ok and
            response ==== "added /local/"
            configs.toList ==== List(Config(SDServerConfig(baseUri.port), Map(
              Path("/foo/") -> MongoDbConfig("mongodb://localhost/foo"),
              Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"),
              Path("/local/") -> MongoDbConfig("mongodb://localhost/test"))))
          }.run
        }
      }

      "succeed with valid, overwritten MongoDB config" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[String](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/foo2" } }""",
            at = mountFs(baseUri) / "foo" / "",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            status ==== Ok and
            response ==== "updated /foo/"
            configs.toList ==== List(Config(SDServerConfig(baseUri.port), Map(
              Path("/foo/") -> MongoDbConfig("mongodb://localhost/foo2"),
              Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"))))
          }.run
        }
      }

      "be Conflict for conflicting mount above" in {
        mockTest { (client, baseUri, mock) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/root" } }""",
            at = mountFs(baseUri) / "non" / "root" / "",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            status ==== Conflict and
            response ==== ErrorMessage("Can't add a mount point above the existing mount point at /non/root/mounting/") and
            mock.actions ==== Nil
          }.run
        }
      }

      "be Conflict for conflicting mount below" in {
        mockTest { (client, baseUri, mock) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/root" } }""",
            at = mountFs(baseUri) / "foo" / "nope" / "",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            status ==== Conflict and
            response ==== ErrorMessage("Can't add a mount point below the existing mount point at /foo/") and
            mock.actions ==== Nil
          }.run
        }
      }

      "be BadRequest with invalid MongoDB path (no trailing slash)" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""",
            at = mountFs(baseUri) / "local",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("Not a directory path: /local") and
            configs.toList ==== Nil
          }.run
        }
      }

      "be BadRequest with invalid JSON" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{ "mongodb": """,
            at = mountFs(baseUri) / "local" / "",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("input error: JSON terminates unexpectedly.") and
            configs.toList ==== Nil
          }.run
        }
      }

      "be BadRequest with invalid MongoDB URI (extra slash)" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          asR[ErrorMessage](
            body = """{ "mongodb": { "connectionUri": "mongodb://localhost:8080//test" } }""",
            at = mountFs(baseUri) / "local" / "",
            method = PUT,
            client = client
          ).map{ case (status, response) =>
            status ==== BadRequest and
            response ==== ErrorMessage("invalid connection URI: mongodb://localhost:8080//test") and
            configs.toList ==== Nil
          }.run
        }
      }
    }

    "DELETE" should {
      "succeed with correct path" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          (for {
            response <- client(Request(
              uri = mountFs(baseUri) / "foo" / "",
              method = DELETE
            ))
            body <- response.as[String]
          } yield {
            body ==== "deleted /foo/" and
            configs.toList ==== List(Config(SDServerConfig(baseUri.port), Map(
              Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"))))

          }).run
        }
      }

      "succeed with missing path (no action)" in {
        withBackendsRecordConfigChange { (client, baseUri, configs) =>
          (for {
            response <- client(Request(
              uri = mountFs(baseUri) / "missing",
              method = DELETE
            ))
            body <- response.as[String]
          } yield {
            body ==== "" and
            configs.toList ==== Nil
          }).run
        }
      }
    }
  }

  "/server" should {
    "be capable of providing it's name and version" in {
      withoutBackend { (client, baseUri) =>
        requestAsUri(baseUri / "server" / "info", client)(Ok, versionAndNameInfo.toString).run
      }
    }

    def serverInfo(base: Uri) = base / "server" / "info"
    def serverPort(base: Uri) = base / "server" / "port"
    def withPort(uri: Uri, port: Int) = uri.copy(authority = uri.authority.map(_.copy(port = Some(port))))

    "restart on new port when PUT /port succeeds" in {
      val newPort = 8889

      def serverInfo(base: Uri) = base / "server" / "info"
      def serverPort(base: Uri) = base / "server" / "port"
      def withPort(uri: Uri, port: Int) = uri.copy(authority = uri.authority.map(_.copy(port = Some(port))))

      withoutBackendExpectingRestart{ (client, baseUri) =>
        val results =
          for {
            result1 <- client(Request(uri = serverInfo(baseUri))).map(_.status)
            req2 <- Request(uri = serverPort(baseUri), method = PUT).withBody(newPort.toString)
            result2 <- client(req2).map(_.status)
          } yield (result1, result2)
        results.run ==== ((Ok, Ok))
      }{ (client, baseUri) =>
        client(withPort(serverInfo(baseUri), newPort)).map(_.status).run ==== Ok
      }
    }

    "restart on default port when DELETE /port successful" in {
      withoutBackendExpectingRestart{ (client, baseUri) =>
        val results =
          for {
            result1 <- client(Request(uri = serverInfo(baseUri))).map(_.status)
            result2 <- client(Request(uri = serverPort(baseUri), method = DELETE)).map(_.status)
          } yield (result1, result2)

        results.run ==== ((Ok, Ok))
      }{ (client, baseUri) =>
        client(withPort(serverInfo(baseUri), SDServerConfig.DefaultPort)).map(_.status).run ==== Ok
      }
    }
  }

  "/welcome" should {
    def welcome(base: Uri) = base / "welcome"
    "show a welcome message" in {
      withBackends { (client, baseUri) =>
        val (status, content) = contentUri(welcome(baseUri), client).run
        content must contain("quasar-logo-vector.png") and
        status ==== Ok
      }
    }

    "show the current version" in {
      withBackends { (client, baseUri) =>
        val (status, content) = contentUri(welcome(baseUri), client).run
        content must contain("Quasar " + quasar.build.BuildInfo.version) and
        status ==== Ok
      }
    }
  } 

  def cors(request: Request, client: Client, expectedMethods: Set[Method]) = {
    client(request).map(
      _.headers.get(headers.`Access-Control-Allow-Methods`).map(v =>
        AccessControlAllowMethodsParser(v.toString)
      )
    ).run must beLike {
      case Some(scala.util.Success(methods)) if expectedMethods ⊆ methods => ok
      case _ => ko
    }
  }
  def corsHeaders(at: Uri, client: Client, method: Method)(expected: HeaderKey) = {
    import org.http4s.Http4s._
    val request = Request(
      method = OPTIONS,
      uri = at,
      headers = Headers(Header("Access-Control-Request-Method", method.name))
    )
    client(request).map(
      _.headers.get(headers.`Access-Control-Allow-Headers`).
        map(_.value.ci)
    ).run ==== Some(expected.name)
  }

  def expectFs(at: Uri, client: Client)(expected: List[Mount]) =
    for {
      response <- client(Request(uri = at))
      body <- response.as[MountResponse]
    } yield {
      body ==== MountResponse(expected) and
      response.headers.get(`Content-Type`) ==== Some(MediaType.`application/json`)
    }

  def expectCsv2(request: Request, client: Client)(expected: List[String], expectedContentType: String) = {
    val wnl = "\r\n"
    client(request).flatMap(response => 
      response.as[String].map(r =>(
        r.split(wnl).toList,
        response.headers.get(`Content-Type`).map(_.toString)))
    ).run ==== ((expected, Some(expectedContentType)))
  }
  def expectCsv(at: Uri, client: Client)(expected: List[String]) = {
    expectCsv2(Request(
      uri = at,
      headers = Headers(Header("Accept", csvContentType))
    ), client)(expected, csvResponseContentType)
  }

  def requestAsUri[T: EntityDecoder](at: Uri, client: Client)(status: Status, body: T) = requestAs(Request(uri = at), client)(status, body)
  def requestAs[T: EntityDecoder](request: Request, client: Client)(status: Status, body: T) = {
    for {
      response <- client(request)
      content <- response.as[T]
    } yield 
      response.status ==== status and
      content ==== body
  }
  def contentUri(at: Uri, client: Client) = 
    for {
      response <- client(Request(uri = at))
      content <- response.as[String]
    } yield (response.status, content)

  def notFoundUri[T: EntityDecoder](at: Uri, client: Client)(body: T) = requestAsUri(at, client)(NotFound, body)
  def notFound[T: EntityDecoder](request: Request, client: Client)(body: T) = requestAs(request, client)(NotFound, body)
  def badRequestUri[T: EntityDecoder](at: Uri, client: Client)(body: T) = requestAsUri(at, client)(BadRequest, body)
  def badRequest[T: EntityDecoder](request: Request, client: Client)(body: T) = requestAs(request, client)(BadRequest, body)

  def countLines(at: Uri, client: Client) = 
    client(Request(uri = at)).
      flatMap(_.body.pipe(utf8Decode).runFoldMap(_.count(_ == '\n'))).run

  def size(at: Uri, client: Client) = 
    client(Request(uri = at,headers = Headers(Header("Accept-Encoding", "gzip")))).
      map(_.body.runFoldMap(_.size)).run.run

  def asR[T : EntityDecoder](body: String, at: Uri, client: Client, method: Method, headers: Headers = Headers.empty) = {
    for {
      request <- Request(uri = at, method = method).withBody(body).
        map(_.withHeaders(headers)) // the headers get replaced by withBody
      response <- client(request)
      responseBody <- response.as[T]
    } yield (response.status, responseBody)
  }

  def parseMultilineJson(at: Uri, client: Client) = parseMultilineJson2(Request(uri = at), client)
  def parseMultilineJson2(request: Request, client: Client) = {
    client(request).flatMap( response =>
      response.as[String].map{mr =>
        val jsons =
          if(!mr.isEmpty) mr.split(nl).toList.map(Parse.parse).sequenceU
          else Nil.right
        (jsons, response.headers.get(`Content-Type`).map(_.value.toString))
      }
    )
  }
}