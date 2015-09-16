package quasar

import argonaut.{Parse, Json}
import argonaut.StringWrap._
import dispatch.Http
import org.specs2.time.NoTimeConversions
import scala.concurrent.Await
import scala.concurrent.duration._
import scalaz._, Scalaz._

import Predef._
import config.{SDServerConfig, Config}
import fs.Path
import sql.{Query, SQLParser}
import Errors._
import specs2.DisjunctionMatchers

class Bug892 extends BackendTest with NoTimeConversions with DisjunctionMatchers {
    // TODO: Consider getting ride of backendName in this API
    backendShould { (dbPath, backend, name) =>
      val databaseName = dbPath.toString
      println(databaseName)
      val initialQueryString = s"""select distinct count(_), state from "/$databaseName/zips" group by state"""
      val outputValueName = "out0"
      val destinationPath = dbPath ++ Path(outputValueName)
      val secondaryQueryString = s"""select count(_) as total from "$destinationPath""""
      "work at the API  level" in {
        // When running an initial query that stores its result in a temporary collection
        interactive.run(backend, initialQueryString, destinationPath).run
        // A secondary query operating over that temporary collection is failing at
        // the time of filling of this bug
        val secondaryQueryResult = interactive.eval(backend, secondaryQueryString)
        secondaryQueryResult.runLog.run.run.length should be_==(1)
      }
      "work at the web server level" in {
        val mongoServerConfig = Config(
          // Will be filled in by withServer
          SDServerConfig(None),
          ListMap(
            Path("/") -> TestConfig.loadConfig(name).run.run.get
          )
        )
        "as described in the bug request" in {
          api.Utils.withServer(backend, mongoServerConfig) { client =>
            val initialQueryPath =
              (client / "query" / "fs" / "")
                .POST
                .setBody(initialQueryString)
                .setHeader("Destination", destinationPath.pathname)

            val initialReq = Http(initialQueryPath)

            val initialResp = Await.result(initialReq, 10.seconds)

            initialResp.getStatusCode must_== 200
            (for {
              json <- Parse.parse(initialResp.getResponseBody).toOption
              out <- json.field("out")
              outStr <- out.string
            } yield outStr) must beSome(destinationPath.pathname)

            val secondaryQueryPath = client / "query" / "fs" / "" <<? Map("q" -> secondaryQueryString)
            val secondaryReq = Http(secondaryQueryPath OK api.Utils.asJson)
            val secondaryResp = Await.result(secondaryReq, 10.seconds)

            secondaryResp must beRightDisjunction((
              api.Utils.readableContentType,
              List(Json("total" := 51))))
          }
        }
        "slight variation" in {
          api.Utils.withServer(backend, mongoServerConfig) { client =>
            val relativeQueryString = """select distinct count(_), state from zips group by state"""
            val initialQueryPath =
              (client / "query" / "fs" / databaseName / "")
                .POST
                .setBody(relativeQueryString)
                .setHeader("Destination", destinationPath.pathname)

            val initialReq = Http(initialQueryPath)

            val initialResp = Await.result(initialReq, 10.seconds)

            initialResp.getStatusCode must_== 200
            (for {
              json <- Parse.parse(initialResp.getResponseBody).toOption
              out <- json.field("out")
              outStr <- out.string
            } yield outStr) must beSome(destinationPath.pathname)

            val relativeSecondaryQueryString = s"select count(_) as total from $outputValueName"
            val secondaryQueryPath = client / "query" / "fs" / databaseName / "" <<? Map("q" -> relativeSecondaryQueryString)
            val secondaryReq = Http(secondaryQueryPath OK api.Utils.asJson)
            val secondaryResp = Await.result(secondaryReq, 10.seconds)

            secondaryResp must beRightDisjunction((
              api.Utils.readableContentType,
              List(Json("total" := 51))))
          }
        }
      }
      ()
    }
}
