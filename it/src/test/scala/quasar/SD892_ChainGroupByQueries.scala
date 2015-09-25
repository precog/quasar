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

class SD892_ChainGroupByQueries extends BackendTest with NoTimeConversions with DisjunctionMatchers {
    // TODO: Consider getting ride of backendName in this API
    backendShould(interactive.zips.run) { (prefix, backend, name, files) =>
      implicit val dataShowInstance = Show.showFromToString[Data]
      val zipsPath = files.head
      val initialQueryString = s"""select distinct count(_), state from "$zipsPath" group by state"""
      val outputValueName = "out0"
      def secondaryQueryString(tempPath: Path) = s"""select count(_) as total from "$tempPath""""
      "work at the API level" in {
        interactive.withTemp(backend, prefix) { tempFile =>
          val tempPath = prefix ++ tempFile
          // When running an initial query that stores its result in a temporary collection
          interactive.run(backend, initialQueryString, tempPath).run
          // A secondary query operating over that temporary collection is failing at
          // the time of filling of this bug
          val secondaryQueryResult = interactive.eval(backend, secondaryQueryString(tempPath))
          secondaryQueryResult.runLog.run.run.map(_.toList) should
            beRightDisjunction[Backend.ProcessingError, List[Data]](List(Data.Obj(Map("total" -> Data.Int(51)))))
        }
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
            interactive.withTemp(backend, prefix) { tempFile =>
              val tempPath = prefix ++ tempFile
              val initialQueryPath =
                (client / "query" / "fs" / "")
                  .POST
                  .setBody(initialQueryString)
                  .setHeader("Destination", tempPath.pathname)

              val initialReq = Http(initialQueryPath)

              val initialResp = Await.result(initialReq, 10.seconds)

              initialResp.getStatusCode must_== 200
              (for {
                json <- Parse.parse(initialResp.getResponseBody).toOption
                out <- json.field("out")
                outStr <- out.string
              } yield outStr) must beSome(tempPath.pathname)

              val secondaryQueryPath = client / "query" / "fs" / "" <<? Map("q" -> secondaryQueryString(tempPath))
              val secondaryReq = Http(secondaryQueryPath OK api.Utils.asJson)
              val secondaryResp = Await.result(secondaryReq, 10.seconds)

              secondaryResp must beRightDisjunction((
                api.Utils.readableContentType,
                List(Json("total" := 51))))
            }
          }
        }
        "using relative path" in {
          api.Utils.withServer(backend, mongoServerConfig) { client =>
            interactive.withTemp(backend, prefix) { tempFile =>
              val tempPath = prefix ++ tempFile
              val relativeQueryString = s"""select distinct count(_), state from ${zipsPath.file.get.value} group by state"""
              val initialQueryPath =
                (client / "query" / "fs" / prefix.toString / "")
                  .POST
                  .setBody(relativeQueryString)
                  .setHeader("Destination", tempPath.pathname)

              val initialReq = Http(initialQueryPath)

              val initialResp = Await.result(initialReq, 10.seconds)

              initialResp.getStatusCode must_== 200
              (for {
                json <- Parse.parse(initialResp.getResponseBody).toOption
                out <- json.field("out")
                outStr <- out.string
              } yield outStr) must beSome(tempPath.pathname)

              val relativeSecondaryQueryString = s"select count(_) as total from $outputValueName"
              val secondaryQueryPath = client / "query" / "fs" / prefix.toString / "" <<? Map("q" -> relativeSecondaryQueryString)
              val secondaryReq = Http(secondaryQueryPath OK api.Utils.asJson)
              val secondaryResp = Await.result(secondaryReq, 10.seconds)

              secondaryResp must beRightDisjunction((
                api.Utils.readableContentType,
                List(Json("total" := 51))))
            }
          }
        }
        "using API level for original request and web for secondary request" in {
          interactive.withTemp(backend, prefix) { tempFile =>
            val tempPath = prefix ++ tempFile
            interactive.run(backend, initialQueryString, tempPath).run
            api.Utils.withServer(backend, mongoServerConfig) { client =>
              val secondaryQueryPath = client / "query" / "fs" / "" <<? Map("q" -> secondaryQueryString(tempPath))
              val secondaryReq = Http(secondaryQueryPath OK api.Utils.asJson)
              val secondaryResp = Await.result(secondaryReq, 10.seconds)

              secondaryResp must beRightDisjunction((
                api.Utils.readableContentType,
                List(Json("total" := 51))))
            }
          }
        }
        "using server level for original request and API for secondary request" in {
          interactive.withTemp(backend, prefix) { tempFile =>
            val tempPath = prefix ++ tempFile
            api.Utils.withServer(backend, mongoServerConfig) { client =>
              val initialQueryPath =
                (client / "query" / "fs" / "")
                  .POST
                  .setBody(initialQueryString)
                  .setHeader("Destination", tempPath.pathname)

              val initialReq = Http(initialQueryPath)

              val initialResp = Await.result(initialReq, 10.seconds)

              initialResp.getStatusCode must_== 200
              (for {
                json <- Parse.parse(initialResp.getResponseBody).toOption
                out <- json.field("out")
                outStr <- out.string
              } yield outStr) must beSome(tempPath.pathname)
            }

            val secondaryQueryResult = interactive.eval(backend, secondaryQueryString(tempPath))
            secondaryQueryResult.runLog.run.run.map(_.toList) should
              beRightDisjunction[Backend.ProcessingError, List[Data]](List(Data.Obj(Map("total" -> Data.Int(51)))))
          }
        }
      }
      ()
    }
}
