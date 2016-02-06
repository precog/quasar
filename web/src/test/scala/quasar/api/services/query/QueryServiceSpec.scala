package quasar.api.services.query

import quasar.Predef._
import quasar._, fs._
import quasar.fs.InMemory._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.server.HttpService
import org.http4s.argonaut._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class QueryServiceSpec extends org.specs2.mutable.Specification with FileSystemFixture with ScalaCheck {
  import queryFixture._

  "Execute and Compile Services" should {
    def testBoth[A](test: (InMemState => HttpService) => Unit) = {
      "Compile" should {
        test(compileService)
      }
      "Execute" should {
        test(executeService)
      }
    }

    testBoth { service =>
      "GET" >> {
        "be 404 for missing directory" ! prop { (dir: ADir, file: AFile) =>
          get(service)(
            path = dir,
            query = Some(Query(selectAll(file))),
            state = InMemState.empty,
            status = Status.NotFound,
            response = (a: String) => a must_== "???"
          )
        }.pendingUntilFixed("SD-773")
        "be 400 for missing query" ! prop { filesystem: SingleFileMemState =>
          get(service)(
            path = filesystem.parent,
            query = None,
            state = filesystem.state,
            status = Status.BadRequest,
            response = (_: Json) must_== Json("error" := "The request must contain a query")
          )
        }
        "be 400 for query error" ! prop { filesystem: SingleFileMemState =>
          get(compileService)(
            path = filesystem.parent,
            query = Some(Query("select date where")),
            state = filesystem.state,
            status = Status.BadRequest,
            response = (_: Json) must_== Json("error" := "keyword 'case' expected; `where'")
          )
        }
      }

      () // TODO: Remove after upgrading to specs2 3.x
    }
  }

}
