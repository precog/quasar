package quasar.api.services

import quasar.Predef._
import quasar.api._
import quasar.effect.Failure
import quasar.fp.{free, liftMT}
import quasar.fs._
import quasar.fs.mount._

import org.http4s._, Method.MOVE
import org.http4s.server.HttpService
import org.http4s.dsl._
import org.http4s.util.CaseInsensitiveString
import org.http4s.headers._
import org.specs2.mutable.Specification
import scalaz.{Failure => _, _}
import scalaz.concurrent.Task

class RestApiSpecs extends Specification {
  import InMemory._

  type Eff0[A] = Coproduct[FileSystemFailureF, MountingFileSystem, A]
  type Eff[A]  = Coproduct[Task, Eff0, A]

  def compositeService(serviceMap: Map[String,HttpService]) = HttpService {
    case req =>
      serviceMap.keys.find{req.uri.renderString.startsWith(_)}.map{key =>
        serviceMap(key)(req)
      }.getOrElse(NotFound())
  }

  "OPTIONS" should {
    val restApi = RestApi(8888,_ => Task.now(()))
    val mount = new (Mounting ~> Task) {
      def apply[A](m: Mounting[A]): Task[A] = Task.fail(new RuntimeException("unimplemented"))
    }
    val fs = runFs(InMemState.empty).map(interpretMountingFileSystem(mount, _)).run
    val eff = free.interpret3[Task, FileSystemFailureF, MountingFileSystem, Task](
      NaturalTransformation.refl,
      Coyoneda.liftTF[FileSystemFailure, Task](Failure.toTaskFailure[FileSystemError]),
      fs)
    val serviceMap = restApi.httpServices(liftMT[Task, ResponseT].compose[Eff](eff))
    val service = compositeService(serviceMap)

    def testAdvertise(path: String,
                      additionalHeaders: List[Header],
                      resultHeaderKey: HeaderKey.Extractable,
                      expected: List[String]) = {
      val req = Request(
        uri = Uri(path = path),
        method = OPTIONS,
        headers = Headers(Header("Origin", "") :: additionalHeaders))
      val response = service(req).run
      response.headers.get(resultHeaderKey).get.value.split(", ").toList must contain(allOf(expected: _*))
    }

    def advertisesMethodsCorrectly(path: String, expected: List[Method]) =
      testAdvertise(path, Nil, `Access-Control-Allow-Methods`, expected.map(_.name))

    def advertisesHeadersCorrectly(path: String, method: Method, expected: List[HeaderKey]) =
      testAdvertise(
        path = path,
        additionalHeaders = List(Header("Access-Control-Request-Method", method.name)),
        resultHeaderKey = `Access-Control-Allow-Headers`,
        expected = expected.map(_.name.value))

    "advertise GET and POST for /query path" >> advertisesMethodsCorrectly("/query/fs", List(GET, POST))

    "advertise Destination header for /query path and method POST" >> {
      advertisesHeadersCorrectly("/query/fs", POST, List(Destination))
    }

    "advertise GET, PUT, POST, DELETE, and MOVE for /data path" >> {
      advertisesMethodsCorrectly("/data/fs", List(GET, PUT, POST, DELETE, MOVE))
    }

    "advertise Destination header for /data path and method MOVE" >> {
      advertisesHeadersCorrectly("/data/fs", MOVE, List(Destination))
    }
  }
}
