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
import quasar.effect.Failure
import quasar.fp.liftMT
import quasar.fp.free, free._
import quasar.fs._
import quasar.fs.mount._

import org.http4s._, Method.MOVE
import org.http4s.dsl._
import org.http4s.headers._
import scalaz.{Failure => _, _}
import scalaz.concurrent.Task

class RestApiSpecs extends quasar.QuasarSpecification {
  import InMemory._, Mounting.PathTypeMismatch

  type Eff0[A] = Coproduct[FileSystemFailure, MountingFileSystem, A]
  type Eff1[A] = Coproduct[MountingFailure, Eff0, A]
  type Eff2[A] = Coproduct[PathMismatchFailure, Eff1, A]
  type Eff[A]  = Coproduct[Task, Eff2, A]

  "OPTIONS" should {
    val mount = new (Mounting ~> Task) {
      def apply[A](m: Mounting[A]): Task[A] =
        Task.fail(new RuntimeException("unimplemented"))
    }

    val fs =
      runFs(InMemState.empty)
        .map(interpretMountingFileSystem(mount, _))

    val eff = fs map { runFs =>
      NaturalTransformation.refl[Task]               :+:
      Failure.toRuntimeError[Task, PathTypeMismatch] :+:
      Failure.toRuntimeError[Task, MountingError]    :+:
      Failure.toRuntimeError[Task, FileSystemError]  :+:
      runFs
    }

    val service = eff map { runEff =>
      RestApi.finalizeServices(RestApi.toHttpServices(
        liftMT[Task, ResponseT] compose runEff,
        RestApi.coreServices[Eff]))
    }

    def testAdvertise(
      path: String,
      additionalHeaders: List[Header],
      resultHeaderKey: HeaderKey.Extractable,
      expected: List[String]
    ) = {
      val req = Request(
        uri = Uri(path = path),
        method = OPTIONS,
        headers = Headers(Header("Origin", "") :: additionalHeaders))

      service.flatMap(_(req)).map { response =>
        response.headers.get(resultHeaderKey)
          .get.value.split(", ")
          .toList must contain(allOf(expected: _*))
      }.unsafePerformSync
    }

    def advertisesMethodsCorrectly(path: String, expected: List[Method]) =
      testAdvertise(path, Nil, `Access-Control-Allow-Methods`, expected.map(_.name))

    def advertisesHeadersCorrectly(path: String, method: Method, expected: List[HeaderKey]) =
      testAdvertise(
        path = path,
        additionalHeaders = List(Header("Access-Control-Request-Method", method.name)),
        resultHeaderKey = `Access-Control-Allow-Headers`,
        expected = expected.map(_.name.value))

    "advertise GET and POST for /query path" >> {
      advertisesMethodsCorrectly("/query/fs", List(GET, POST))
    }

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
