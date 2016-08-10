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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.effect.Read
import quasar.fp.free._

import com.marklogic.client._
import com.marklogic.client.io.{InputStreamHandle, StringHandle}
import java.io.ByteArrayInputStream

import argonaut._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.stream.io
import jawn._
import jawnstreamz._

sealed trait WriteError {
  def message: String
}

object WriteError {
  final case class ResourceNotFound(message: String) extends WriteError
  final case class Forbidden(message: String)        extends WriteError
  final case class FailedRequest(message: String)    extends WriteError

  def fromException(ex: scala.Throwable): WriteError = ex match {
    case ex: ResourceNotFoundException => ResourceNotFound(ex.getMessage)
    case ex: ForbiddenUserException        => Forbidden(ex.getMessage)
    case ex: FailedRequestException    => FailedRequest(ex.getMessage)
  }
}

final case class Client(client: DatabaseClient) {

  val docManager = client.newJSONDocumentManager

  def readDocument_[S[_]](uri: String): Task[ResourceNotFoundException \/ Process[Task, Json]] = {
    val bufferSize = 100
    val chunkSizes: Process[Task, Int] = Process.constant(64)
    for {
      inputStream <- Task.delay{
                       val buffer = Array.ofDim[Byte](bufferSize)
                       new ByteArrayInputStream(buffer)
                     }
      handle      = new InputStreamHandle(inputStream)
      stream      = chunkSizes.through(io.chunkR(inputStream)).unwrapJsonArray
      exception   <- Task.delay(\/.fromTryCatchThrowable[InputStreamHandle, ResourceNotFoundException](docManager.read(uri, handle)))
    } yield exception.as(stream)
  }

  def readDocument[S[_]](uri: String)(implicit
    S: Task :<: S
  ): Free[S, ResourceNotFoundException \/ Process[Task, Json]] =
    lift(readDocument_(uri)).into[S]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def exists_(uri: String): Task[Boolean] =
    // I believe that if the `DocumentDescriptor` is null, that means that
    // the resource does not exist, unfortunatly this is not documented
    // properly though.
    Task.delay(docManager.exists(uri) != null)

  def exists[S[_]](uri: String)(implicit
    S: Task :<: S
  ): Free[S, Boolean] =
    lift(exists_(uri)).into[S]

  def write_(uri: String, content: String): Task[Option[WriteError]] =
    Task.delay(docManager.write(uri, new StringHandle(content)))
      .attempt.map(_.swap.toOption.map(WriteError.fromException))

  def write[S[_]](uri: String, content: String)(implicit
    S: Task :<: S
  ): Free[S, Option[WriteError]] =
    lift(write_(uri, content)).into[S]

  //////

  /* Temporary parser until jawn-argonaut supports 6.2.x. */
  @SuppressWarnings(Array(
    "org.wartremover.warts.NonUnitStatements",
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var"))
  private implicit val facade: Facade[Json] = {
    new Facade[Json] {
      def jnull() = Json.jNull
      def jfalse() = Json.jFalse
      def jtrue() = Json.jTrue
      def jnum(s: String) = Json.jNumber(JsonNumber.unsafeDecimal(s))
      def jint(s: String) = Json.jNumber(JsonNumber.unsafeDecimal(s))
      def jstring(s: String) = Json.jString(s)

      def singleContext() = new FContext[Json] {
        var value: Json = null
        def add(s: String) = { value = jstring(s) }
        def add(v: Json) = { value = v }
        def finish: Json = value
        def isObj: Boolean = false
      }

      def arrayContext() = new FContext[Json] {
        val vs = scala.collection.mutable.ListBuffer.empty[Json]
        def add(s: String) = { vs += jstring(s); () }
        def add(v: Json) = { vs += v; () }
        def finish: Json = Json.jArray(vs.toList)
        def isObj: Boolean = false
      }

      def objectContext() = new FContext[Json] {
        var key: String = null
        var vs = JsonObject.empty
        def add(s: String): Unit =
          if (key == null) { key = s } else { vs = vs + (key, jstring(s)); key = null }
        def add(v: Json): Unit =
        { vs = vs + (key, v); key = null }
        def finish = Json.jObject(vs)
        def isObj = true
      }
    }
  }
}

// Is there a better way of doing this without as much duplication?
object Client {
  def readDocument[S[_]](uri: String)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, ResourceNotFoundException \/ Process[Task, Json]] =
    getClient.ask.flatMap(_.readDocument(uri))

  def exists[S[_]](uri: String)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Boolean] =
    getClient.ask.flatMap(_.exists(uri))

  def write[S[_]](uri: String, content: String)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Option[WriteError]] =
    getClient.ask.flatMap(_.write(uri, content))

}
