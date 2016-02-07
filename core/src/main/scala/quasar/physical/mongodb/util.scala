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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{EnvErrF, EnvironmentError2}
import quasar.config._
import quasar.effect.Failure
import quasar.fp.free
import quasar.fp.prism._
import quasar.fs.mount.{ConnectionUri, FileSystemDef}

import java.util.concurrent.{TimeUnit, TimeoutException}

import com.mongodb._
import com.mongodb.async.client.{MongoClient => AMongoClient, MongoClients, MongoClientSettings}
import com.mongodb.connection.{ClusterSettings, SocketSettings, SslSettings}
import scalaz._
import scalaz.concurrent.{Strategy, Task}
import scalaz.syntax.applicative._

object util {
  import ConfigError._, EnvironmentError2._

  def createMongoClient(config: MongoDbConfig): Task[MongoClient] =
    disableMongoLogging *> mongoClient(config.uri)

  /** Returns an async `MongoClient` for the given `ConnectionUri`. Will fail
    * with a `ConfigError` if the uri is invalid and with an `EnvironmentError`
    * if there is a problem connecting to the server.
    *
    * NB: The connection is tested during creation and creation will fail if
    *     connecting to the server times out.
    */
  def createAsyncMongoClient[S[_]: Functor](
    uri: ConnectionUri
  )(implicit
    S0: Task :<: S,
    S1: EnvErrF :<: S,
    S2: CfgErrF :<: S
  ): Free[S, AMongoClient] = {
    type M[A] = Free[S, A]
    val cfgErr = Failure.Ops[ConfigError, S]
    val envErr = Failure.Ops[EnvironmentError2, S]

    val disableLogging =
      free.lift(disableMongoLogging).into[S]

    val connString =
      liftAndHandle(Task.delay(new ConnectionString(uri.value)))(t =>
        cfgErr.fail(malformedConfig(uri.value, t.getMessage)))

    /** Attempts a benign operation (reading the server version) using the
      * given client in order to test whether the connection was successful,
      * necessary as otherwise, given a bad connection URI, the driver will
      * retry indefinitely, on a separate monitor thread, to test the connection
      * while the next operation on the returned `MongoClient` will just block
      * indefinitely (i.e. somewhere in userland).
      *
      * TODO: Is there a better way to achieve this? Or somewhere we can set
      *       a timeout for the driver to consider an operation that takes
      *       too long an error?
      */
    def testConnection(aclient: AMongoClient): Task[Unit] =
      MongoDbIO.serverVersion.run(aclient)
        .timed(defaultTimeoutMillis)(Strategy.DefaultTimeoutScheduler)
        .attempt flatMap {
          case -\/(tout: TimeoutException) =>
            val hosts = aclient.getSettings.getClusterSettings.getHosts.toString
            Task.fail(new TimeoutException(s"Timed out attempting to connect to: $hosts"))
          case -\/(t) =>
            Task.fail(t)
          case \/-(_) =>
            Task.now(())
        }

    def createClient(cs: ConnectionString) =
      liftAndHandle(for {
        client <- Task.delay(MongoClients.create(cs))
        _      <- testConnection(client) onFinish {
                    case Some(_) => Task.delay(client.close())
                    case None    => Task.now(())
                  }
      } yield client)(t => envErr.fail(connectionFailed(t.getMessage)))

    disableLogging *> connString >>= createClient
  }

  ////

  // TODO: Externalize
  private val defaultTimeoutMillis: Int = 5000

  private val DefaultOptions =
    (new MongoClientOptions.Builder)
      .serverSelectionTimeout(defaultTimeoutMillis)
      .build

  private val mongoClient: ConnectionString => Task[MongoClient] = {
    val memo = Memo.mutableHashMapMemo[ConnectionString, MongoClient] { (uri: ConnectionString) =>
      new MongoClient(
        new MongoClientURI(uri.getConnectionString, new MongoClientOptions.Builder(DefaultOptions)))
    }

    uri => Task.delay { memo(uri) }
  }

  private def disableMongoLogging: Task[Unit] = {
    import java.util.logging._
    Task.delay { Logger.getLogger("org.mongodb").setLevel(Level.WARNING) }
  }

  private def liftAndHandle[S[_]: Functor, A]
              (ta: Task[A])(f: Throwable => Free[S, A])
              (implicit S0: Task :<: S)
              : Free[S, A] =
    free.lift(ta.attempt).into[S].flatMap(_.fold(f, _.point[Free[S, ?]]))
}
