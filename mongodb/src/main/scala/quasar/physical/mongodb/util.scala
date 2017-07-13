/*
 * Copyright 2014–2017 SlamData Inc.
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

import slamdata.Predef._
import quasar.connector.{EnvErr, EnvironmentError}
import quasar.config._
import quasar.effect.Failure
import quasar.fp.free
import quasar.fs.mount.ConnectionUri

import java.util.concurrent.TimeoutException

import com.mongodb._
import com.mongodb.async.client.{MongoClient => AMongoClient, MongoClients, MongoClientSettings}
import scalaz._
import scalaz.concurrent.{Strategy, Task}
import scalaz.syntax.applicative._

object util {
  import ConfigError._, EnvironmentError._

  /** Returns an async `MongoClient` for the given `ConnectionUri`. Will fail
    * with a `ConfigError` if the uri is invalid and with an `EnvironmentError`
    * if there is a problem connecting to the server.
    *
    * NB: The connection is tested during creation and creation will fail if
    *     connecting to the server times out.
    */
  def createAsyncMongoClient[S[_]](
    uri: ConnectionUri
  )(implicit
    S0: Task :<: S,
    S1: EnvErr :<: S,
    S2: CfgErr :<: S
  ): Free[S, AMongoClient] = {
    val cfgErr = Failure.Ops[ConfigError, S]
    val envErr = Failure.Ops[EnvironmentError, S]

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
        .timed(defaultTimeoutMillis.toLong)(Strategy.DefaultTimeoutScheduler)
        .attempt flatMap {
          case -\/(tout: TimeoutException) =>
            // NB: This is a java List of mongo objects – never going to have Show.
            @SuppressWarnings(Array("org.wartremover.warts.ToString"))
            val hosts = aclient.getSettings.getClusterSettings.getHosts.toString
            Task.fail(new TimeoutException(s"Timed out attempting to connect to: $hosts"))
          case -\/(t) =>
            Task.fail(t)
          case \/-(_) =>
            Task.now(())
        }

    val InvalidHostNameAllowedProp = "invalidHostNameAllowed"

    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    def settings(cs: ConnectionString, invalidHostNameAllowed: Boolean): Task[MongoClientSettings] = Task.delay {
      import com.mongodb.connection._

      // NB: this is apparently the only way to get from a ConnectionString to a
      // MongoClient while also inspecting/modifying anything in the settings.
      // This is following `MongoClients.create(ConnectionString)`, and will have
      // to be revisited if a driver release adds additional settings objects.
      val settings = MongoClientSettings.builder

      settings.clusterSettings(ClusterSettings.builder
        .applyConnectionString(cs)
        .build)

      settings.connectionPoolSettings(ConnectionPoolSettings.builder
        .applyConnectionString(cs)
        .build)

      settings.credentialList(cs.getCredentialList)

      settings.serverSettings(ServerSettings.builder
        .build)

      settings.socketSettings(SocketSettings.builder
        .applyConnectionString(cs)
        .build)

      val sslSettings = SslSettings.builder
        .applyConnectionString(cs)
        .invalidHostNameAllowed(invalidHostNameAllowed)
        .build
      settings.sslSettings(sslSettings)

      // NB: Netty _must_ be used if SSL is required, but we do not use it by default
      // mostly because it seems to cause the REPL to fail to exit cleanly. If necessary,
      // it can also be forced using a system property (see MongoDB docs).
      if (sslSettings.isEnabled) {
        settings.streamFactoryFactory(com.mongodb.connection.netty.NettyStreamFactoryFactory.builder.build())
      }

      settings.build
    }

    def createClient(cs: ConnectionString) = {
      import quasar.console.booleanProp

      liftAndHandle(for {
        invalidHostNameAllowed <- booleanProp(InvalidHostNameAllowedProp)
        stngs  <- settings(cs, invalidHostNameAllowed)
        client <- Task.delay(MongoClients.create(stngs))
        _      <- testConnection(client) onFinish {
                    case Some(_) => Task.delay(client.close())
                    case None    => Task.now(())
                  }
      } yield client)(t => envErr.fail(connectionFailed(t)))
    }

    disableLogging *> connString >>= createClient
  }

  ////

  // TODO: Externalize
  private val defaultTimeoutMillis: Int = 10000

  private def disableMongoLogging: Task[Unit] = {
    import java.util.logging._
    Task.delay { Logger.getLogger("org.mongodb").setLevel(Level.WARNING) }
  }

  private def liftAndHandle[S[_], A]
              (ta: Task[A])(f: Throwable => Free[S, A])
              (implicit S0: Task :<: S)
              : Free[S, A] =
    free.lift(ta.attempt).into[S].flatMap(_.fold(f, _.point[Free[S, ?]]))
}
