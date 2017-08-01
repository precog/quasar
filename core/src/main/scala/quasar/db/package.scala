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

package quasar

import slamdata.Predef._

import com.zaxxer.hikari.HikariConfig
import doobie.imports._
import doobie.contrib.hikari.hikaritransactor._
import scalaz._
import scalaz.concurrent.Task

package object db {
  type NotFoundErrT[F[_], A] = EitherT[F, NotFound, A]

  val NotFound: NotFound = new NotFound

  def connFail[A](message: String): ConnectionIO[A] =
    Catchable[ConnectionIO].fail(new RuntimeException(message))

  def poolingTransactor(cxn: ConnectionInfo, config: HikariConfig => Task[Unit]): Task[StatefulTransactor] =
    for {
      xa <- HikariTransactor[Task](cxn.driverClassName, cxn.url, cxn.userName, cxn.password)
      _  <- xa.configure(config)
    } yield StatefulTransactor(xa, xa.shutdown)

  val DefaultConfig: HikariConfig => Task[Unit] = _ => Task.now(())
}
