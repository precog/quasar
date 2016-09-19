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

package quasar.physical.sparkcore

import quasar.Predef._
import quasar.EnvironmentError
import quasar.fs.mount.FileSystemDef._
import quasar.fs.mount.ConnectionUri
import quasar.contrib.pathy._

import pathy.Path._
import org.apache.spark._
import scalaz._
import Scalaz._

package object fs {

  final case class SparkFSConf(sparkConf: SparkConf, prefix: ADir)

  def parseUri(uri: ConnectionUri): DefinitionError \/ SparkFSConf = {

    def error(msg: String): DefinitionError \/ SparkFSConf =
      NonEmptyList(msg).left[EnvironmentError].left[SparkFSConf]

    def forge(master: String, rootPath: String): DefinitionError \/ SparkFSConf =
      posixCodec.parseAbsDir(rootPath)
        .map { prefix =>
        SparkFSConf(new SparkConf().setMaster(master), sandboxAbs(prefix))
      }.fold(error(s"Could not extrat a path from $rootPath"))(_.right[DefinitionError])

    // TODO use spark://host:port/var/hadoop
    // note: that some master configs can be in different shape than spark://host:port
    uri.value.split('|').toList match {
      case master :: prefixPath :: Nil => forge(master, prefixPath)
      case _ =>
        error("Missing master and prefixPath (seperated by |)" +
          " e.g spark://host:port|/var/hadoop/")
    }
  }
}
