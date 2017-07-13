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

package quasar.config

import slamdata.Predef._
import quasar.fp._
import quasar.fp.ski._

import argonaut._
import org.scalacheck.Arbitrary
import pathy._, Path._
import scalaz._, concurrent.Task, Scalaz._

abstract class ConfigSpec[Config: Arbitrary: CodecJson: ConfigOps] extends quasar.Qspec {
  import FsPath._, ConfigError._

  sequential

  def configOps = ConfigOps[Config]

  val TestConfig: Config
  val TestConfigStr: String

  def testConfigFile: Task[FsPath.Aux[Rel, File, Sandboxed]] =
    Task.delay(scala.util.Random.nextInt.toString)
      .map(i => Uniform(currentDir </> file(s"test-config-${i}.json")))

  def withTestConfigFile[A](f: FsPath.Aux[Rel, File, Sandboxed] => Task[A]): Task[A] = {
    import java.nio.file._

    def deleteIfExists(fp: FsPath[File, Sandboxed]): Task[Unit] =
      systemCodec
        .map(c => printFsPath(c, fp))
        .flatMap(s => Task.delay(Files.deleteIfExists(Paths.get(s))))
        .void

    testConfigFile >>= (fp => f(fp) onFinish κ(deleteIfExists(fp)))
  }

  "fromString" should {
    "parse valid config" in {
      ConfigOps.fromString(TestConfigStr) must beRightDisjunction(TestConfig)
    }
  }

  "toString" should {
    "render same config" in {
      ConfigOps.asString(TestConfig) must_= TestConfigStr
    }
  }

  "toFile" should {
    "create loadable config" in {
      withTestConfigFile(fp =>
        configOps.toFile(TestConfig, Some(fp)) *>
        configOps.fromFile(fp).run
      ).unsafePerformSync must beRightDisjunction(TestConfig)
    }
  }

  "fromFile" should {
    "result in error when file not found" in {
      val (p, r) =
        withTestConfigFile(fp =>
          configOps.fromFile(fp).run.map((fp, _))
        ).unsafePerformSync
      r must beLeftDisjunction(fileNotFound(p))
    }
  }

  "encoding" should {
    "lawful json codec" >> prop { (cfg: Config) =>
      CodecJson.codecLaw(CodecJson.derived[Config])(cfg)
    }
  }
}
