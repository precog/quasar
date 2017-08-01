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

package quasar.physical.sparkcore.fs.hdfs

import slamdata.Predef._

import org.apache.spark.SparkConf
import java.net.URI
import pathy._, Path._
import scalaz._, concurrent.Task

class HdfsPackageSpec extends quasar.Qspec {

  "hdfs.generateHdfsFS" should {
    "create a valid HDFS file system if URL is valid" in {
      val url = "hdfs://localhost:9000"
      hdfsUri(url).unsafePerformSync must_== new URI(url)
    }

    "fail if URL is not valid" in {
      val url = "blabalbab"
      hdfsUri(url).attempt.map(_.leftMap(_.getMessage)).unsafePerformSync must_== -\/("Provided URL is not valid HDFS URL")
    }
    
  }

  private def hdfsUri(url: String): Task[URI] =
    generateHdfsFS(SparkFSConf(new SparkConf(), url, rootDir)).map(_.getUri())

}
