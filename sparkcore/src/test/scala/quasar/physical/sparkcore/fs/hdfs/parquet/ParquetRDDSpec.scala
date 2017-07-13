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

package quasar.physical.sparkcore.fs.hdfs.parquet

import slamdata.Predef._
import quasar.Qspec
import quasar.Data

import org.apache.spark._
import org.apache.spark.rdd._
import scalaz._, Scalaz._

class ParquetRDDSpec extends Qspec {

  sequential

  val config =
    new SparkConf().setMaster("local[*]").setAppName("ParquetRDDSpec")

  val path = "sparkcore/src/test/resources/example-data.parquet"

  import ParquetRDD._

  "ParquetRDDE" should {
    "should read parquet files by using sc.parquet" in {
      withinSpark(sc => {
        // when
        val rdd: RDD[Data] = sc.parquet(path)
        // then
        rdd.first must_= Data.Obj(ListMap(
          "id" -> Data.Int(1),
          "login" -> Data.Str("login1"),
          "age" -> Data.Int(11)
        ))
      })
    }
  }

  def withinSpark[T](runnable: SparkContext => T): T = {
    val sc = new SparkContext(config)
    val result = runnable(sc)
    sc.stop()
    result
  }

}
 
