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

import quasar.Predef._
import quasar.{Data, Qspec}
import quasar.fp._

import java.time._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

class DataReadSupportSpec extends Qspec {

  def readAll(p: Path): Task[Vector[Data]] = {
    for {
      pr <- Task.delay(ParquetReader.builder[Data](new DataReadSupport, p).build())
      r  <- Process.repeatEval(Task.delay(pr.read())).takeWhile(Option(_).isDefined).runLog
    } yield r
  }

  def path(path: String): Path = new Path(getClass.getResource(path).toString)

  "DataReadSupport " should {

    "read primitive types" in {
      val data = readAll(path("/test-data-1.parquet")).unsafePerformSync
      data must_= Vector(
        Data.Obj(
          "score" -> Data.Dec(13.9),
          "age" -> Data.Int(11),
          "id" -> Data.Int(1),
          "active" -> Data.Bool(false),
          "height" -> Data.Dec(101.19999694824219),
          "key" -> Data.Binary(ImmutableArray.fromArray(scala.Array[Byte](1, 1, 1, 1)))
        ),
         Data.Obj(
           "score" -> Data.Dec(14.9),
           "age" -> Data.Int(12),
           "id" -> Data.Int(2),
           "active" -> Data.Bool(true),
           "height" -> Data.Dec(102.19999694824219),
           "key" -> Data.Binary(ImmutableArray.fromArray(scala.Array[Byte](2, 2, 2, 2)))
        )
      )
    }

    "read logical types" in {
      val data = readAll(path("/test-data-2.parquet")).unsafePerformSync
      data must_= Vector(
        Data.Obj(
          "description" -> Data.Str("this is a description"),
          "creation" -> Data.Date(LocalDate.of(2017,2,3)),
          "creationTimestamp" -> Data.Timestamp(Instant.parse("2017-02-03T15:53:44.851Z")),
          "meetingTime" -> Data.Time(LocalTime.of(0,0,0,400000))
        )
      )
    }

    "read lists" in {
      val data = readAll(path("/test-data-4.parquet")).unsafePerformSync
      data must_= Vector(
        Data.Obj(
          "skills" -> Data.Arr(List(
            Data.Str("scala"),
            Data.Str("FP"),
            Data.Str("spillikins")
          ))
        )
      )
    }

    "read lists alternative" in {
      val data = readAll(path("/test-data-3.parquet")).unsafePerformSync
      data must_= Vector(
        Data.Obj(
          "skills" -> Data.Arr(List(
            Data.Obj("element" -> Data.Str("scala")),
            Data.Obj("element" -> Data.Str("FP")),
            Data.Obj("element" -> Data.Str("spillikins"))
          ))
        )
      )
    }
  }
}
