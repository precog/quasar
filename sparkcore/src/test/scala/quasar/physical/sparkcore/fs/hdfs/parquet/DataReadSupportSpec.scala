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

    "read logical types: UTF8, DATE, TIME_MILIS, TIME_MICROS, TIMESTAMP_MILIS, TIMESTAMP_MICROS" in {
      val data = readAll(path("/test-data-2.parquet")).unsafePerformSync
      data must_= Vector(
        Data.Obj(
          "description" -> Data.Str("this is a description"),
          "creation" -> Data.Date(LocalDate.of(2017,2,17)),
          "creationStamp" -> Data.Timestamp(Instant.parse("2017-02-17T13:42:05.017Z")),
          "meetingTime" -> Data.Time(LocalTime.of(0,0,4,0)),
          "creationStampMicros" -> Data.Timestamp(Instant.parse("2017-02-17T13:42:05.017Z")),
          "meetingTimeMicros" -> Data.Time(LocalTime.of(0,0,4,0))
        )
      )
    }

    "read JSON type" in {
      val data = readAll(path("/test-data-9.parquet")).unsafePerformSync
      data must_= Vector(
        Data.Obj(
          "document" -> Data.Obj(
            "name" -> Data.Str("hello"),
            "a" -> Data.Int(10)
          ),
          "invalid_document" -> Data.NA
        )
      )
    }

    "lists" should {
      "be read" in {
        val data = readAll(path("/test-data-5.parquet")).unsafePerformSync
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

      "be read even if data is not following LIST specification" in {
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
    }

    "map" should {
      "be read" in {
        val data = readAll(path("/test-data-6.parquet")).unsafePerformSync
        data must_= Vector(
          Data.Obj(
            "skills" -> Data.Obj(ListMap(
              "scala" -> Data.Str("good"),
              "FP" -> Data.Str("very good"),
              "spillikins" -> Data.Str("bad")
            ))
          )
        )
      }

      "be read even if data is not following MAP spec - it does not contain 'key_value' entry" in {
        val data = readAll(path("/test-data-7.parquet")).unsafePerformSync
        data must_= Vector(
          Data.Obj(
            "skills" -> Data.Obj(ListMap(
              "scala" -> Data.Str("good"),
              "FP" -> Data.Str("very good"),
              "spillikins" -> Data.Str("bad")
            ))
          )
        )
      }


      "be read even if data is not following MAP spec - has extrac entries beside 'key' & 'value'" in {
        val data = readAll(path("/test-data-10.parquet")).unsafePerformSync
        data must_= Vector(
          Data.Obj(
            "skills" -> Data.Arr(List(
              Data.Obj(
                "key" -> Data.Str("scala"),
                "value" -> Data.Str("good"),
                "desc" -> Data.Str("some description")
              ),
              Data.Obj(
                "key" -> Data.Str("FP"),
                "value" -> Data.Str("very good")
              ),
              Data.Obj(
                "key" -> Data.Str("spillikins"),
                "value" -> Data.Str("bad")
              )
            ))
          )
        )
      }

      "be read even if data is using outdated MAP_KEY_VALUE label" in {
        val data = readAll(path("/test-data-8.parquet")).unsafePerformSync
        data must_= Vector(
          Data.Obj(
            "skills" -> Data.Obj(ListMap(
              "scala" -> Data.Str("good"),
              "FP" -> Data.Str("very good"),
              "spillikins" -> Data.Str("bad")
            ))
          )
        )
      }
    }
  }
}
