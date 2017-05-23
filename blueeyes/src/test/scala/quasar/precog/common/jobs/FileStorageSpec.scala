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

package quasar.precog.common.jobs

import quasar.blueeyes._
import quasar.blueeyes.MimeTypes.{html, plain}

import org.specs2.mutable._

import scalaz._, Scalaz._

class FileStorageSpec extends Specification {
  lazy val TEXT = MimeTypes.text / plain
  lazy val HTML = MimeTypes.text / html

  type M[A] = Need[A]

  implicit val M: Monad[Need] with Comonad[Need] = Need.need
  val fs: FileStorage[M] = new InMemoryFileStorage[Need]

  lazy val data1: FileData[M] = {
    val strings = "Hello" :: "," :: " " :: "world!" :: StreamT.empty[M, String]
    val data = strings map { s => s.getBytes("UTF-8") }
    FileData(Some(TEXT), data)
  }

  lazy val data2: FileData[M] = {
    val strings = "Goodbye" :: "," :: " " :: "cruel world." :: StreamT.empty[M, String]
    val data = strings map { s => s.getBytes("UTF-8") }
    FileData(Some(HTML), data)
  }

  def encode(s: StreamT[M, Array[Byte]]): M[String] = s.foldLeft("") { (acc, bytes) =>
    acc + new String(bytes, "UTF-8")
  }

  "File storage" should {
    "save (and load) arbitrary file" in {
      fs.save("f1", data1).copoint must not(throwA[Exception])
      fs.load("f1").copoint must beLike {
        case Some(FileData(Some(TEXT), data)) =>
          encode(data).copoint must_== "Hello, world!"
      }
    }

    "allow files to be overwritten" in {
      val file: M[Option[FileData[M]]] = for {
        _ <- fs.save("f2", data1)
        _ <- fs.save("f2", data2)
        file <- fs.load("f2")
      } yield file

      file.copoint must beLike {
        case Some(FileData(Some(HTML), data)) =>
          encode(data).copoint must_== "Goodbye, cruel world."
      }
    }

    "return None when loading a non-existent file" in {
      fs.load("I do not exists!").copoint must_== None
    }

    "say a file exists when it's been saved" in {
      val result: M[Boolean] = for {
        _ <- fs.save("f3", data1): M[Unit]
        exists <- fs.exists("f3"): M[Boolean]
      } yield exists
      
      result.copoint must_== true
    }

    "not pretend files exist when they don't" in {
      fs.exists("non-existant file").copoint must_== false
    }

    "allow removal of files" in {
      val result: M[(Boolean, Boolean)]  = for {
        _ <- fs.save("f4", data1)
        e1 <- fs.exists("f4")
        _ <- fs.remove("f4")
        e2 <- fs.exists("f4")
      } yield { e1 -> e2 }

      result.copoint must beLike { case (true, false) => ok }
    }
  }
}
