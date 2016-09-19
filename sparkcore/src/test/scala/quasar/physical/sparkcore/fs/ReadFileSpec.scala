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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._, ReadFile.ReadHandle

import java.io._

import org.apache.spark._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class ReadFileSpec extends quasar.Qspec {
  type Eff[A] = (
        MonotonicSeq
    :\: Task
    :\: KeyValueStore[ReadHandle, SparkCursor, ?]
    :/: Read[SparkContext, ?]
  )#M[A]

  "readfile" should {
    "open - read chunk - close" in skipped("Skipped until the local spark emulator can be avoided as it appears to leak resources, even after a context.stop()")
/*
    "open - read chunk - close" in {
      // given
      import quasar.Data._
      implicit val sc = newSc()

      val content = List(
        """{"login" : "john", "age" : 28}""",
        """{"login" : "kate", "age" : 31}"""
      )
      val path: String = tempFile(content)
      val aFile: AFile = sandboxAbs(posixCodec.parseAbsFile(path).get)
      // when
      readOneChunk(aFile).run.foldMap(inter).unsafePerformSync must beLike {
        case \/-(results) =>
          // then
          results.size must be_==(2)
          results must contain(Obj(ListMap("login" -> Str("john"), "age" -> Int(28))))
          results must contain(Obj(ListMap("login" -> Str("kate"), "age" -> Int(31))))
      }
      sc.stop()
      ok
    }
*/
  }

  private def readOneChunk(f: AFile)
    (implicit unsafe: ReadFile.Unsafe[ReadFile]):
      FileSystemErrT[Free[ReadFile, ?], Vector[Data]] = for {
    handle   <- unsafe.open(f, Natural(0).get, None)
    readData <- unsafe.read(handle)
    _        <- unsafe.close(handle).liftM[FileSystemErrT]
  } yield readData

  private def run(implicit sc: SparkContext): Eff ~> Task = {
    val genState = TaskRef(0L).unsafePerformSync
    val kvsState = TaskRef(Map.empty[ReadHandle, SparkCursor]).unsafePerformSync

    MonotonicSeq.fromTaskRef(genState) :+:
    NaturalTransformation.refl[Task] :+:
    KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](kvsState) :+:
    Read.constant[Task, SparkContext](sc)
  }


  private def inter(implicit sc: SparkContext): ReadFile ~> Task =
    readfile.interpret[Eff](local.readfile.input[Eff]) andThen foldMapNT[Eff, Task](run)

  private def newSc(): SparkContext = {
    val config = new SparkConf().setMaster("local[*]").setAppName(this.getClass().getName())
    new SparkContext(config)
  }

  private def tempFile(content: Seq[String]): String = {
    val file = File.createTempFile(scala.util.Random.nextInt().toString, ".tmp")
    val writer = new PrintWriter(file)
    content.foreach {
      line => writer.write(line + "\n")
    }
    writer.flush()
    writer.close()
    file.getAbsolutePath()
  }
}
