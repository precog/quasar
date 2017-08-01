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

package quasar.physical.sparkcore.fs.elastic
import slamdata.Predef._
import pathy._, Path._

class File2ESSpec extends quasar.Qspec {

  "file2ES" should {
    "create IndexType for first lvl file" in {
      val IndexType(index, typ) = file2ES(rootDir </> dir("folder") </> file("file"))
      index must_== "folder"
      typ must_== "file"
    }

    "create IndexType for n-th lvl file" in {
      val IndexType(index, typ) = file2ES(rootDir </> dir("folder") </> dir("subfolder") </> file("file"))
      index must_== s"folder${separator}subfolder"
      typ must_== "file"
    }
  }
}
