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

package quasar.physical.rdbms

import quasar.physical.rdbms.common.{CustomSchema, DefaultSchema, TableName, TablePath}
import quasar.Qspec
import quasar.contrib.pathy.AFile
import quasar.contrib.pathy._
import quasar.physical.rdbms.testutils.RdbmsPathyArbitrary.Generators._

import pathy.Path
import Path.{FileName, dir1, _}
import pathy.scalacheck.PathyArbitrary._

class TablePathSpec extends Qspec {

  import TablePath._

  "TablePath" should {
    "extract table name" in {
      prop { (file: AFile) =>
        TablePath.create(file).table must_=== TableName(Path.fileName(file).value)
      }
    }

    "extract empty schema name" in {
      prop { (fileName: FileName) =>
        {
          create(rootDir </> file1(fileName)).schema must_=== DefaultSchema
        }
      }
    }

    "extract some schema name" in {
      prop { (parentDirName: DirName, fileName: FileName) =>
        {
          val file = rootDir </> dir1(parentDirName) </> file1(fileName)
          create(file).schema must_=== CustomSchema(parentDirName.value)
        }
      }.setGens(alphaNumDirGen, alphaNumFileGen)
    }

    "extract some schema name for 2 levels of depth" in {
      prop { (dirName1: DirName, dirName2: DirName, fileName: FileName) =>
        {
          val file = rootDir </> dir1(dirName1) </> dir1(dirName2) </> file1(fileName)
          create(file).schema must_=== CustomSchema(dirName1.value + Separator + dirName2.value)
        }
      }.setGens(alphaNumDirGen, alphaNumDirGen, alphaNumFileGen)
    }

    "extract some schema name for 3 levels of depth" in {

      prop { (dirName1: DirName, dirName2: DirName, dirName3: DirName, fileName: FileName) =>
        {
          val path = rootDir </> dir1(dirName1) </> dir1(dirName2) </> dir1(dirName3) </> file1(
            fileName)
          create(path).schema must_===
            CustomSchema(
              dirName1.value + Separator + dirName2.value + Separator + dirName3.value)
        }
      }.setGens(alphaNumDirGen, alphaNumDirGen, alphaNumDirGen, alphaNumFileGen)
    }
  }

}
