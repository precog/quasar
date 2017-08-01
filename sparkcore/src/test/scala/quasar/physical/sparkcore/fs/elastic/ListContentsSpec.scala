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
import quasar.contrib.pathy._
import quasar.fs.FileSystemError

import pathy._, Path._
import scalaz._, Scalaz._
import org.specs2.scalaz.DisjunctionMatchers

class ListContentsSpec extends quasar.Qspec with DisjunctionMatchers {

  def elasticInterpreter(indices: List[String]): ElasticCall ~> Id =
    elasticInterpreter(Map[String, List[String]](indices.map(idx => (idx -> List.empty[String])): _*))

  def elasticInterpreter(indices: Map[String, List[String]]): ElasticCall ~> Id = new (ElasticCall ~> Id) {
    def apply[A](from: ElasticCall[A]) = from match {
      case CreateIndex(_) => ()
      case IndexInto(_, _) => ()
      case CopyType(_, _) => ()
      case CopyIndex(_, _) => ()
      case TypeExists(indexType) => true
      case ListTypes(index) => indices.get(index).getOrElse(List.empty[String])
      case ListIndices() => indices.keys.toList
      case DeleteIndex(index: String) => ()
      case DeleteType(_) => ()
    }
  }

  def exec(adir: ADir, indices: List[String]): FileSystemError \/ Set[PathSegment] =
    queryfile.listContents[ElasticCall](adir).run.foldMap(elasticInterpreter(indices))

  def exec(adir: ADir, indices: Map[String, List[String]]): FileSystemError \/ Set[PathSegment] =
    queryfile.listContents[ElasticCall](adir).run.foldMap(elasticInterpreter(indices))

  "ListContents" should {
    "for a /" should {
      "list all indexes if there are no multi-level folders" in {
        val indices = List("foo", "bar", "baz")
        val result = exec(rootDir, indices)

        result must be_\/-(indices.map(i => DirName(i).left[FileName]).toSet)
      }

      "list all indexes if there are multi-level folders" in {
        val indices = List(s"foo${separator}bar", "baz", s"muu${separator}nuu${separator}buu")
        val result = exec(rootDir, indices)

        result must be_\/-(Set(DirName("foo"), DirName("baz"), DirName("muu")).map(_.left[FileName]))
      }
    }

    "for any folder directly under /" should {
      "return its subfolders" in {
        val indices = List(
          "folder1",
          s"folder2${separator}folder3",
          s"folder2${separator}folder4${separator}folder5",
          s"folder2${separator}folder4${separator}folder6${separator}folder7",
          s"folder8${separator}folder9${separator}folder10",
          s"folder11${separator}folder12${separator}folder13${separator}folder7"
        )
        val result = exec(rootDir </> dir("folder2"), indices)

        result must be_\/-(Set(DirName("folder3"), DirName("folder4")).map(_.left[FileName]))
      }

      "return empty if no subfolders" in {
        val indices = List(
          "folder1",
          s"folder2${separator}folder3",
          s"folder2${separator}folder4${separator}folder5"
        )
        val result = exec(rootDir </> dir("folder1"), indices)

        result must be_\/-(Set.empty[PathSegment])
      }

      "return list of files under that folder if it has subfolders" in {
        val indices = Map(
          "folder1" -> List.empty[String],
          "folder2" -> List("type1", "type2"),
          s"folder2${separator}folder3" -> List("type3"),
          s"folder2${separator}folder4${separator}folder5" -> List("type4")
        )
        val result = exec(rootDir </> dir("folder2"), indices)

        result must be_\/-(Set(
          FileName("type1").right[DirName],
          FileName("type2").right[DirName],
          DirName("folder3").left[FileName],
          DirName("folder4").left[FileName]
        ))
      }

      "return list of files under that folder if it doet not have subfolders" in {
        val indices = Map(
          "folder1" -> List.empty[String],
          "folder2" -> List("type1", "type2")
        )
        val result = exec(rootDir </> dir("folder2"), indices)

        result must be_\/-(Set(FileName("type1"), FileName("type2")).map(_.right[DirName]))
      }
    }

    "for folder not directly under /" should {
      "return its subfolders" in {
        val indices = List(
          "folder1",
          s"folder2${separator}folder3",
          s"folder2${separator}folder4${separator}folder5",
          s"folder2${separator}folder4${separator}folder6${separator}folder7",
          s"folder8${separator}folder9${separator}folder10",
          s"folder11${separator}folder12${separator}folder13${separator}folder7"
        )
        val result = exec(rootDir </> dir("folder2") </> dir("folder4"), indices)

        result must be_\/-(Set(DirName("folder5"), DirName("folder6")).map(_.left[FileName]))
      }

      "return empty if no subfolders" in {
        val indices = List(
          "folder1",
          s"folder2${separator}folder3",
          s"folder2${separator}folder4${separator}folder5"
        )
        val result = exec(rootDir </> dir("folder2") </> dir("folder3"), indices)

        result must be_\/-(Set.empty[PathSegment])
      }

      "return list of files under that folder if it has subfolders" in {
        val indices = Map(
          "folder1" -> List.empty[String],
          s"folder2${separator}folder3" -> List.empty[String],
          s"folder2${separator}folder4" -> List("type1", "type2"),
          s"folder2${separator}folder4${separator}folder5" -> List.empty[String],
          s"folder2${separator}folder4${separator}folder6${separator}folder7" -> List.empty[String],
          s"folder8${separator}folder9${separator}folder10" -> List.empty[String],
          s"folder11${separator}folder12${separator}folder13${separator}folder7" -> List.empty[String]
        )
        val result = exec(rootDir </> dir("folder2") </> dir("folder4"), indices)

        result must be_\/-(Set(
          FileName("type1").right[DirName],
          FileName("type2").right[DirName],
          DirName("folder5").left[FileName],
          DirName("folder6").left[FileName]
        ))
      }

      "return list of files under that folder if it doet not have subfolders" in {
        val indices = Map(
          "folder1" -> List.empty[String],
          s"folder2${separator}folder3" -> List.empty[String],
          s"folder2${separator}folder4" -> List("type1", "type2"),
          s"folder11${separator}folder12${separator}folder13${separator}folder7" -> List.empty[String]
        )

        val result = exec(rootDir </> dir("folder2") </> dir("folder4"), indices)

        result must be_\/-(Set(
          FileName("type1").right[DirName],
          FileName("type2").right[DirName]
        ))
      }
    }
  }
}
