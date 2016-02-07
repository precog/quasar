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

package quasar.fs

import quasar.Predef._
import quasar.{Data, DataArbitrary, LogicalPlan, PhaseResults}
import quasar.fp._
import quasar.scalacheck._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

class QueryFileSpec extends Specification with ScalaCheck with FileSystemFixture {
  import InMemory._, FileSystemError._, PathError2._, DataArbitrary._, query._

  "QueryFile" should {
    "descendantFiles" >> {
      "returns all descendants of the given directory" ! prop {
        (target: ADir, descendants: NonEmptyList[RFile], others: List[AFile]) =>
          val data = Vector(Data.Str("foo"))
          val outsideOfTarget = others.filterNot(_.relativeTo(target).isDefined)
          val insideOfTarget = descendants.list.map(target </> _)

          val state = InMemState fromFiles (insideOfTarget ++ outsideOfTarget).map((_,data)).toMap
          val expected = descendants.list

          Mem.interpret(query.descendantFiles(target)).eval(state).toEither must
            beRight(containTheSameElementsAs(expected))
      }(implicitly,implicitly,implicitly,nonEmptyListSmallerThan(10),implicitly,listSmallerThan(5),implicitly) // Use better syntax once specs2 3.x
        .set(workers = java.lang.Runtime.getRuntime.availableProcessors)

      "returns not found when dir does not exist" ! prop { d: ADir => (d =/= rootDir) ==> {
        Mem.interpret(query.descendantFiles(d)).eval(emptyMem)
          .toEither must beLeft(pathError(PathNotFound(d)))
      }}
    }

    "fileExists" >> {
      val interpret = Mem.interpretT[FileSystemErrT]

      "return true when file exists" ! prop { s: SingleFileMemState =>
        interpret(query.fileExists(s.file)).run.eval(s.state) ==== true.right
      }

      "return false when file doesn't exist" ! prop { (absentFile: AFile, s: SingleFileMemState) =>
        absentFile ≠ s.file ==> {
          interpret(query.fileExists(absentFile)).run.eval(s.state) ==== false.right
        }
      }

      "return false when dir exists with same name as file" ! prop { (f: AFile, data: Vector[Data]) =>
        val n = fileName(f)
        val fd = parentDir(f).get </> dir(n.value) </> file("different.txt")

        interpret(query.fileExists(f)).run.eval(InMemState fromFiles Map(fd -> data)) ==== false.right
      }
    }

    "evaluate" >> {
      "streams the results of evaluating the logical plan" ! prop { s: SingleFileMemState =>
        val query = LogicalPlan.Read(Path.fromAPath(s.file))
        val state = s.state.copy(queryResps = Map(query -> s.contents))
        val result = MemTask.runLog[FileSystemError, PhaseResults, Data](evaluate(query)).run.run.eval(state)
        result.run._2.toEither must beRight(s.contents)
      }
    }
  }
}
