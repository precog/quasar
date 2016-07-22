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
import quasar.specs2._

import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazArbitrary._

class QueryFileSpec
  extends quasar.QuasarSpecification
  with ScalaCheck
  with FileSystemFixture
  with DisjunctionMatchers {

  import InMemory._, FileSystemError._, PathError._, DataArbitrary._
  import query._, transforms.ExecM

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  "QueryFile" should {
    "descendantFiles" >> {
      "returns all descendants of the given directory" ! prop {
        (target: ADir, descendants: NonEmptyList[RFile], others: List[AFile]) =>
          val data = Vector(Data.Str("foo"))
          val outsideOfTarget = others.filterNot(_.relativeTo(target).isDefined)
          val insideOfTarget = descendants.list.map(target </> _)

          val state = InMemState fromFiles (insideOfTarget.toList ++ outsideOfTarget).map((_,data)).toMap
          val expected = descendants.list.toList

          Mem.interpret(query.descendantFiles(target)).eval(state).toEither must
            beRight(containTheSameElementsAs(expected))
      }.setArbitrary2(nonEmptyListSmallerThan(10)).setArbitrary3(listSmallerThan(5))
        .set(workers = java.lang.Runtime.getRuntime.availableProcessors)

      "returns not found when dir does not exist" ! prop { d: ADir => (d =/= rootDir) ==> {
        Mem.interpret(query.descendantFiles(d)).eval(emptyMem)
          .toEither must beLeft(pathErr(pathNotFound(d)))
      }}
    }

    "fileExists" >> {
      "return true when file exists" ! prop { s: SingleFileMemState =>
        Mem.interpret(query.fileExists(s.file)).eval(s.state) ==== true
      }

      "return false when file doesn't exist" ! prop { (absentFile: AFile, s: SingleFileMemState) =>
        absentFile ≠ s.file ==> {
          Mem.interpret(query.fileExists(absentFile)).eval(s.state) ==== false
        }
      }

      "return false when dir exists with same name as file" ! prop { (f: AFile, data: Vector[Data]) =>
        val n = fileName(f)
        val fd = parentDir(f).get </> dir(n.value) </> file("different.txt")

        Mem.interpret(query.fileExists(f)).eval(InMemState fromFiles Map(fd -> data)) ==== false
      }
    }

    "evaluate" >> {
      "streams the results of evaluating the logical plan" ! prop { s: SingleFileMemState =>
        val query = LogicalPlan.Read(s.file)
        val state = s.state.copy(queryResps = Map(query -> s.contents))
        val result = MemTask.runLog[FileSystemError, PhaseResults, Data](evaluate(query)).run.run.eval(state)
        result.unsafePerformSync._2.toEither must beRight(s.contents)
      }
    }

    "enumerate" >> {
      "streams results until an empty vector is received" ! prop {
        s: SingleFileMemState =>

        val query = LogicalPlan.Read(s.file)
        val state = s.state.copy(queryResps = Map(query -> s.contents))
        val result = MemTask.interpret(enumerate(query).drainTo[Vector].run.value)

        result.run(state)
          .unsafePerformSync
          .leftMap(_.resultMap) ==== ((Map.empty, \/.right(s.contents)))
      }

      "closes result handle when terminated early" ! prop {
        s: SingleFileMemState =>

        val n = s.contents.length / 2
        val query = LogicalPlan.Read(s.file)
        val state = s.state.copy(queryResps = Map(query -> s.contents))
        val result = MemTask.interpret(
          enumeratee.take[Data, ExecM](n)
            .run(enumerate(query))
            .drainTo[Vector]
            .run.value
        )

        result.run(state)
          .unsafePerformSync
          .leftMap(_.resultMap) ==== ((Map.empty, \/.right(s.contents take n)))
      }
    }
  }
}
