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

package quasar.fs

import slamdata.Predef._
import quasar.{Data, DataArbitrary}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.stream._
import quasar.fp._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}
import quasar.scalacheck._

import scala.Predef.$conforms

import matryoshka.data.Fix
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazArbitrary._

class QueryFileSpec extends quasar.Qspec with FileSystemFixture {
  import InMemory._, FileSystemError._, PathError._, DataArbitrary._
  import query._

  val lpf = new LogicalPlanR[Fix[LogicalPlan]]

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  "QueryFile" should {
    "descendantFiles" >> {
      "returns all descendants of the given directory" >> prop {
        (target: ADir, descendants: NonEmptyList[RFile], others: List[AFile]) =>
          val data = Vector(Data.Str("foo"))
          val outsideOfTarget = others.filterNot(_.relativeTo(target).isDefined)
          val insideOfTarget = descendants.list.map(target </> _)

          val state = InMemState fromFiles (insideOfTarget.toList ++ outsideOfTarget).map((_,data)).toMap
          val expected = descendants.list.toList.distinct

          Mem.interpret(query.descendantFiles(target).run).eval(state).toEither must
            beRight(containTheSameElementsAs(expected))
      }.setArbitrary2(nonEmptyListSmallerThan(10)).setArbitrary3(listSmallerThan(5))
        .set(workers = java.lang.Runtime.getRuntime.availableProcessors)

      "returns not found when dir does not exist" >> prop { d: ADir => (d =/= rootDir) ==> {
        Mem.interpretEmpty(query.descendantFiles(d).run)
          .toEither must beLeft(pathErr(pathNotFound(d)))
      }}
    }

    "fileExists" >> {
      "return true when file exists" >> prop { s: SingleFileMemState =>
        Mem.interpret(query.fileExists(s.file)).eval(s.state) must_=== true
      }

      "return false when file doesn't exist" >> prop { (absentFile: AFile, s: SingleFileMemState) =>
        absentFile ≠ s.file ==> {
          Mem.interpret(query.fileExists(absentFile)).eval(s.state) must_=== false
        }
      }

      "return false when dir exists with same name as file" >> prop { (f: AFile, data: Vector[Data]) =>
        val n = fileName(f)
        val fd = parentDir(f).get </> dir(n.value) </> file("different.txt")

        Mem.interpret(query.fileExists(f)).eval(InMemState fromFiles Map(fd -> data)) must_=== false
      }
    }

    "evaluate" >> {
      "streams the results of evaluating the logical plan" >> prop { s: SingleFileMemState =>
        val query = lpf.read(s.file)
        val state = s.state.copy(queryResps = Map(query -> s.contents))
        val result = Mem.interpret(evaluate(query).runLogCatch.run.value).eval(state)
        result must_=== s.contents.right.right
      }
    }

    "results" >> {
      "returns the results of the query" >> prop {
        s: SingleFileMemState =>

        val query = lpf.read(s.file)
        val state = s.state.copy(queryResps = Map(query -> s.contents))
        val result = Mem.interpret(results(query).map(_.toVector).run.value)

        result.run(state)
          .leftMap(_.resultMap) must_=== ((Map.empty, s.contents.right))
      }
    }
  }
}
