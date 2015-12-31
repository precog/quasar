package quasar
package fs

import quasar.Predef._
import quasar.fp._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import pathy.scalacheck._
import scalaz.scalacheck.ScalazArbitrary._

import scalaz._, Scalaz._

class QueryFileSpec extends Specification with ScalaCheck with FileSystemFixture {
  import InMemory._, FileSystemError._, PathError2._, DataGen._, query._

  "QueryFile" should {
    "descendantFiles" >> {
      "returns all descendants of the given directory" ! prop {
        (dp: AbsDirOf[AlphaCharacters], dc1: RelDirOf[AlphaCharacters], dc2: RelDirOf[AlphaCharacters], od: AbsDirOf[AlphaCharacters], fns: NonEmptyList[String]) =>
          ((dp != od) && depth(dp.path) > 0 && depth(od.path) > 0) ==> {
            val body = Vector(Data.Str("foo"))
            val fs  = fns.list take 5 map file
            val f1s = fs map (f => (dp.path </> dc1.path </> f, body))
            val f2s = fs map (f => (dp.path </> dc2.path </> f, body))
            val fds = fs map (f => (od.path </> f, body))

            val state = InMemState fromFiles (f1s ::: f2s ::: fds).toMap
            val expectedFiles = (fs.map(dc1.path </> _) ::: fs.map(dc2.path </> _)).distinct

            Mem.interpret(query.descendantFiles(dp.path)).eval(state).toEither must
              beRight(containTheSameElementsAs(expectedFiles))
        }
      }

      "returns not found when dir does not exist" ! prop { d: ADir =>
        Mem.interpret(query.descendantFiles(d)).eval(emptyMem).toEither must beLeft(PathError(PathNotFound(d)))
      }
    }

    "fileExists" >> {
      "return true when file exists" ! prop { s: SingleFileMemState =>
        Mem.interpret(query.fileExists(s.file)).eval(s.state) must beTrue
      }

      "return false when file doesn't exist" ! prop { (absentFile: AFile, s: SingleFileMemState) =>
        absentFile ≠ s.file ==> {
          Mem.interpret(query.fileExists(absentFile)).eval(s.state) must beFalse
        }
      }

      "return false when dir exists with same name as file" ! prop { (f: AFile, data: Vector[Data]) =>
        val n = fileName(f)
        val fd = parentDir(f).get </> dir(n.value) </> file("different.txt")

        Mem.interpret(query.fileExists(f)).eval(InMemState fromFiles Map(fd -> data)) must beFalse
      }
    }

    "evaluate" >> {
      "streams the results of evaluating the logical plan" ! prop { s: SingleFileMemState =>
        val query = LogicalPlan.Read(convert(s.file))
        val state = s.state.copy(queryResps = Map(query -> s.contents))
        val result = MemTask.runLog[FileSystemError, PhaseResults, Data](evaluate(query)).run.run.eval(state)
        result.run._2.toEither must beRight(s.contents)
      }
    }
  }
}
