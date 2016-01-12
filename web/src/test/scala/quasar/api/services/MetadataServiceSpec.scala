package quasar
package api
package services

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import pathy.scalacheck.AbsFileOf
import quasar.Predef._
import quasar.fs._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.http4s.server._
import scalaz._
import scalaz.concurrent.Task
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._

class MetadataServiceSpec extends Specification with ScalaCheck with FileSystemFixture with Http4s {
  import InMemory._

  def runService(mem: InMemState): QueryFile ~> Task =
    new (QueryFile ~> Task) {
      def apply[A](fs: QueryFile[A]) =
        Task.now(queryFile(fs).eval(mem))
    }

  def service(mem: InMemState): HttpService =
    metadata.service[QueryFileF](Coyoneda.liftTF(runService(mem)))

  import posixCodec.printPath

  "Metadata Service" should {
    "respond with NotFound" >> {
      "if directory does not exist" ! prop { dir: AbsDir[Sandboxed] =>
        val path:String = printPath(dir)
        val response = service(InMemState.empty)(Request(uri = Uri(path = path))).run
        response.status must_== Status.NotFound
        response.as[String].run must_== s"${printPath(dir)}: doesn't exist"
      }

      "file does not exist" ! prop { file: AbsFileOf[AlphaCharacters] =>
        val path:String = posixCodec.printPath(file.path)
        val response = service(InMemState.empty)(Request(uri = Uri(path = path))).run
        response.status must_== Status.NotFound
        response.as[Json].run must_== jSingleObject("error", jString(s"File not found: $path"))
      }

      "if file with same name as existing directory (without trailing slash)" ! prop { s: SingleFileMemState =>
        depth(s.file) > 1 ==> {
          val parent = fileParent(s.file)
          // .get here is because we know thanks to the property guard, that the parent directory has a name
          val fileWithSameName = parentDir(parent).get </> file(dirName(parent).get.value)
          val path = printPath(fileWithSameName)
          val response = service(s.state)(Request(uri = Uri(path = path))).run
          response.status must_== Status.NotFound
          response.as[Json].run must_== jSingleObject("error", jString(s"File not found: $path"))
        }
      }
    }

    "respond with empty list for existing empty directory" >>
      todo // The current in-memory filesystem does not support empty directories

    "respond with list of children for existing nonempty directory" ! prop { s: NonEmptyDir =>
      val childNodes = s.ls.map(_.fold(
        f => Node.Plain(currentDir </> file1(f)),
        d => Node.Plain(currentDir </> dir1(d))))

      service(s.state)(Request(uri = Uri(path = printPath(s.dir))))
        .as[Json].run must_== Json("children" := childNodes)
    }

    "respond with Ok for existing file" ! prop { s: SingleFileMemState =>
      service(s.state)(Request(uri = Uri(path = s.path)))
        .as[Json].run must_== Json.obj()
    }
  }
}
