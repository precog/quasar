package quasar.api.services.query

import quasar.Predef._
import quasar._, fp._, fs._

import org.http4s._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.Path._, posixCodec._
import scalaz._, Scalaz._

class CompileServiceSpec extends Specification with FileSystemFixture with ScalaCheck {
  import queryFixture._

  "Compile" should {

    "plan simple query" ! prop { filesystem: SingleFileMemState =>
      // Representation of the directory as a string without the leading slash
      val pathString = printPath(filesystem.file).drop(1)
      get[String](compileService)(
        path = filesystem.parent,
        query = Some(Query(selectAll(file(filesystem.filename.value)))),
        state = filesystem.state,
        status = Status.Ok,
        response = κ(ok)
      )
    }

    "plan query with var" ! prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int) =>
      val pathString = printPath(filesystem.file).drop(1)
      val query = selectAllWithVar(file(filesystem.filename.value),varName.value)
      get[String](compileService)(
        path = filesystem.parent,
        query = Some(Query(query,varNameAndValue = Some((varName.value, var_.toString)))),
        state = filesystem.state,
        status = Status.Ok,
        response = κ(ok)
      )
    }

  }

}
