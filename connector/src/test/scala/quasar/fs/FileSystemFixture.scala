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

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.Data
import quasar.DataArbitrary._
import quasar.contrib.pathy._, PathArbitrary._
import quasar.fp._
import quasar.fp.free.Interpreter
import quasar.fs.SandboxedPathy._
import quasar.scalacheck._

import org.scalacheck.{Gen, Arbitrary, Shrink}
import org.scalacheck.Shrink.shrink
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazArbitrary._

/** Useful for debugging by producing easier to read paths but that still tend to trigger corner cases */
case class AlphaAndSpecialCharacters(value: String)

object AlphaAndSpecialCharacters {
  implicit val arb: Arbitrary[AlphaAndSpecialCharacters] =
    Arbitrary(Gen.nonEmptyListOf(Gen.oneOf(Gen.alphaChar, Gen.const('/'), Gen.const('.'))).map(chars => AlphaAndSpecialCharacters(chars.mkString)))
  implicit val show: Show[AlphaAndSpecialCharacters] = Show.shows(_.value)
}

trait FileSystemFixture {
  import FileSystemFixture._, InMemory._

  val query  = QueryFile.Ops[FileSystem]
  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val emptyMem = InMemState.empty

  case class SingleFileMemState(file: AFile, contents: Vector[Data]) {
    def state = InMemState fromFiles Map(file -> contents)
    def parent = fileParent(file)
    def filename = fileName(file)
  }

  case class NonEmptyDir(
                          dir: ADir,
                          filesInDir: NonEmptyList[(RFile, Vector[Data])]
                        ) {
    def state = {
      val fileMapping = filesInDir.map{ case (relFile,data) => (dir </> relFile, data)}
      InMemState fromFiles fileMapping.toList.toMap
    }
    def relFiles: NonEmptyList[RFile] = filesInDir.unzip._1
    def ls = relFiles.map(segAt(0,_)).list.toList.flatten.distinct
      .sortBy((pname: PathSegment) => posixCodec.printPath(pname.fold(dir1, file1)))
  }

  // NB: scale down because `Vector[Data]` is `O(n^2)`
  implicit val arbSingleFileMemState: Arbitrary[SingleFileMemState] = Arbitrary(
    (Arbitrary.arbitrary[AFile] |@|
      scaleSize(Arbitrary.arbitrary[Vector[Data]], scalePow(1/2.0)))(SingleFileMemState.apply))

  implicit val shrinkSingleFileMemSate: Shrink[SingleFileMemState] = Shrink { fs =>
    (shrink(fs.file).map(newFile => fs.copy(file = newFile))) append
    (shrink(fs.contents).map(newContents => fs.copy(contents = newContents)))
  }

  // NB: scale down even more because `NonEmptyList[(..., Vector[Data])]` is `O(n^3)`
  implicit val arbNonEmptyDir: Arbitrary[NonEmptyDir] = Arbitrary(
    (Arbitrary.arbitrary[ADir] |@|
      scaleSize(Arbitrary.arbitrary[NonEmptyList[(RFile, Vector[Data])]], scalePow(1/3.0)))(NonEmptyDir.apply))

  type FreeFS[A] = Free[FileSystem, A]

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import StateT.stateTMonadState

  object Mem extends Interpreter[FileSystem, InMemoryFs](interpretTerm = fileSystem) {
    def interpretEmpty[A](program: Free[FileSystem, A]): A =
      interpret(program).eval(emptyMem)
    def interpretInjectingWriteErrors[A](program: Free[FileSystem, A], errors: List[Vector[FileSystemError]]): InMemoryFs[A] =
      program.foldMap(alterResponses).eval((Nil, errors))
  }

  val alterResponses: FileSystem ~> ReadWriteT[InMemoryFs, ?] = interpretFileSystem[ReadWriteT[InMemoryFs, ?]](
    liftMT[InMemoryFs, ReadWriteT] compose queryFile,
    interceptReads(readFile),
    amendWrites(writeFile),
    liftMT[InMemoryFs, ReadWriteT] compose manageFile)
}

object FileSystemFixture {
  import ReadFile._, WriteFile._

  type Reads      = List[FileSystemError \/ Vector[Data]]
  type Writes     = List[Vector[FileSystemError]]
  type ReadWrites = (Reads, Writes)

  type ReadWriteT[F[_], A] = StateT[F, ReadWrites, A]

  /** Transforms a `ReadFile` interpreter, intercepting responses to `Read`s
    * until the provided state is empty, falling back to the base interperter
    * thereafter. All other operations use the base interpreter.
    *
    * For some base interpreter, f,
    *   interceptReads(f).eval((Nil, Nil)) == f
    */
  def interceptReads[F[_]: Monad](f: ReadFile ~> F): ReadFile ~> ReadWriteT[F, ?] =
    new (ReadFile ~> ReadWriteT[F, ?]) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case Read(_) => for {
          nr <- nextReadL.st.lift[F]
          rs <- restReadsL.st.lift[F]
          _  <- (readsL := rs.orZero).lift[F]
          rd <- nr.cata(_.point[ReadWriteT[F, ?]], f(rf).liftM[ReadWriteT])
        } yield rd

        case _ => f(rf).liftM[ReadWriteT]
      }
    }

  /** Transforms a `WriteFile` interpreter, amending errors to `Write`s
    * until the provided state is empty, falling back to the base interpreter
    * thereafter. All other operations use the base interpreter.
    *
    * For some base interpreter, f,
    *   amendWrites(f).eval((Nil, Nil)) == f
    */
  def amendWrites[F[_]: Monad](f: WriteFile ~> F): WriteFile ~> ReadWriteT[F, ?] =
    new (WriteFile ~> ReadWriteT[F, ?]) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case Write(h, d) => for {
          nw <- nextWriteL.st.lift[F]
          ws <- restWritesL.st.lift[F]
          _  <- (writesL := ws.orZero).lift[F]
          es <- nw.cata(_.point[ReadWriteT[F, ?]], f(Write(h, d)).liftM[ReadWriteT])
        } yield es

        case _ => f(wf).liftM[ReadWriteT]
      }
    }

  ////

  private val readsL: ReadWrites @> Reads =
    Lens.firstLens

  private val nextReadL: ReadWrites @?> (FileSystemError \/ Vector[Data]) =
    PLens.listHeadPLens <=< ~readsL

  private val restReadsL: ReadWrites @?> (List[FileSystemError \/ Vector[Data]]) =
    PLens.listTailPLens <=< ~readsL

  private val writesL: ReadWrites @> Writes =
    Lens.secondLens

  private val nextWriteL: ReadWrites @?> Vector[FileSystemError] =
    PLens.listHeadPLens <=< ~writesL

  private val restWritesL: ReadWrites @?> List[Vector[FileSystemError]] =
    PLens.listTailPLens <=< ~writesL
}
