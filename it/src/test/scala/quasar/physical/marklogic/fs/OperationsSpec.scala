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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.{Data, TestConfig}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.writerT._
import quasar.effect._
import quasar.effect.uuid.GenUUID
import quasar.fp._
import quasar.fp.free._
import quasar.fp.ski.κ
import quasar.physical.marklogic._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._

import java.util.UUID

import com.marklogic.xcc._
import org.specs2.matcher.MatchResult
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

final class OperationsSpec extends quasar.Qspec {
  def markLogicOpsShould(f: Op ~> Id): Unit = {
    "Appending to files consisting of a single Map" >> {
      def appendToMapTest[T: SearchOptions: AsContent[?, Data]: StructuralPlanner[Op, ?]](
        loc: AFile, append: Data)(
        check: (ListMap[String, Data], Data) => MatchResult[_]
      ): MatchResult[_] = {
        val initialMap: ListMap[String, Data] =
          ListMap(
            "foo"  -> Data._int(42),
            "bar"  -> Data._str("baz"),
            "quux" -> Data._arr(List(Data._dec(1.234), Data._dec(452.498347))))

        val insertAndAppend =
          ops.insertFile[Op, T, Data](loc, Data._obj(initialMap)) *>
          ops.appendToFile[Op, T, Data](loc, append)              *>
          fileContents[T](loc)

        f(insertAndAppend map (check(initialMap, _)))
      }

      val toAppend: ListMap[String, Data] =
        ListMap(
          "blorp" -> Data.Obj("a" -> Data._int(1), "b" -> Data.Null),
          "fizz"  -> Data.True,
          "buzz"  -> Data.Id("ae453df234"))

      def verifyAppended(initial: ListMap[String, Data], contents: Data): MatchResult[_] = {
          val expValues = (initial ++ toAppend).values.toList
          Data._obj.getOption(contents) map (_.values.toList) must beSome(equal(expValues))
      }

      def appendArrayToMapTest[T: SearchOptions: AsContent[?, Data]: StructuralPlanner[Op, ?]](loc: AFile): MatchResult[_] =
        appendToMapTest(loc, Data._arr(toAppend.values.toList))(verifyAppended)

      def appendMapToMapTest[T: SearchOptions: AsContent[?, Data]: StructuralPlanner[Op, ?]](loc: AFile): MatchResult[_] =
        appendToMapTest(loc, Data._obj(toAppend))(verifyAppended)

      "appends array content to XML element" >>
        appendArrayToMapTest[DocType.Xml](rootDir </> dir("appendtest") </> file("xmlarr"))

      "appends map content to XML element" >>
        appendMapToMapTest[DocType.Xml](rootDir </> dir("appendtest") </> file("xmlmap"))

      "appends array content to JSON object" >>
        appendArrayToMapTest[DocType.Json](rootDir </> dir("appendtest") </> file("jsonarr"))

      "appends map content to JSON object" >>
        appendMapToMapTest[DocType.Json](rootDir </> dir("appendtest") </> file("jsonmap"))
    }; ()
  }

  /** Returns the root document of the file at the given path. */
  def fileContents[T: SearchOptions](path: AFile): Op[Data] =
    Xcc[Op].queryResults(fileNode(path))
      .map(_.headOption flatMap (xdmitem.toData[ErrorMessages \/ ?](_).toOption) getOrElse Data.NA)

  type SRT[F[_], A]  = Kleisli[F, Session, A]
  type SR[A]         = SRT[Task, A]
  type MLPlanFail[A] = Failure[MarkLogicPlannerError, A]

  type OpF[A] = (
        Task
    :\: GenUUID
    :\: MonotonicSeq
    :\: MLPlanFail
    :\: XccSessionR
    :/: XccContentSourceR
  )#M[A]

  type Op[A] = PrologT[Free[OpF, ?], A]

  private implicit val xccSessionR = Read.monadReader_[Session, OpF]
  private implicit val xccSourceR  = Read.monadReader_[ContentSource, OpF]
  private implicit val uuidR       = Read.monadReader_[UUID, OpF]
  private implicit val mlPlanE     = Failure.monadError_[MarkLogicPlannerError, OpF]
  private implicit val listMapShow = Show[Map[String, Data]].contramap[ListMap[String, Data]](x => x)

  // Wraps the program in a transaction and forces a rollback, leaving the db untouched.
  val ephemerally: Op ~> Op = {
    import Session.TransactionMode.UPDATE
    val begin    = SessionReader.withSession[Op, Unit](_.setTransactionMode(UPDATE))
    val rollback = SessionReader.withSession[Op, Unit](_.rollback)
    λ[Op ~> Op](op => begin *> op.ensuring(κ(rollback)))
  }

  def interpOpF(cs: ContentSource): Task[OpF ~> SR] = {
    val lt = liftMT[Task, SRT]
    (GenUUID.type4[Task] |@| MonotonicSeq.fromZero) { (genUUID, monoSeq) =>
      lt                                                :+:
      (lt compose genUUID)                              :+:
      (lt compose monoSeq)                              :+:
      Failure.toRuntimeError[SR, MarkLogicPlannerError] :+:
      Read.toReader[SR, Session]                        :+:
      Read.constant[SR, ContentSource](cs)
    }
  }

  val dropWritten = λ[Op ~> Free[OpF, ?]](_.value)
  val performTask = λ[Task ~> Id](_.unsafePerformSync)

  TestConfig.fileSystemConfigs(FsType).flatMap(_.headOption traverse_ {
    case (_, uri, _) => for {
      cs       <- contentSourceConnection[Task](uri)
      opFToSR  <- interpOpF(cs)
      run      =  performTask              compose
                  provideSession[Task](cs) compose
                  foldMapNT(opFToSR)       compose
                  dropWritten              compose
                  ephemerally
    } yield markLogicOpsShould(run)
  }).unsafePerformSync
}
