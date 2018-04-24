/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.{BackendName, TestConfig}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.catchable._
import quasar.effect._
import quasar.ejson.EJson
import quasar.fp.reflNT
import quasar.fp.free._
import quasar.physical.marklogic._
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript.{Read => _, _}

import com.marklogic.xcc.{ContentSource, Session}
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.specification.core.Fragment
import pathy.Path._
import scala.util.Random
import scalaz._, Scalaz._
import scalaz.concurrent._

final class FilterPlannerSpec extends quasar.ExclusiveQuasarSpecification {
  // Shadow the monadMTMAB instance
  import EitherT.eitherTMonad

  type U      = Fix[Query[Fix[EJson], ?]]
  type SR[A]  = Const[ShiftedRead[ADir], A]
  type QSR[A] = Coproduct[QScriptCore[Fix, ?], SR, A]

  type M[A] = MarkLogicPlanErrT[PrologT[StateT[Free[XccEvalEff, ?], Long, ?], ?], A]
  type G[A] = WriterT[Id, Prologs, A]

  private val recFunc = quasar.qscript.construction.RecFunc[Fix]
  private val json = quasar.ejson.Fixed[Fix[EJson]]

  xccSpec[DocType.Json](bn => s"Filter Planner for ${bn.name}") { (evalPlan, evalXQuery) =>
    "uses a path range index when it exists" >> {
      val field            = randomIndexName
      val idxName          = prettyPrint(rootDir[Sandboxed] </> file(field))
      val (create, delete) = mkPathRangeIndex[G](idxName)
      val planFilter       = evalPlan(filterExpr(field))

      val test = (evalXQuery(create) >> planFilter).onFinish(_ => evalXQuery(delete))

      test.unsafePerformSync must beSome.which(includesPathRange(idxName, _))
    }
  }

  xccSpec[DocType.Xml](bn => s"Filter Planner for ${bn.name}") { (evalPlan, evalXQuery) =>
    "uses a path range index when it exists" >> {
      val field            = randomIndexName
      val idxName          = prettyPrint(rootDir[Sandboxed] </> dir("*") </> file(field))
      val (create, delete) = mkPathRangeIndex[G](idxName)
      val planFilter       = evalPlan(filterExpr(field))

      val test = (evalXQuery(create) >> planFilter).onFinish(_ => evalXQuery(delete))

      test.unsafePerformSync must beSome.which(includesPathRange(idxName, _))
    }
  }

  // prefixed with an underscore to avoid problems with leading numbers causing invalid QNames
  private def randomIndexName: String = s"_${Random.alphanumeric.take(15).mkString}"

  private def includesPathRange(idxName: String, q: Search[U] \/ XQuery): Boolean = {
    val alg: AlgebraM[Option, Query[Fix[EJson], ?], U] = {
      case Query.PathRange(paths, _, _) if paths === IList(idxName) => None
      case other => other.embed.some
    }

    q.fold(src => src.query.cataM(alg).isEmpty, _ => false)
  }

  private def filterExpr(idxName: String): Fix[QSR] = {
    def eq(lhs: RecFreeMap[Fix], rhs: String): RecFreeMap[Fix] =
      recFunc.Eq(lhs, recFunc.Constant(json.str(rhs)))

    def projectField(str: String): RecFreeMap[Fix] =
      recFunc.ProjectKeyS(recFunc.Hole, str)

    def shiftedRead(path: ADir): Fix[QSR] =
      Fix(Inject[SR, QSR].inj(Const(ShiftedRead(path, IncludeId))))

    def filter(src: Fix[QSR], f: RecFreeMap[Fix]): Fix[QSR] =
      Fix(Inject[QScriptCore[Fix, ?], QSR].inj(Filter(src, f)))

    filter(shiftedRead(rootDir[Sandboxed] </> dir("some")), eq(projectField(idxName), "foobar"))
  }

  private def xccSpec[FMT: StructuralPlanner[M, ?]: FilterPlanner[Fix, ?]: SearchOptions](desc: BackendName => String)(
   tests: (Fix[QSR] => Task[Option[Search[U] \/ XQuery]], G[XQuery] => Task[Unit]) => Fragment
  ): Unit =
    TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
      contentSourceConnection[Task](uri).map { cs =>
        val evalPlan: Fix[QSR] => Task[Option[Search[U] \/ XQuery]] = plan[FMT](cs, _)
        val evalXQuery: G[XQuery] => Task[Unit] = runXQuery(cs, _)

        desc(backend.name) >> tests(evalPlan, evalXQuery)
      }.void
    }).unsafePerformSync

  private def mkPathRangeIndex[F[_]: Monad: PrologW](idxName: String): (F[XQuery], F[XQuery]) = {
    val idx = admin.databaseRangePathIndex[F](
      xdmp.database,
      "string".xs,
      idxName.xs,
      XQuery.StringLit("http://marklogic.com/collation/"),
      fn.False,
      "ignore".xs)

    (idx >>= (createPathRangeIndex[F](_)), idx >>= (deletePathRangeIndex[F](_)))
  }

  private def bracketConfig[F[_]: Monad: PrologW](fx: XQuery => F[XQuery]) =
    for {
      config    <- admin.getConfiguration[F]
      newConfig <- fx(config)
      save      <- admin.saveConfiguration[F](newConfig)
    } yield save

  private def createPathRangeIndex[F[_]: Monad: PrologW](pathIdx: XQuery): F[XQuery] =
    bracketConfig(admin.databaseAddRangePathIndex[F](_, xdmp.database, pathIdx))

  private def deletePathRangeIndex[F[_]: Monad: PrologW](pathIdx: XQuery): F[XQuery] =
    bracketConfig(admin.databaseDeleteRangePathIndex[F](_, xdmp.database, pathIdx))

  private def runXQuery(cs: ContentSource, fx: G[XQuery]): Task[Unit] = {
    val (prologs, body) = fx.run
    val mainModule = MainModule(Version.`1.0-ml`, prologs, body)

    testing.moduleResults[ReaderT[Task, ContentSource, ?]](mainModule).run(cs).void
  }

  private def runXcc[A](f: Free[XccEvalEff, A], sess: Session, cs: ContentSource): Task[A] =
    MonotonicSeq.from(0L) >>= { (monoSeq: MonotonicSeq ~> Task) =>
      val xccToTask: XccEvalEff ~> Task =
        reflNT[Task] :+: monoSeq :+: Read.constant[Task, Session](sess) :+: Read.constant[Task, ContentSource](cs)
      val eval: Free[XccEvalEff, ?] ~> Task = foldMapNT(xccToTask)

      eval(f)
    }

  private def plan[FMT: StructuralPlanner[M, ?]: FilterPlanner[Fix, ?]: SearchOptions](
    cs: ContentSource, qs: Fix[QSR]
  ): Task[Option[Search[U] \/ XQuery]] = {
    val plan = qs.cataM(Planner[M, FMT, QSR, Fix[EJson]].plan[U])

    runXcc(plan.run.run.eval(1), cs.newSession, cs).map(_._2.toOption)
  }
}
