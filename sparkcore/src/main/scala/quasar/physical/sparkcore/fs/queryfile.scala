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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar.Data
import quasar.Planner._
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT}
import quasar.connector.PlannerErrT
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.fp._, eitherT._
import quasar.fp.free._
import quasar.fs._, FileSystemError._, QueryFile._
import quasar.frontend.logicalplan.LogicalPlan
import quasar.qscript._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import org.apache.spark._
import org.apache.spark.rdd._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfile {

  type SparkQScript[A] = (QScriptCore[Fix, ?] :\: EquiJoin[Fix, ?] :/: Const[ShiftedRead, ?])#M[A]

  implicit val sparkQScriptToQSTotal
      : Injectable.Aux[SparkQScript, QScriptTotal[Fix, ?]] =
    ::\::[QScriptCore[Fix, ?]](::/::[Fix, EquiJoin[Fix, ?], Const[ShiftedRead, ?]])

  final case class Input(
    fromFile: (SparkContext, AFile) => Task[RDD[String]],
    store: (RDD[Data], AFile) => Task[Unit],
    fileExists: AFile => Task[Boolean],
    listContents: ADir => EitherT[Task, FileSystemError, Set[PathSegment]],
    readChunkSize: () => Int
  )

  type SparkContextRead[A] = Read[SparkContext, A]

  def chrooted[S[_]](input: Input, fsType: FileSystemType, prefix: ADir)(implicit
    s0: Task :<: S,
    s1: SparkContextRead :<: S,
    s2: MonotonicSeq :<: S,
    s3: KeyValueStore[ResultHandle, RddState, ?] :<: S
  ): QueryFile ~> Free[S, ?] =
    flatMapSNT(interpreter(input, fsType)) compose chroot.queryFile[QueryFile](prefix)

  def interpreter[S[_]](input: Input, fsType: FileSystemType)(implicit
    s0: Task :<: S,
    s1: SparkContextRead :<: S,
    s2: MonotonicSeq :<: S,
    s3: KeyValueStore[ResultHandle, RddState, ?] :<: S
  ): QueryFile ~> Free[S, ?] = {

    def toQScript(lp: Fix[LogicalPlan]): FileSystemErrT[PhaseResultT[Free[S, ?], ?], Fix[SparkQScript]] = {
      val lc: DiscoverPath.ListContents[FileSystemErrT[PhaseResultT[Free[S, ?],?],?]] =
        (adir: ADir) => EitherT(listContents(input, adir).liftM[PhaseResultT])
      val C = quasar.qscript.Coalesce[Fix, SparkQScript, SparkQScript]
      val rewrite = new Rewrite[Fix]
      for {
        qs <- QueryFile.convertToQScriptRead[Fix, FileSystemErrT[PhaseResultT[Free[S, ?],?],?], QScriptRead[Fix, ?]](lc)(lp)
          .map(simplifyRead[Fix, QScriptRead[Fix, ?], QScriptShiftRead[Fix, ?], SparkQScript].apply(_))
        optQS = qs.transAna(
          repeatedly(C.coalesceQC[SparkQScript](idPrism)) ⋙
            repeatedly(C.coalesceEJ[SparkQScript](idPrism.get)) ⋙
            repeatedly(C.coalesceSR[SparkQScript](idPrism)))
        .transCata(rewrite.optimize(idPrism.reverseGet))
        _  <- EitherT(WriterT[Free[S, ?], PhaseResults, FileSystemError \/ Unit]((Vector(PhaseResult.tree("QScript (Spark)", optQS)), ().right[FileSystemError]).point[Free[S, ?]]))
      } yield qs
    }

    def qsToProgram[T](
      exec: (Fix[SparkQScript]) => Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, T]],
      lp: Fix[LogicalPlan]
    ): Free[S, (PhaseResults, FileSystemError \/ T)] = {
          val qs = toQScript(lp) >>= (qs => EitherT(WriterT(exec(qs).map(_.run.run))))
          qs.run.run
        }

    new (QueryFile ~> Free[S, ?]) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case FileExists(f) => fileExists(input, f)
        case ListContents(dir) => listContents(input, dir)
        case QueryFile.ExecutePlan(lp: Fix[LogicalPlan], out: AFile) =>
          qsToProgram(qs => executePlan(input, qs, out, lp), lp)
        case QueryFile.EvaluatePlan(lp: Fix[LogicalPlan]) =>
          qsToProgram(qs => evaluatePlan(input, qs, lp), lp)
        case QueryFile.More(h) => more(h, input.readChunkSize())
        case QueryFile.Close(h) => close(h)
        case QueryFile.Explain(lp: Fix[LogicalPlan]) =>
          qsToProgram(qs => explainPlan(input, fsType, qs, lp), lp)
      }
    }}

  implicit def composedFunctor[F[_]: Functor, G[_]: Functor]:
      Functor[(F ∘ G)#λ] =
    new Functor[(F ∘ G)#λ] {
      def map[A, B](fa: F[G[A]])(f: A => B) = fa ∘ (_ ∘ f)
    }

  // TODO unify explainPlan, executePlan & evaluatePlan
  // This might be more complicated then it looks at first glance
  private def explainPlan[S[_]](input: Input, fsType: FileSystemType, qs: Fix[SparkQScript], lp: Fix[LogicalPlan]) (implicit
    s0: Task :<: S,
    read: Read.Ops[SparkContext, S]
  ): Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, ExecutionPlan]] = {

    val total = scala.Predef.implicitly[Planner[SparkQScript]]

    read.asks { sc =>
      val sparkStuff: Task[PlannerError \/ RDD[Data]] =
        qs.cataM(total.plan(input.fromFile)).eval(sc).run

      injectFT.apply {
        sparkStuff.flatMap(mrdd => mrdd.bitraverse[(Task ∘ Writer[PhaseResults, ?])#λ, FileSystemError, ExecutionPlan](
          planningFailed(lp, _).point[Writer[PhaseResults, ?]].point[Task],
          rdd => Task.delay(Writer(Vector(PhaseResult.detail("RDD", rdd.toDebugString)), ExecutionPlan(fsType, "RDD Directed Acyclic Graph"))))).map(EitherT(_))
      }
    }.join
  }

  private def executePlan[S[_]](input: Input, qs: Fix[SparkQScript], out: AFile, lp: Fix[LogicalPlan]) (implicit
    s0: Task :<: S,
    read: Read.Ops[SparkContext, S]
  ): Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, AFile]] = {

    val total = scala.Predef.implicitly[Planner[SparkQScript]]

    read.asks { sc =>
      val sparkStuff: Task[PlannerError \/ RDD[Data]] =
        qs.cataM(total.plan(input.fromFile)).eval(sc).run

      injectFT.apply {
        sparkStuff >>= (mrdd => mrdd.bitraverse[(Task ∘ Writer[PhaseResults, ?])#λ, FileSystemError, AFile](
          planningFailed(lp, _).point[Writer[PhaseResults, ?]].point[Task],
          rdd => input.store(rdd, out).as (Writer(Vector(PhaseResult.detail("RDD", rdd.toDebugString)), out))).map(EitherT(_)))
      }
    }.join
  }

  // TODO for Q4.2016  - unify it with ReadFile
  final case class RddState(maybeRDD: Option[RDD[(Data, Long)]], pointer: Int)

  private def evaluatePlan[S[_]](input: Input, qs: Fix[SparkQScript], lp: Fix[LogicalPlan])(implicit
      s0: Task :<: S,
      kvs: KeyValueStore.Ops[ResultHandle, RddState, S],
      read: Read.Ops[SparkContext, S],
      ms: MonotonicSeq.Ops[S]
  ): Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, ResultHandle]] = {

    val total = scala.Predef.implicitly[Planner[SparkQScript]]

    val open: Free[S, PlannerError \/ (ResultHandle, RDD[Data])] = (for {
      h <- EitherT(ms.next map (ResultHandle(_).right[PlannerError]))
      rdd <- EitherT(read.asks { sc =>
        lift(qs.cataM(total.plan(input.fromFile)).eval(sc).run).into[S]
      }.join)
      _ <- kvs.put(h, RddState(rdd.zipWithIndex.some, 0)).liftM[PlannerErrT]
    } yield (h, rdd)).run

    open
      .map(_.leftMap(planningFailed(lp, _)))
      .map {
      disj => EitherT(disj.traverse {
        case (rh, rdd) => Writer(Vector(PhaseResult.detail("RDD", rdd.toDebugString)), rh)
      })
    }
  }

  private def more[S[_]](h: ResultHandle, step: Int)(implicit
      s0: Task :<: S,
      kvs: KeyValueStore.Ops[ResultHandle, RddState, S]
  ): Free[S, FileSystemError \/ Vector[Data]] = {

    kvs.get(h).toRight(unknownResultHandle(h)).flatMap {
      case RddState(None, _) =>
        Vector.empty[Data].pure[EitherT[Free[S, ?], FileSystemError, ?]]
      case RddState(Some(rdd), p) =>
        for {
          collected <- lift(Task.delay {
            rdd
              .filter(d => d._2 >= p && d._2 < (p + step))
              .map(_._1).collect.toVector
          }).into[S].liftM[FileSystemErrT]
          rddState = if(collected.isEmpty) RddState(None, 0) else RddState(Some(rdd), p + step)
          _ <- kvs.put(h, rddState).liftM[FileSystemErrT]
        } yield(collected)
    }.run
  }

  private def close[S[_]](h: ResultHandle)(implicit
      kvs: KeyValueStore.Ops[ResultHandle, RddState, S]
     ): Free[S, Unit] = kvs.delete(h)

  private def fileExists[S[_]](input: Input, f: AFile)(implicit
    s0: Task :<: S): Free[S, Boolean] =
    injectFT[Task, S].apply (input.fileExists(f))

  private def listContents[S[_]](input: Input, d: ADir)(implicit
    s0: Task :<: S): Free[S, FileSystemError \/ Set[PathSegment]] =
    injectFT[Task, S].apply(input.listContents(d).run)
}
