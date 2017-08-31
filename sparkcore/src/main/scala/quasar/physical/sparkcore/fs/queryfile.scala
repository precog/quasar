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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar.Data
import quasar.Planner._
import quasar.common._
import quasar.connector.PlannerErrT
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect, effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.ejson.implicits._
import quasar.fp.free._
import quasar.fp.ignore
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fs._, FileSystemError._
import quasar.qscript._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.apache.spark._
import org.apache.spark.rdd._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfile {

  type SparkContextRead[A] = effect.Read[SparkContext, A]

  def chrooted[S[_]](fsType: FileSystemType, prefix: ADir)(implicit
    s0: Task :<: S,
    s1: SparkContextRead :<: S,
    s2: MonotonicSeq :<: S,
    s3: KeyValueStore[QueryFile.ResultHandle, RddState, ?] :<: S,
    s4: SparkConnectorDetails :<: S
  ): QueryFile ~> Free[S, ?] =
    flatMapSNT(interpreter(fsType)) compose chroot.queryFile[QueryFile](prefix)

  def interpreter[S[_]](fsType: FileSystemType)(implicit
    s0: Task :<: S,
    s1: SparkContextRead :<: S,
    s2: MonotonicSeq :<: S,
    s3: KeyValueStore[QueryFile.ResultHandle, RddState, ?] :<: S,
    details: SparkConnectorDetails.Ops[S]
  ): QueryFile ~> Free[S, ?] = {

    def qsToProgram[T](
      exec: (Fix[SparkQScript]) => Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, T]],
      lp: Fix[LogicalPlan]
    ): Free[S, (PhaseResults, FileSystemError \/ T)] = {
          val qs = toQScript[Free[S, ?]](details.listContents(_).run)(lp) >>= (qs => EitherT(WriterT(exec(qs).map(_.run.run))))
          qs.run.run
        }

    new (QueryFile ~> Free[S, ?]) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case QueryFile.FileExists(f) => details.fileExists(f)
        case QueryFile.ListContents(dir) => details.listContents(dir).run
        case QueryFile.ExecutePlan(lp: Fix[LogicalPlan], out: AFile) =>
          qsToProgram(qs => executePlan(qs, out, lp), lp)
        case QueryFile.EvaluatePlan(lp: Fix[LogicalPlan]) =>
          qsToProgram(qs => evaluatePlan(qs, lp), lp)
        case QueryFile.More(h) => more(h)
        case QueryFile.Close(h) => close(h)
        case QueryFile.Explain(lp: Fix[LogicalPlan]) =>
          qsToProgram(qs => explainPlan(fsType, qs, lp), lp)
      }
    }}

  implicit def composedFunctor[F[_]: Functor, G[_]: Functor]:
      Functor[(F ∘ G)#λ] =
    new Functor[(F ∘ G)#λ] {
      def map[A, B](fa: F[G[A]])(f: A => B) = fa ∘ (_ ∘ f)
    }

  def rddFrom[S[_]](implicit
    details: SparkConnectorDetails.Ops[S]
  ) = (afile: AFile) => details.rddFrom(afile)

  def first[S[_]](implicit
    S: Task :<: S
  ) = (rdd: RDD[Data]) => lift(Task.delay {
    rdd.first
  }).into[S]

  // TODO unify explainPlan, executePlan & evaluatePlan
  // This might be more complicated then it looks at first glance
  private def explainPlan[S[_]](fsType: FileSystemType, qs: Fix[SparkQScript], lp: Fix[LogicalPlan]) (implicit
    s0: Task :<: S,
    read: Read.Ops[SparkContext, S],
    details: SparkConnectorDetails.Ops[S]
  ): Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, ExecutionPlan]] = {

    val total = scala.Predef.implicitly[Planner[SparkQScript, S]]

    read.asks { sc =>
      val sparkStuff: Free[S, PlannerError \/ RDD[Data]] =
        qs.cataM(total.plan(rddFrom, first)).eval(sc).run

        sparkStuff.flatMap(mrdd => mrdd.bitraverse[(Free[S, ?] ∘ Writer[PhaseResults, ?])#λ, FileSystemError, ExecutionPlan](
          planningFailed(lp, _).point[Writer[PhaseResults, ?]].point[Free[S, ?]],
          rdd => {
            val rddDebug = rdd.toDebugString
            val inputs   = qs.cata(ExtractPath[SparkQScript, APath].extractPath[DList])
            lift(Task.delay(Writer(
              Vector(PhaseResult.detail("RDD", rddDebug)),
              ExecutionPlan(fsType, rddDebug, ISet fromFoldable inputs)))).into[S]
          })).map(EitherT(_))
    }.join
  }

  private def executePlan[S[_]](qs: Fix[SparkQScript], out: AFile, lp: Fix[LogicalPlan]) (implicit
    s0: Task :<: S,
    read: effect.Read.Ops[SparkContext, S],
    details: SparkConnectorDetails.Ops[S]
  ): Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, AFile]] = {

    val total = scala.Predef.implicitly[Planner[SparkQScript, S]]

    read.asks { sc =>
      val sparkStuff: Free[S, PlannerError \/ RDD[Data]] =
        qs.cataM(total.plan(rddFrom, first)).eval(sc).run

      sparkStuff >>= (mrdd => mrdd.bitraverse[(Free[S, ?] ∘ Writer[PhaseResults, ?])#λ, FileSystemError, AFile](
        planningFailed(lp, _).point[Writer[PhaseResults, ?]].point[Free[S, ?]],
        rdd => details.storeData(rdd, out).as (Writer(Vector(PhaseResult.detail("RDD", rdd.toDebugString)), out))).map(EitherT(_)))

    }.join
  }

  // TODO for Q4.2016  - unify it with ReadFile
  final case class RddState(maybeRDD: Option[RDD[(Data, Long)]], pointer: Int)

  private def evaluatePlan[S[_]](qs: Fix[SparkQScript], lp: Fix[LogicalPlan])(implicit
    s0: Task :<: S,
    kvs: KeyValueStore.Ops[QueryFile.ResultHandle, RddState, S],
    read: Read.Ops[SparkContext, S],
    ms: MonotonicSeq.Ops[S],
    details: SparkConnectorDetails.Ops[S]
  ): Free[S, EitherT[Writer[PhaseResults, ?], FileSystemError, QueryFile.ResultHandle]] = {

    val total = scala.Predef.implicitly[Planner[SparkQScript, S]]

    val open: Free[S, PlannerError \/ (QueryFile.ResultHandle, RDD[Data])] = (for {
      h <- EitherT(ms.next map (QueryFile.ResultHandle(_).right[PlannerError]))
      rdd <- EitherT(read.asks { sc =>
        qs.cataM(total.plan(rddFrom, first)).eval(sc).run
      }.join)
      _ <- kvs.put(h, RddState(rdd.zipWithIndex.persist.some, 0)).liftM[PlannerErrT]
    } yield (h, rdd)).run

    open
      .map(_.leftMap(planningFailed(lp, _)))
      .map {
      disj => EitherT(disj.traverse {
        case (rh, rdd) => Writer(Vector(PhaseResult.detail("RDD", rdd.toDebugString)), rh)
      })
    }
  }

  def more[S[_]](h: QueryFile.ResultHandle)(implicit
    s0: Task :<: S,
    kvs: KeyValueStore.Ops[QueryFile.ResultHandle, RddState, S],
    details: SparkConnectorDetails.Ops[S]
  ): Free[S, FileSystemError \/ Vector[Data]] = for {
    step <- details.readChunkSize
    res  <- (kvs.get(h).toRight(unknownResultHandle(h)).flatMap {
      case RddState(None, _) =>
        Vector.empty[Data].pure[EitherT[Free[S, ?], FileSystemError, ?]]
      case RddState(Some(rdd), p) =>
        for {
          collected <- lift(Task.delay {
            rdd
              .filter(d => d._2 >= p && d._2 < (p + step))
              .map(_._1).collect.toVector
          }).into[S].liftM[FileSystemErrT]
          rddState <- lift(Task.delay {
            if(collected.isEmpty) {
              ignore(rdd.unpersist())
              RddState(None, 0)
            } else RddState(Some(rdd), p + step)
          }).into[S].liftM[FileSystemErrT]
          _ <- kvs.put(h, rddState).liftM[FileSystemErrT]
        } yield collected
    }).run
  } yield res

  def close[S[_]](h: QueryFile.ResultHandle)(implicit
      kvs: KeyValueStore.Ops[QueryFile.ResultHandle, RddState, S]
  ): Free[S, Unit] = kvs.delete(h)
}
