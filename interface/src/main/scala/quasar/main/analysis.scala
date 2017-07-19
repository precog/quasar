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

package quasar.main

import slamdata.Predef._
import quasar.{Data, queryPlan, resolveImports_, Variables}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.ejson.{EJson, EncodeEJson}
import quasar.ejson.implicits._
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.fs.mount.Mounting
import quasar.frontend.SemanticErrors
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}
import quasar.sql.{ScopedExpr, Sql}
import quasar.sst._
import quasar.std.StdLib

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.stream._
import spire.algebra.{Field, NRoot}
import spire.math.ConvertableTo

object analysis {
  /** Knobs controlling various aspects of SST compression.
    *
    * @param arrayMaxSize    arrays larger than this will be compressed
    * @param mapMaxSize      maps larger than this will be compressed
    * @param stringMaxLength all strings longer than this are compressed to char[]
    * @param unionMaxSize    unions larger than this will be compressed
    */
  final case class CompressionSettings(
    arrayMaxLength:  Positive,
    mapMaxSize:      Positive,
    stringMaxLength: Positive,
    unionMaxSize:    Positive
  )

  object CompressionSettings {
    val DefaultArrayMaxLength:  Positive = 64L
    val DefaultMapMaxSize:      Positive = 64L
    val DefaultStringMaxLength: Positive = 64L
    val DefaultUnionMaxSize:    Positive =  1L

    val Default: CompressionSettings =
      CompressionSettings(
        arrayMaxLength  = DefaultArrayMaxLength,
        mapMaxSize      = DefaultMapMaxSize,
        stringMaxLength = DefaultStringMaxLength,
        unionMaxSize    = DefaultUnionMaxSize)
  }

  /** Reduces the input to an `SST` summarizing its structure. */
  def extractSchema[J: Order, A: ConvertableTo: Field: Order](
    settings: CompressionSettings
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): Process1[Data, SST[J, A]] = {
    val structuralTrans =
      compression.coalesceWithUnknown[J, A]                    >>>
      compression.coalesceKeys[J, A](settings.mapMaxSize)      >>>
      compression.z85EncodedBinary[J, A]                       >>>
      compression.limitStrings[J, A](settings.stringMaxLength) >>>
      compression.limitArrays[J, A](settings.arrayMaxLength)   >>>
      compression.coalescePrimary[J, A]                        >>>
      compression.narrowUnion[J, A](settings.unionMaxSize)

    val compress = (sst: SST[J, A]) => {
      val compSST = sst.transCata[SST[J, A]](structuralTrans)
      (sst ≠ compSST) option compSST
    }

    process1.lift(SST.fromData[J, A](Field[A].one, _: Data))
      .reduce((x, y) => repeatedly(compress)(x |+| y))
  }

  /** The schema of the results of the given query. */
  def querySchema[S[_], J: Order, A: ConvertableTo: Field: Order](
    query: ScopedExpr[Fix[Sql]],
    vars: Variables,
    baseDir: ADir,
    sampleSize: Positive,
    settings: CompressionSettings
  )(implicit
    Q : QueryFile.Ops[S],
    M : Mounting.Ops[S],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): Q.M[SemanticErrors \/ Option[SST[J, A] \/ PopulationSST[J, A]]] = {
    type TS   = TypeStat[A]
    type P[X] = StructuralType[J, X]

    val k: A = ConvertableTo[A].fromLong(sampleSize.value)

    EitherT(sampleOfQuery[S](query, vars, baseDir, sampleSize))
      .map(s =>
        s.pipe(extractSchema[J, A](settings))
          .map(sst => (SST.size(sst) < k) either Population.subst[P, TS](sst) or sst)
          .toVector.headOption)
      .run
  }

  /** A plan representing a random sample of `size` items from the dataset
    * represented by the given plan.
    */
  def sampled[P](plan: P, size: Positive)(
    implicit
    TR: Recursive.Aux[P, LogicalPlan],
    TC: Corecursive.Aux[P, LogicalPlan]
  ): P = {
    val lpr   = new LogicalPlanR[P]
    val dsize = Data._int(size.value)
    lpr.invoke2(StdLib.set.Sample, plan, lpr.constant(dsize))
  }

  /** A random sample of at most `size` elements from the dataset represented
    * by the given plan.
    */
  def sampleOfPlan[S[_]](plan: Fix[LogicalPlan], size: Positive)(
    implicit Q: QueryFile.Ops[S]
  ): Q.M[Process0[Data]] =
    Q.transforms.dropPhases(Q.results(sampled(plan, size)))

  /** A random sample of at most `size` elements from the dataset represented
    * by the given query.
    */
  def sampleOfQuery[S[_]](expr: ScopedExpr[Fix[Sql]], vars: Variables, baseDir: ADir, size: Positive)(
    implicit Q: QueryFile.Ops[S], M: Mounting.Ops[S]
  ): Q.M[SemanticErrors \/ Process0[Data]] =
    resolveImports_[S](expr, baseDir)
      .leftMap(_.wrapNel)
      .flatMapF(query =>
        queryPlan(query, vars, baseDir, 0L, none).run.value
          .traverse(_.fold(
            xs => Process.emitAll(xs).point[Q.M],
            sampleOfPlan[S](_, size))))
      .run

  def schemaToData[T[_[_]]: BirecursiveT, A: EncodeEJson: Equal: Field: NRoot](
    schema: SST[T[EJson], A] \/ PopulationSST[T[EJson], A]
  ): Data =
    schema.fold(_.asEJson[T[EJson]], _.asEJson[T[EJson]]).cata(Data.fromEJson)
}
