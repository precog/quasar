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

package quasar
package repl2

import slamdata.Predef._
import quasar.api._
import quasar.contrib.cats.effect._
import quasar.contrib.pathy._
import quasar.csv.CsvWriter
import quasar.fp.minspace
import quasar.fp.ski._
import quasar.main.Prettify
import quasar.repl._
import quasar.run.{QuasarError, SqlQuery}

import java.lang.Exception

import argonaut.{Json, JsonParser, JsonScalaz}, JsonScalaz._
import cats.effect._
import eu.timepit.refined.refineV
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.scalaz._
import fs2.Stream
import fs2.async.Ref
import pathy.Path._
import scalaz._, Scalaz._

final class Evaluator[F[_]: Monad: Effect, G[_]: Functor: Effect](
  stateRef: Ref[F, ReplState],
  sources: DataSources[F, Json],
  queryEvaluator: QueryEvaluator[F, Stream[G, ?], SqlQuery, Stream[G, Data]]) {

  import Command._
  import DataSourceError._
  import Evaluator._

  val F = Effect[F]
  val G = Effect[G]

  def evaluate(cmd: Command): F[Result] = {
    val exitCode = if (cmd === Exit) Some(()) else None
    recoverSomeErrors(doEvaluate(cmd))
      .map(Result(exitCode, _))
  }

  ////

  private def children(path: ResourcePath)
      : F[Stream[G, (ResourceName, ResourcePathType)]] =
    queryEvaluator.children(path) >>=
      fromEither[ResourceError.CommonError, Stream[G, (ResourceName, ResourcePathType)]]

  private def doEvaluate(cmd: Command): F[Option[String]] =
    cmd match {
      case Help =>
        F.pure(helpMsg.some)

      case Debug(level) =>
        stateRef.modify(_.copy(debugLevel = level)) *>
          F.pure(s"Set debug level: $level".some)

      case SummaryCount(rows) =>
        val count: Option[Option[Int Refined Positive]] =
          if (rows === 0) Some(None)
          else refineV[Positive](rows).fold(κ(None), p => Some(Some(p)))
        count match {
          case None => F.pure("Rows must be a positive integer or 0 to indicate no limit".some)
          case Some(c) => stateRef.modify(_.copy(summaryCount = c)) *>
            F.pure(s"Set rows to show in result: $rows".some)
        }

      case Format(fmt) =>
        stateRef.modify(_.copy(format = fmt)) *>
          F.pure(s"Set output format: $fmt".some)

      case SetPhaseFormat(fmt) =>
        stateRef.modify(_.copy(phaseFormat = fmt)) *>
          F.pure(s"Set phase format: $fmt".some)

      case SetTimingFormat(fmt) =>
        stateRef.modify(_.copy(timingFormat = fmt)) *>
          F.pure(s"Set timing format: $fmt".some)

      case SetVar(n, v) =>
        stateRef.modify(state => state.copy(variables = state.variables + (n -> v))) *>
          F.pure(s"Set variable ${n.value} = ${v.value}".some)

      case UnsetVar(n) =>
        stateRef.modify(state => state.copy(variables = state.variables - n)) *>
          F.pure(s"Unset variable ${n.value}".some)

      case ListVars =>
        stateRef.get.map(_.variables.value).map(
          _.toList.map { case (VarName(name), VarValue(value)) => s"$name = $value" }
            .mkString("Variables:\n", "\n", "").some)

      case DataSources =>
        sources.metadata.map(
          _.toList.map { case (k, v) => s"${k.value} - ${printMetadata(v)}" }
            .mkString("Datasources:\n", "\n", "").some)

      case DataSourceTypes =>
        doSupportedTypes.map(
          _.toList.map(tp => s"${tp.name} (${tp.version})")
            .mkString("Supported datasource types:\n", "\n", "").some)

      case DataSourceLookup(name) =>
        (sources.lookup(name) >>=
          fromEither[CommonError, (DataSourceMetadata, Json)]).map
          { case (metadata, cfg) =>
              List("Datasource:", s"${printMetadata(metadata)} $cfg").mkString("\n").some
          }

      case DataSourceAdd(name, tp, cfg, onConflict) =>
        for {
          tps <- supportedTypes
          dsType <- findTypeF(tps, tp)
          cfgJson <- JsonParser.parse(cfg).fold(raiseEvalError, _.point[F])
          c <- sources.add(name, dsType, cfgJson, onConflict)
          _ <- ensureNormal(c)
        } yield s"Added datasource ${name.value}".some

      case DataSourceRemove(name) =>
        (sources.remove(name) >>= ensureNormal[CommonError]).map(
          κ(s"Removed datasource $name".some))

      case Cd(path: ReplPath) =>
        for {
          cwd <- stateRef.get.map(_.cwd)
          dir = newPath(cwd, path)
          _ <- ensureValidDir(dir)
          _ <- stateRef.modify(_.copy(cwd = dir))
        } yield s"cwd is now ${printPath(dir)}".some

      case Ls(path: Option[ReplPath]) =>
        def printType(tpe: ResourcePathType): String = tpe match {
          case ResourcePathType.ResourcePrefix => "prefix  "
          case ResourcePathType.Resource       => "resource"
        }

        def convert(s: Stream[G, (ResourceName, ResourcePathType)])
            : G[Option[String]] =
          s.map { case (name, tpe) => s"${printType(tpe)} ${name.value}" }
            .compile.toVector.map(_.mkString("type     name\n---------------------\n", "\n", "").some)

        for {
          cwd <- stateRef.get.map(_.cwd)
          p = path.map(newPath(cwd, _)).getOrElse(cwd)
          cs <- children(p)
          res <- gTof(convert(cs))
        } yield res

      case Select(q) =>
        def convert(format: OutputFormat, s: Stream[G, Data]): G[Option[String]] =
          s.compile.toList.map(ds => renderData(format, ds).some)

        for {
          state <- stateRef.get
          qres <- evaluateQuery(SqlQuery(q, state.variables, toADir(state.cwd)), state.summaryCount)
          res <- gTof(convert(state.format, qres))
        } yield res

      case Exit =>
        F.pure("Exiting...".some)
    }

    private def doSupportedTypes: F[ISet[DataSourceType]] =
      sources.supported >>!
        (types => stateRef.modify(_.copy(supportedTypes = types.some)))

    private def ensureNormal[E: Show](c: Condition[E]): F[Unit] =
      c match {
        case Condition.Normal() => F.unit
        case Condition.Abnormal(err) => raiseEvalError(err.shows)
      }

    // TODO
    // We could enhance isResource(path): F[Boolean] to
    // getResourceTypes(path): ISet[ResourcePathType]
    // Then this impl would only need 1 api call.
    // Note that with the current impl we assume that a path cannot be both
    // a prefix and a resource
    private def ensureValidDir(p: ResourcePath): F[Unit] =
      children(p) *>
        (queryEvaluator.isResource(p) >>= { b =>
          if (b) raiseEvalError(s"$p is a resource not a dir")
          else F.unit
        })

    private def evaluateQuery(q: SqlQuery, summaryCount: Option[Int Refined Positive]): F[Stream[G, Data]] =
      (queryEvaluator.evaluate(q) >>=
        fromEither[ResourceError.ReadError, Stream[G, Data]]).map(s =>
          summaryCount.map(c => s.take(c.value.toLong)).getOrElse(s))

    private def findType(tps: ISet[DataSourceType], tp: DataSourceType.Name): Option[DataSourceType] =
      tps.toList.find(_.name === tp)

    private def findTypeF(tps: ISet[DataSourceType], tp: DataSourceType.Name): F[DataSourceType] =
      findType(tps, tp) match {
        case None => raiseEvalError(s"Unsupported datasource type: $tp")
        case Some(z) => z.point[F]
      }

    private def formatJson(codec: DataCodec)(data: Data): Option[String] =
      codec.encode(data).map(_.pretty(minspace))

    private def fromEither[E: Show, A](e: E \/ A): F[A] =
      e match {
        case -\/(err) => raiseEvalError(err.shows)
        case \/-(a) => a.point[F]
      }

    private def gTof[A](ga: G[A]): F[A] = LiftIO[F].liftIO(ga.to[IO])

    // This is just similar to `..` from file systems, not meant to
    // be equivalent. E.g. a difference with `..`` from filesystem is that
    // `cd ../..` from dir `/mydir` won't give an error but evaluates to `/`
    private def interpretDotsAsParent(p: ResourcePath): ResourcePath = {
      val names = ResourcePath.resourceNamesIso.get(p)
      val interpreted = names.foldLeft(IList.empty[ResourceName]) { case (acc, n) =>
        if (n === ResourceName("..")) acc.dropRight(1)
        else acc :+ n
      }
      ResourcePath.resourceNamesIso(interpreted)
    }

    private def newPath(cwd: ResourcePath, change: ReplPath): ResourcePath =
      change match {
        case ReplPath.Absolute(p) => interpretDotsAsParent(p)
        case ReplPath.Relative(p) => interpretDotsAsParent(cwd ++ p)
      }

    private def printMetadata(m: DataSourceMetadata): String =
      s"${m.kind.name} ${m.kind.version} ${printCondition[Exception](m.status, _.getMessage)}"

    private def printCondition[A](c: Condition[A], onAbnormal: A => String) =
      c match {
        case Condition.Normal() => "ok"
        case Condition.Abnormal(a) => s"error: ${onAbnormal(a)}"
      }

    private def printPath(p: ResourcePath): String =
      posixCodec.printPath(p.toPath)

    private def raiseEvalError[A](s: String): F[A] =
      F.raiseError(new EvalError(s))

    private def recoverSomeErrors(fa: F[Option[String]]): F[Option[String]] =
      F.recover(fa) {
        case ee: EvalError => s"Evaluation error: ${ee.getMessage}".some
        case QuasarError.throwableP(qe) => s"Quasar error: $qe".some
      }

    private def renderData(format: OutputFormat, ds: List[Data]): String =
      (format match {
        case OutputFormat.Table =>
          Prettify.renderTable(ds)
        case OutputFormat.Precise =>
          ds.map(formatJson(DataCodec.Precise)).unite
        case OutputFormat.Readable =>
          ds.map(formatJson(DataCodec.Readable)).unite
        case OutputFormat.Csv =>
          Prettify.renderValues(ds).map(CsvWriter(none)(_).trim)
      }).mkString("\n")

    private def supportedTypes: F[ISet[DataSourceType]] =
      stateRef.get.map(_.supportedTypes) >>=
        (_.map(_.point[F]).getOrElse(doSupportedTypes))

    private def toADir(path: ResourcePath): ADir =
      path.fold(f => fileParent(f) </> dir(fileName(f).value), rootDir)

}

object Evaluator {
  //TODO change back to exitCode: Option[ExitCode] once we are back on
  //cats-effect 1.0.0
  final case class Result(exitCode: Option[Unit], string: Option[String])

  final class EvalError(msg: String) extends java.lang.RuntimeException(msg)

  def apply[F[_]: Monad: Effect, G[_]: Functor: Effect](
    stateRef: Ref[F, ReplState],
    sources: DataSources[F, Json],
    queryEvaluator: QueryEvaluator[F, Stream[G, ?], SqlQuery, Stream[G, Data]])
      : Evaluator[F, G] =
    new Evaluator[F, G](stateRef, sources, queryEvaluator)

  val helpMsg =
    """Quasar REPL, Copyright © 2014–2018 SlamData Inc.
      |
      |Available commands:
      |  exit
      |  help
      |  types
      |  datasources
      |  add [name] [type] (preserve | replace) [cfg]
      |  rm [name]
      |  get [name]
      |  cd [path]
      |  ls [path]
      |  [query]
      |  set format = table | precise | readable | csv
      |  set [var] = [value]
      |  env
      |
      |TODO:
      |  set debug = 0 | 1 | 2
      |  set phaseFormat = tree | code
      |  set timingFormat = tree | onlytotal
      |  set summaryCount = [rows]""".stripMargin
}
