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
package repl

import slamdata.Predef._
import quasar.api._, datasource._
import quasar.common.data.Data
import quasar.common.resource._
import quasar.contrib.pathy._
import quasar.contrib.std.uuid._
import quasar.fp.minspace
import quasar.fp.ski._
import quasar.frontend.data.DataCodec
import quasar.run.{QuasarError, SqlQuery}
import quasar.run.ResourceRouter.DatasourceResourcePrefix
import quasar.run.optics.{stringUUIDP => UuidString}

import java.lang.Exception
import scala.util.control.NonFatal

import argonaut.{Json, JsonParser, JsonScalaz}, JsonScalaz._
import cats.effect._
import eu.timepit.refined.refineV
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.scalaz._
import fs2.{Stream, StreamApp}, StreamApp.ExitCode
import fs2.async.Ref
import pathy.Path._
import scalaz._, Scalaz._
import shims._

final class Evaluator[F[_]: Effect](
    stateRef: Ref[F, ReplState],
    sources: Datasources[F, Stream[F, ?], UUID, Json],
    queryEvaluator: QueryEvaluator[F, SqlQuery, Stream[F, Data]]) {

  import Command._
  import DatasourceError._
  import Evaluator._

  val F = Effect[F]

  def evaluate(cmd: Command): F[Result] = {
    val exitCode = if (cmd === Exit) Some(ExitCode.Success) else None
    recoverErrors(doEvaluate(cmd)).map(Result(exitCode, _))
  }

  ////

  private def children(path: ResourcePath)
      : F[Stream[F, (ResourceName, ResourcePathType)]] =
    path match {
      case ResourcePath.Root =>
        Stream.emit((ResourceName(DatasourceResourcePrefix), ResourcePathType.prefix))
          .covary[F].point[F]

      case DatasourceResourcePrefix /: ResourcePath.Root =>
        sources.allDatasourceMetadata.map(_.evalMap {
          case (id, DatasourceMeta(_, n, _)) =>
            sources.pathIsResource(id, ResourcePath.root())
              .flatMap(fromEither[ExistentialError[UUID], Boolean])
              .map(b => (
                ResourceName(s"$id"),
                if (b) ResourcePathType.leafResource else ResourcePathType.prefix))
        })

      case DatasourceResourcePrefix /: UuidString(id) /: p =>
        sources.prefixedChildPaths(id, p)
          .flatMap(fromEither[DiscoveryError[UUID], Stream[F, (ResourceName, ResourcePathType)]])

      case _ =>
        fromEither[DiscoveryError[UUID], Stream[F, (ResourceName, ResourcePathType)]](
          pathNotFound[DiscoveryError[UUID]](path).left)
    }

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
            F.pure {
              val r = c.map(_.toString).getOrElse("unlimited")
              s"Set rows to show in result: $r".some
            }
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

      case DatasourceList =>
        sources.allDatasourceMetadata
          .flatMap(_.map({ case (k, v) => s"$k ${printMetadata(v)}" }).compile.toList)
          .map(_.mkString("Datasources:\n", "\n", "").some)

      case DatasourceTypes =>
        doSupportedTypes.map(
          _.toList.map(printType)
            .mkString("Supported datasource types:\n", "\n", "").some)

      case DatasourceLookup(id) =>
        sources.datasourceRef(id)
          .flatMap(fromEither[ExistentialError[UUID], DatasourceRef[Json]])
          .map(ref =>
            List(s"Datasource[$id](name = ${ref.name.shows}, type = ${printType(ref.kind)})", ref.config.spaces2).mkString("\n").some)

      case DatasourceAdd(name, tp, cfg) =>
        for {
          tps <- supportedTypes
          dsType <- findTypeF(tps, tp)
          cfgJson <- JsonParser.parse(cfg).fold(raiseEvalError, _.point[F])
          r <- sources.addDatasource(DatasourceRef(dsType, name, cfgJson))
          i <- r.fold(e => raiseEvalError(e.shows), _.point[F])
        } yield s"Added datasource $i (${name.value})".some

      case DatasourceRemove(id) =>
        (sources.removeDatasource(id) >>= ensureNormal[ExistentialError[UUID]]).map(
          κ(s"Removed datasource $id".some))

      case Cd(path: ReplPath) =>
        for {
          cwd <- stateRef.get.map(_.cwd)
          dir = newPath(cwd, path)
          _ <- ensureValidDir(dir)
          _ <- stateRef.modify(_.copy(cwd = dir))
        } yield s"cwd is now ${printPath(dir)}".some

      case Ls(path: Option[ReplPath]) =>
        def postfix(tpe: ResourcePathType): String = tpe match {
          case ResourcePathType.LeafResource => ""
          case _ => "/"
        }

        def convert(s: Stream[F, (ResourceName, ResourcePathType)])
            : F[Option[String]] =
          s.map { case (name, tpe) => s"${name.value}${postfix(tpe)}" }
            .compile.toVector.map(_.mkString("\n").some)

        for {
          cwd <- stateRef.get.map(_.cwd)
          p = path.map(newPath(cwd, _)).getOrElse(cwd)
          cs <- children(p)
          res <- convert(cs)
        } yield res

      case Pwd =>
        stateRef.get.map(s => printPath(s.cwd).some)

      case Select(q) =>
        def convert(format: OutputFormat, s: Stream[F, Data]): F[Option[String]] =
          s.compile.toList.map(ds => renderData(format, ds).some)

        for {
          state <- stateRef.get
          qres <- evaluateQuery(SqlQuery(q, state.variables, toADir(state.cwd)), state.summaryCount)
          res <- convert(state.format, qres)
        } yield res

      case Exit =>
        F.pure("Exiting...".some)
    }

    private def doSupportedTypes: F[ISet[DatasourceType]] =
      sources.supportedDatasourceTypes >>!
        (types => stateRef.modify(_.copy(supportedTypes = types.some)))

    private def ensureNormal[E: Show](c: Condition[E]): F[Unit] =
      c match {
        case Condition.Normal() => F.unit
        case Condition.Abnormal(err) => raiseEvalError(err.shows)
      }

    private def ensureValidDir(p: ResourcePath): F[Unit] =
      p.fold[F[Unit]](
        f => children(ResourcePath.fromPath(fileParent(f))) flatMap { s =>
          s.exists(t => t._1 === ResourceName(fileName(f).value) && t._2 =/= ResourcePathType.leafResource)
            .compile.fold(false)(_ || _)
            .flatMap(_.unlessM(raiseEvalError(s"${printPath(p)} is not a directory")))
        },
        F.unit)

    private def evaluateQuery(q: SqlQuery, summaryCount: Option[Int Refined Positive]): F[Stream[F, Data]] =
      queryEvaluator.evaluate(q) map { s =>
        summaryCount.map(c => s.take(c.value.toLong)).getOrElse(s)
      }

    private def findType(tps: ISet[DatasourceType], tp: DatasourceType.Name): Option[DatasourceType] =
      tps.toList.find(_.name === tp)

    private def findTypeF(tps: ISet[DatasourceType], tp: DatasourceType.Name): F[DatasourceType] =
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

    private def printType(t: DatasourceType): String =
      s"${t.name}-v${t.version}"

    private def printMetadata(m: DatasourceMeta): String =
      s"${m.name.shows} (${printType(m.kind)}): ${printCondition[Exception](m.status, _.getMessage)}"

    private def printCondition[A](c: Condition[A], onAbnormal: A => String) =
      c match {
        case Condition.Normal() => "ok"
        case Condition.Abnormal(a) => s"error: ${onAbnormal(a)}"
      }

    private def printPath(p: ResourcePath): String =
      posixCodec.printPath(p.toPath)

    private def raiseEvalError[A](s: String): F[A] =
      F.raiseError(new EvalError(s))

    private def recoverErrors(fa: F[Option[String]]): F[Option[String]] =
      F.recover(fa) {
        case ee: EvalError => s"Evaluation error: ${ee.getMessage}".some
        case QuasarError.throwableP(qe) => s"Quasar error: $qe".some
        case NonFatal(t) =>
          (s"Unexpected error: ${t.getClass.getCanonicalName}: ${t.getMessage}" +
            t.getStackTrace.mkString("\n  ", "\n  ", "")).some
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

    private def supportedTypes: F[ISet[DatasourceType]] =
      stateRef.get.map(_.supportedTypes) >>=
        (_.map(_.point[F]).getOrElse(doSupportedTypes))

    private def toADir(path: ResourcePath): ADir =
      path.fold(f => fileParent(f) </> dir(fileName(f).value), rootDir)

}

object Evaluator {
  final case class Result(exitCode: Option[ExitCode], string: Option[String])

  final class EvalError(msg: String) extends java.lang.RuntimeException(msg)

  def apply[F[_]: Effect](
      stateRef: Ref[F, ReplState],
      sources: Datasources[F, Stream[F, ?], UUID, Json],
      queryEvaluator: QueryEvaluator[F, SqlQuery, Stream[F, Data]])
      : Evaluator[F] =
    new Evaluator[F](stateRef, sources, queryEvaluator)

  val helpMsg =
    """Quasar REPL, Copyright © 2014–2018 SlamData Inc.
      |
      |Available commands:
      |  exit
      |  help
      |  ds (list | ls)
      |  ds types
      |  ds add [name] [type] [cfg]
      |  ds (remove | rm) [uuid]
      |  ds (lookup | get) [uuid]
      |  cd [path]
      |  ls [path]
      |  pwd
      |  [query]
      |  set format = table | precise | readable | csv
      |  set summaryCount = [rows]
      |  set [var] = [value]
      |  env
      |
      |TODO:
      |  set debug = 0 | 1 | 2
      |  set phaseFormat = tree | code
      |  set timingFormat = tree | onlytotal""".stripMargin
}
