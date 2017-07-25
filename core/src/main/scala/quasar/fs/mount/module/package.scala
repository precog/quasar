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

package quasar.fs.mount

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fp.free._
import quasar.fs._, FileSystemError._, PathError._, MountType._
import quasar.sql.FunctionDecl

import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._

package object module {
  import MountConfig._

  /** Intercept and fail any read to a module path; all others are passed untouched. */
  def readFile[S[_]](
                      implicit
                      S0: ReadFile :<: S,
                      S4: Mounting :<: S
                    ): ReadFile ~> Free[S, ?] = {
    import ReadFile._

    val readUnsafe = ReadFile.Unsafe[S]
    val mount = Mounting.Ops[S]

    λ[ReadFile ~> Free[S, ?]] {
      case Open(file, off, lim) =>
        mount.havingPrefix(rootDir).flatMap { map =>
          val modules = map.filter { case (k, v) => v ≟ ModuleMount.right }
          if(modules.keys.exists { path => refineType(path).fold(dir => file.relativeTo(dir).isDefined, _ => false) })
            pathErr(invalidPath(file, "Cannot read file in a module.")).left.point[Free[S, ?]]
          else readUnsafe.open(file, off, lim).run
        }
      case Read(handle)  => readUnsafe.read(handle).run
      case Close(handle) => readUnsafe.close(handle)
    }
  }

  /** Intercept and fail any write to a module path; all others are passed untouched. */
  def writeFile[S[_]](
                       implicit
                       S0: WriteFile :<: S,
                       S1: Mounting :<: S
                     ): WriteFile ~> Free[S, ?] = {
    val mount = Mounting.Ops[S]
    nonFsMounts.failSomeWrites(
      on = file => mount.lookupType(file).run.run.map(_.filter(_ ≟ ModuleMount.right).isDefined),
      message = "Cannot write to a module.")
  }

  /** Overlay modules when enumerating files and directories. */
  def queryFile[S[_]](
                       implicit
                       S0: QueryFile :<: S,
                       S1: Mounting :<: S
                     ): QueryFile ~> Free[S, ?] = {
    import QueryFile._

    val query = QueryFile.Ops[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    val mount = Mounting.Ops[S]

    def listModules(dir: ADir): Free[S, Set[PathSegment]] =
      mount.modulesHavingPrefix(dir).map(_ foldMap { d =>
        d.relativeTo(dir).flatMap(firstSegmentName).toSet
      })

    def moduleFiles(dir: ADir): OptionT[Free[S, ?], Set[PathSegment]] =
      mount.lookupConfig(dir).run
        .flatMap(i => OptionT((i.toOption >>= (c => moduleConfig.getOption(c))).η[Free[S, ?]]))
        .map(statements =>
          statements.collect { case FunctionDecl(name, _, _) => liftFileName(FileName(name.value)) }.toSet)

    λ[QueryFile ~> Free[S, ?]] {
      case ExecutePlan(lp, out) =>
        query.execute(lp, out).run.run

      case EvaluatePlan(lp) =>
        queryUnsafe.eval(lp).run.run

      case More(handle) =>
        queryUnsafe.more(handle).run

      case Close(handle) =>
        queryUnsafe.close(handle)

      case Explain(lp) =>
        query.explain(lp).run.run

      case ListContents(dir) =>
        // If this directory is a module, return the "files" within it, or else add modules to the underlying call to ls
        moduleFiles(dir).map(_.right[FileSystemError]).getOrElseF {
          (listModules(dir) |@| query.ls(dir).run) ((mls, qls) => qls match {
            case \/-(ps) =>
              (ps ++ mls).right
            case -\/(err@PathErr(PathNotFound(_))) =>
              if (mls.nonEmpty) mls.right else err.left
            case -\/(v) =>
              v.left
          })
        }

      case FileExists(file) =>
          query.fileExists(file)
    }
  }

  def fileSystem[S[_]](
                        implicit
                        S0: ReadFile :<: S,
                        S1: WriteFile :<: S,
                        S2: ManageFile :<: S,
                        S3: QueryFile :<: S,
                        S4: MonotonicSeq :<: S,
                        S5: ViewState :<: S,
                        S6: Mounting :<: S,
                        S7: MountingFailure :<: S,
                        S8: PathMismatchFailure :<: S
                      ): FileSystem ~> Free[S, ?] = {
    val mount = Mounting.Ops[S]
    val manageFile = nonFsMounts.manageFile(dir => mount.modulesHavingPrefix_(dir).map(paths => paths.map(p => (p:RPath))))
    interpretFileSystem[Free[S, ?]](queryFile, readFile, writeFile, manageFile)
  }
  // FIX-ME
  def backendEffect[S[_]](
                        implicit
                        S0: ReadFile :<: S,
                        S1: WriteFile :<: S,
                        S2: ManageFile :<: S,
                        S3: QueryFile :<: S,
                        S4: MonotonicSeq :<: S,
                        S5: ViewState :<: S,
                        S6: Mounting :<: S,
                        S7: MountingFailure :<: S,
                        S8: PathMismatchFailure :<: S,
                        S9: Analyze :<: S
                      ): BackendEffect ~> Free[S, ?] = {
    (injectFT[Analyze, S]) :+: fileSystem[S]
  }
}
