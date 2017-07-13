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

import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.free.{flatMapSNT, liftFT, transformIn}
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}

import matryoshka.data.Fix
import matryoshka.implicits._
import monocle.{Lens, Optional}
import monocle.syntax.fields._
import scalaz.{Optional => _, _}
import scalaz.NaturalTransformation.natToFunction
import pathy.Path._

object transformPaths {
  import ReadFile.ReadHandle, WriteFile.WriteHandle

  /** Returns a natural transformation that transforms all paths in `ReadFile`
    * operations using the given transformations.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    */
  def readFile[S[_]](
    inPath: EndoK[AbsPath],
    outPath: EndoK[AbsPath]
  )(implicit
    S: ReadFile :<: S
  ): S ~> Free[S, ?] = {
    import ReadFile._

    val R = ReadFile.Unsafe[S]

    val g = λ[ReadFile ~> Free[S, ?]] {
      case Open(src, off, lim) =>
        R.open(inPath(src), off, lim)
          .bimap(transformErrorPath(outPath), readHFile.modify(outPath(_)))
          .run

      case Read(h) =>
        R.read(readHFile.modify(inPath(_))(h))
          .leftMap(transformErrorPath(outPath))
          .run

      case Close(h) =>
        R.close(readHFile.modify(inPath(_))(h))
    }

    transformIn(g, liftFT[S])
  }

  /** Returns a natural transformation that transforms all paths in `WriteFile`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    */
  def writeFile[S[_]](
    inPath: EndoK[AbsPath],
    outPath: EndoK[AbsPath]
  )(implicit
    S: WriteFile :<: S
  ): S ~> Free[S, ?] = {
    import WriteFile._

    val W = WriteFile.Unsafe[S]

    val g = λ[WriteFile ~> Free[S, ?]] {
      case Open(dst) =>
        W.open(inPath(dst))
          .bimap(transformErrorPath(outPath), writeHFile.modify(outPath(_)))
          .run

      case Write(h, d) =>
        W.write(writeHFile.modify(inPath(_))(h), d)
          .map(_ map transformErrorPath(outPath))

      case Close(h) =>
        W.close(writeHFile.modify(inPath(_))(h))
    }
    transformIn(g, liftFT[S])
  }

  /** Returns a natural transformation that transforms all paths in `ManageFile`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    */
  def manageFile[S[_]](
    inPath: EndoK[AbsPath],
    outPath: EndoK[AbsPath]
  )(implicit
    S: ManageFile :<: S
  ): S ~> Free[S, ?] = {
    import ManageFile._, MoveScenario._

    val M = ManageFile.Ops[S]
    val g = λ[ManageFile ~> Free[S, ?]] {
      case Move(scn, sem) =>
        M.move(
          scn.fold(
            (src, dst) => dirToDir(inPath(src), inPath(dst)),
            (src, dst) => fileToFile(inPath(src), inPath(dst))),
          sem
        ).leftMap(transformErrorPath(outPath)).run

      case Delete(p) =>
        M.delete(inPath(p))
          .leftMap(transformErrorPath(outPath))
          .run

      case TempFile(p) =>
        M.tempFile(inPath(p))
          .bimap(transformErrorPath(outPath), outPath(_))
          .run
    }

    transformIn(g, liftFT[S])
  }

  private def transformFile(inPath: EndoK[AbsPath])(lp: Fix[LP]): Fix[LP] =
    lp.transAna[Fix[LP]](transformLPPaths(natToFunction[AbsPath, AbsPath, File](inPath)))

  /** Returns a natural transformation that transforms all paths in `QueryFile`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    * @param outPathR transforms relative output paths
    */
  def queryFile[S[_]](
    inPath: EndoK[AbsPath],
    outPath: EndoK[AbsPath],
    outPathR: EndoK[RelPath]
  )(implicit
    S: QueryFile :<: S
  ): S ~> Free[S, ?] = {
    import QueryFile._

    val Q = QueryFile.Ops[S]
    val U = QueryFile.Unsafe[S]

    val g = λ[QueryFile ~> Free[S, ?]] {
      case ExecutePlan(lp, out) =>
        Q.execute(transformFile(inPath)(lp), inPath(out))
          .bimap(transformErrorPath(outPath), outPath(_))
          .run.run

      case EvaluatePlan(lp) =>
        U.eval(transformFile(inPath)(lp))
          .leftMap(transformErrorPath(outPath))
          .run.run

      case More(h) =>
        U.more(h)
          .leftMap(transformErrorPath(outPath))
          .run

      case Close(h) =>
        U.close(h)

      case Explain(lp) =>
        Q.explain(transformFile(inPath)(lp))
          .bimap(transformErrorPath(outPath), ExecutionPlan.inputs.modify(_ map outPath))
          .run.run

      case ListContents(d) =>
        Q.ls(inPath(d))
          .leftMap(transformErrorPath(outPath))
          .run

      case FileExists(f) =>
        Q.fileExists(inPath(f))
    }
    transformIn(g, liftFT[S])
  }

  def analyze[S[_]](
    inPath: EndoK[AbsPath],
    outPath: EndoK[AbsPath],
    outPathR: EndoK[RelPath]
  )(implicit
    S: Analyze :<: S
  ): S ~> Free[S, ?] = {
    import Analyze._

    val O = Analyze.Ops[S]

    val g = λ[Analyze ~> Free[S, ?]] {
      case QueryCost(lp) =>
        O.queryCost(transformFile(inPath)(lp)).leftMap(transformErrorPath(outPath)).run
    }
    transformIn(g, liftFT[S])
  }

  /** Returns a natural transformation that transforms all paths in `FileSystem`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    * @param outPathR transforms relative output paths
    */
  def fileSystem[S[_]](
    inPath: EndoK[AbsPath],
    outPath: EndoK[AbsPath],
    outPathR: RelPath ~> RelPath
  )(implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S
  ): S ~> Free[S, ?] = {
    flatMapSNT(readFile[S](inPath, outPath))   compose
    flatMapSNT(writeFile[S](inPath, outPath))  compose
    flatMapSNT(manageFile[S](inPath, outPath)) compose
    queryFile[S](inPath, outPath, outPathR)
  }

  ////

  private val readHFile: Lens[ReadHandle, AFile] =
    ReadHandle.tupleIso composeLens _1

  private val fsUnkRdError: Optional[FileSystemError, AFile] =
    FileSystemError.unknownReadHandle composeLens readHFile

  private val writeHFile: Lens[WriteHandle, AFile] =
    WriteHandle.tupleIso composeLens _1

  private val fsUnkWrError: Optional[FileSystemError, AFile] =
    FileSystemError.unknownWriteHandle composeLens writeHFile

  private val fsPathError: Optional[FileSystemError, APath] =
    FileSystemError.pathErr composeLens PathError.errorPath

  private val fsPlannerError: Optional[FileSystemError, Fix[LP]] =
    FileSystemError.planningFailed composeLens _1

  private def transformErrorPath(
    f: EndoK[AbsPath]
  ): FileSystemError => FileSystemError =
    fsPathError.modify(f(_)) compose
    fsUnkRdError.modify(f(_)) compose
    fsUnkWrError.modify(f(_)) compose
    fsPlannerError.modify(transformFile(f))

  private def transformLPPaths(f: AFile => AFile) = λ[LP ~> LP] {
    // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
    case lp.Read(p) => lp.read(f(mkAbsolute(rootDir, p)))
    case lp         => lp
  }
}
