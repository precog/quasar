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

package quasar.fs

import quasar.LogicalPlan, LogicalPlan.ReadF
import quasar.fp.free.{flatMapSNT, liftFT, transformIn}

import matryoshka.{FunctorT, Fix}, FunctorT.ops._
import monocle.{Lens, Optional}
import monocle.syntax.fields._
import monocle.std.tuple2._
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
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath
  )(implicit
    S: ReadFile :<: S
  ): S ~> Free[S, ?] = {
    import ReadFile._

    val R = ReadFile.Unsafe[S]

    val g = new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]) = rf match {
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
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath
  )(implicit
    S: WriteFile :<: S
  ): S ~> Free[S, ?] = {
    import WriteFile._

    val W = WriteFile.Unsafe[S]

    val g = new (WriteFile ~> Free[S, ?]) {
      def apply[A](wf: WriteFile[A]) = wf match {
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
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath
  )(implicit
    S: ManageFile :<: S
  ): S ~> Free[S, ?] = {
    import ManageFile._, MoveScenario._

    val M = ManageFile.Ops[S]

    val g = new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]) = mf match {
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
    }

    transformIn(g, liftFT[S])
  }

  /** Returns a natural transformation that transforms all paths in `QueryFile`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    * @param outPathR transforms relative output paths
    */
  def queryFile[S[_]](
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath,
    outPathR: RelPath ~> RelPath
  )(implicit
    S: QueryFile :<: S
  ): S ~> Free[S, ?] = {
    import QueryFile._

    val Q = QueryFile.Ops[S]
    val U = QueryFile.Unsafe[S]

    val g = new (QueryFile ~> Free[S, ?]) {

      val translateFile = natToFunction[AbsPath, AbsPath, File](inPath)

      def apply[A](qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) =>
          Q.execute(lp.transAna(transformLPPaths(translateFile)), inPath(out))
            .bimap(transformErrorPath(outPath), outPath(_))
            .run.run

        case EvaluatePlan(lp) =>
          U.eval(lp.transAna(transformLPPaths(translateFile)))
            .leftMap(transformErrorPath(outPath))
            .run.run

        case More(h) =>
          U.more(h)
            .leftMap(transformErrorPath(outPath))
            .run

        case Close(h) =>
          U.close(h)

        case Explain(lp) =>
          Q.explain(lp.transAna(transformLPPaths(translateFile)))
            .leftMap(transformErrorPath(outPath))
            .run.run

        case ListContents(d) =>
          Q.ls(inPath(d))
            .leftMap(transformErrorPath(outPath))
            .run

        case FileExists(f) =>
          Q.fileExists(inPath(f))
      }
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
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath,
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

  private val fsPlannerError: Optional[FileSystemError, Fix[LogicalPlan]] =
    FileSystemError.planningFailed composeLens _1

  private def transformErrorPath(
    f: AbsPath ~> AbsPath
  ): FileSystemError => FileSystemError =
    fsPathError.modify(f(_)) compose
    fsUnkRdError.modify(f(_)) compose
    fsUnkWrError.modify(f(_)) compose
    fsPlannerError.modify(_.transAna(transformLPPaths(natToFunction[AbsPath,AbsPath,File](f))))

  private def transformLPPaths(f: AFile => AFile): LogicalPlan ~> LogicalPlan =
    new (LogicalPlan ~> LogicalPlan) {
      def apply[A](lp: LogicalPlan[A]) = lp match {
        // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
        case ReadF(p) => ReadF(f(mkAbsolute(rootDir, p)))
        case _        => lp
      }
    }
}
