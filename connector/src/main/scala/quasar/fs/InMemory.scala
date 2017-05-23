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

import slamdata.Predef._
import quasar.Data
import quasar.Planner.UnsupportedPlan
import quasar.common.{PhaseResult, PhaseResults}
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fp.ski._
import quasar.frontend.{logicalplan => lp}, lp._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import shapeless._

/** In-Memory FileSystem interpreters, useful for testing/stubbing
  * when a "real" interpreter isn't needed or desired.
  *
  * NB: Since this is in-memory, careful with writing large amounts of data to
  *     the file system.
  */
object InMemory {
  import ReadFile._, WriteFile._, ManageFile._, QueryFile._
  import FileSystemError._, PathError._

  type FileMap = Map[AFile, Vector[Data]]
  type RM = Map[ReadHandle, Reading]
  type WM = Map[WriteHandle, AFile]
  type QueryResponses = Map[Fix[LogicalPlan], Vector[Data]]
  type ResultMap = Map[ResultHandle, Vector[Data]]

  type InMemoryFs[A]  = State[InMemState, A]

  final case class Reading(f: AFile, start: Natural, lim: Option[Positive], pos: Int)

  /** Represents the current state of the InMemoryFilesystem
    * @param seq Represents the next available uid for a ReadHandle or WriteHandle
    * @param contents The mapping of Data associated with each file in the Filesystem
    * @param rm Currently open [[quasar.fs.ReadFile.ReadHandle]]s
    * @param wm Currently open [[quasar.fs.WriteFile.WriteHandle]]s
    */
  final case class InMemState(
    seq: Long,
    contents: FileMap,
    rm: RM,
    wm: WM,
    queryResps: QueryResponses,
    resultMap: ResultMap)

  object InMemState {
    val empty = InMemState(0, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)

    def fromFiles(files: FileMap): InMemState =
      empty copy (contents = files)
  }

  val readFile: ReadFile ~> InMemoryFs = new (ReadFile ~> InMemoryFs) {
    def apply[A](rf: ReadFile[A]) = rf match {
      case ReadFile.Open(f, off, lim) =>
        (for {
          h <- nextSeq ∘ (ReadHandle(f, _))
          _ <- readingL(h) := Reading(f, off, lim, 0).some
        } yield h.right)

      case ReadFile.Read(h) =>
        readingL(h) flatMap {
          case Some(Reading(f, st, lim, pos)) =>
            fileL(f).st flatMap {
              case Some(xs) =>
                val rIdx =
                  st.value.toInt + pos

                val rCount =
                  rChunkSize                          min
                  lim.cata(_.value.toInt - pos, rChunkSize) min
                  (xs.length - rIdx)

                if (rCount <= 0)
                  Vector.empty.right.point[InMemoryFs]
                else
                  (rPosL(h) := (pos + rCount))
                    .map(κ(xs.slice(rIdx, rIdx + rCount).right))

              case None =>
                Vector.empty.right.point[InMemoryFs]
            }

          case None =>
            unknownReadHandle(h).left.point[InMemoryFs]
        }

      case ReadFile.Close(h) =>
        rClose(h)
    }
  }

  val writeFile: WriteFile ~> InMemoryFs = new (WriteFile ~> InMemoryFs) {
    def apply[A](wf: WriteFile[A]) = wf match {
      case WriteFile.Open(f) =>
        for {
          h <- nextSeq ∘ (WriteHandle(f, _))
          _ <- wFileL(h) := Some(f)
        } yield h.right

      case WriteFile.Write(h, xs) =>
        wFileL(h).st flatMap {
          case Some(f) =>
            fileL(f) mods (_ map (_ ++ xs) orElse Some(xs)) as Vector.empty

          case None =>
            Vector(unknownWriteHandle(h)).point[InMemoryFs]
        }

      case WriteFile.Close(h) =>
        (wFileL(h) := None).void
    }
  }

  val manageFile: ManageFile ~> InMemoryFs = new (ManageFile ~> InMemoryFs) {
    def apply[A](fsa: ManageFile[A]) = fsa match {
      case Move(scenario, semantics) =>
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))

      case Delete(path) =>
        refineType(path).fold(deleteDir, deleteFile)

      case TempFile(path) =>
        nextSeq map (n => refineType(path).fold(
          _ </> file(tmpName(n)),
          renameFile(_, κ(FileName(tmpName(n))))
        ).right[FileSystemError])
    }
  }

  val queryFile: QueryFile ~> InMemoryFs = new (QueryFile ~> InMemoryFs) {
    def apply[A](qf: QueryFile[A]) = qf match {
      case ExecutePlan(lp, out) =>
        evalPlan(lp) { data =>
          (fileL(out) := some(data)) as out
        }

      case EvaluatePlan(lp) =>
        evalPlan(lp) { data =>
          for {
            h <- nextSeq ∘ (ResultHandle(_))
            _ <- resultL(h) := some(data)
          } yield h
        }

      case More(h) =>
        resultL(h) flatMap {
          case Some(xs) =>
            (resultL(h) := some(Vector())) as xs.right

          case None =>
            unknownResultHandle(h).left.point[InMemoryFs]
        }

      case QueryFile.Close(h) =>
        (resultL(h) := none).void

      case Explain(lp) =>
        phaseResults(lp)
          .tuple(queryResponsesL.st map (qrs => executionPlan(lp, qrs).right))

      case ListContents(dir) =>
        ls(dir) map (r => if (dir === rootDir) r.getOrElse(Set()).right else r)

      case FileExists(file) =>
        contentsL.st.map(_.contains(file))
    }

    private def evalPlan[A](lp: Fix[LogicalPlan])
                           (f: Vector[Data] => InMemoryFs[A])
                           : InMemoryFs[(PhaseResults, FileSystemError \/ A)] = {
      phaseResults(lp)
        .tuple(simpleEvaluation(lp).flatMap(f andThen EitherT.right[InMemoryFs, FileSystemError, A]).run)
    }

    private def phaseResults(lp: Fix[LogicalPlan]): InMemoryFs[PhaseResults] =
      queryResponsesL.st map (qrs =>
        Vector(PhaseResult.detail("Lookup in Memory", executionPlan(lp, qrs).physicalPlan)))

    private def executionPlan(lp: Fix[LogicalPlan], queries: QueryResponses): ExecutionPlan =
      ExecutionPlan(
        FileSystemType("in-memory"),
        s"Lookup $lp in $queries",
        lpr.absolutePaths(lp))

    private val optimizer = new Optimizer[Fix[LogicalPlan]]

    private def simpleEvaluation(lp0: Fix[LogicalPlan]): FileSystemErrT[InMemoryFs, Vector[Data]] = {
      val optLp = optimizer.optimize(lp0)
      EitherT[InMemoryFs, FileSystemError, Vector[Data]](State.gets { mem =>
        import quasar.std.StdLib.set.{Drop, Take}
        import quasar.std.StdLib.identity.Squash
        optLp.para[FileSystemError \/ Vector[Data]] {
          case lp.Read(path) =>
            // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
            val aPath = mkAbsolute(rootDir, path)
            fileL(aPath).get(mem).toRightDisjunction(pathErr(pathNotFound(aPath)))
          case InvokeUnapply(Drop, Sized((_,src), (Fix(Constant(Data.Int(skip))),_))) =>
            (src ⊛ skip.safeToInt.toRightDisjunction(unsupported(optLp)))(_.drop(_))
          case InvokeUnapply(Take, Sized((_,src), (Fix(Constant(Data.Int(limit))),_))) =>
            (src ⊛ limit.safeToInt.toRightDisjunction(unsupported(optLp)))(_.take(_))
          case InvokeUnapply(Squash, Sized((_,src))) => src
          case Constant(data) => Vector(data).right
          case other =>
            queryResponsesL
              .get(mem)
              .mapKeys(optimizer.optimize)
              .get(Fix(other.map(_._1)))
              .toRightDisjunction(unsupported(optLp))
        }
      })
    }

    private def unsupported(lp: Fix[LogicalPlan]) =
      planningFailed(lp, UnsupportedPlan(
        lp.project,
        some("In Memory interpreter does not currently support this plan")))
  }

  val fileSystem: FileSystem ~> InMemoryFs =
    interpretFileSystem(queryFile, readFile, writeFile, manageFile)

  def runStatefully(initial: InMemState): Task[InMemoryFs ~> Task] =
    runInspect(initial).map(_._1)

  def runInspect(initial: InMemState): Task[(InMemoryFs ~> Task, Task[InMemState])] =
    TaskRef(initial) map { ref =>
      val trans = new (InMemoryFs ~> Task) {
        def apply[A](mfs: InMemoryFs[A]) =
          ref.modifyS(mfs.run)
      }

      (trans, ref.read)
    }

  def runFs(initial: InMemState): Task[FileSystem ~> Task] =
    runStatefully(initial).map(_ compose fileSystem)

  ////

  private val lpr = new LogicalPlanR[Fix[LogicalPlan]]

  private def tmpName(n: Long) = s"__quasar.gen_$n"

  private val seqL: InMemState @> Long =
    Lens.lensg(s => n => s.copy(seq = n), _.seq)

  private def nextSeq: InMemoryFs[Long] =
    seqL <%= (_ + 1)

  private val contentsL: InMemState @> FileMap =
    Lens.lensg(s => newContents => s.copy(contents = newContents), _.contents)

  private def fileL(f: AFile): InMemState @> Option[Vector[Data]] =
    Lens.mapVLens(f) <=< contentsL

  //----

  /** Chunk size to use for [[Read]]s. */
  private val rChunkSize = 10

  private val rmL: InMemState @> RM =
    Lens.lensg(s => m => s.copy(rm = m), _.rm)

  private val queryResponsesL: InMemState @> QueryResponses =
    Lens.lensg(s => newQueryResps => s.copy(queryResps = newQueryResps), _.queryResps)

  private def readingL(h: ReadHandle): InMemState @> Option[Reading] =
    Lens.mapVLens(h) <=< rmL

  private val readingPosL: Reading @> Int =
    Lens.lensg(r => p => r.copy(pos = p), _.pos)

  private def rPosL(h: ReadHandle): InMemState @?> Int =
    ~readingL(h) >=> PLens.somePLens >=> ~readingPosL

  private def rClose(h: ReadHandle): InMemoryFs[Unit] =
    (readingL(h) := None).void

  //----

  private val wmL: InMemState @> WM =
    Lens.lensg(s => m => s.copy(wm = m), _.wm)

  private def wFileL(h: WriteHandle): InMemState @> Option[AFile] =
    Lens.mapVLens(h) <=< wmL

  //----

  private def fsPathNotFound[A](f: AFile): InMemoryFs[FileSystemError \/ A] =
    pathErr(pathNotFound(f)).left.point[InMemoryFs]

  private def fsPathExists[A](f: AFile): InMemoryFs[FileSystemError \/ A] =
    pathErr(pathExists(f)).left.point[InMemoryFs]

  private def moveDir(src: ADir, dst: ADir, s: MoveSemantics):
      InMemoryFs[FileSystemError \/ Unit] =
    for {
      files <- contentsL.st ∘
                 (_.keys.toStream.map(_ relativeTo src).unite ∘
                   (sufx => (src </> sufx, dst </> sufx)))
      r     <- files.traverse { case (sf, df) => EitherT(moveFile(sf, df, s)) }.run ∘
                 (_ >>= (_.nonEmpty either (()) or pathErr(pathNotFound(src))))
    } yield r

  private def moveFile(src: AFile, dst: AFile, s: MoveSemantics):
      InMemoryFs[FileSystemError \/ Unit] = {
    import MoveSemantics._

    val move0: InMemoryFs[FileSystemError \/ Unit] =
      (fileL(src) <:= None) >>=
        (_.cata(xs => (fileL(dst) := Some(xs)) as ().right, fsPathNotFound(src)))

    s match {
      case Overwrite =>
        move0
      case FailIfExists =>
        fileL(dst).st flatMap (_ ? fsPathExists[Unit](dst) | move0)
      case FailIfMissing =>
        fileL(dst).st flatMap (_ ? move0 | fsPathNotFound(dst))
    }
  }

  private def deleteDir(d: ADir): InMemoryFs[FileSystemError \/ Unit] =
    for {
      ss <- contentsL.st ∘ (_.keys.toStream.map(_ relativeTo d).unite)
      r  <- ss.traverse(f => EitherT(deleteFile(d </> f))).run ∘
              (_ >>= (_.nonEmpty either (()) or pathErr(pathNotFound(d))))
    } yield r

  private def deleteFile(f: AFile): InMemoryFs[FileSystemError \/ Unit] =
    (fileL(f) <:= None) map (_.void \/> pathErr(pathNotFound(f)))

  //----

  private val resultMapL: InMemState @> ResultMap =
    Lens.lensg(s => m => s.copy(resultMap = m), _.resultMap)

  private def resultL(h: ResultHandle): InMemState @> Option[Vector[Data]] =
    Lens.mapVLens(h) <=< resultMapL

  private def ls(d: ADir): InMemoryFs[FileSystemError \/ Set[PathSegment]] =
    contentsL.st map (
      _.keys.toList.map(_ relativeTo d).unite.toNel
        .map(_ foldMap (f => firstSegmentName(f).toSet))
        .toRightDisjunction(pathErr(pathNotFound(d))))
}
