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
import quasar._, RenderTree.ops._
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.ski._

import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

/** Interpreter that just records a log of the actions that are performed. Only a
  * handful of operations actually produce results:
  * - ManageFile.TempFile always produces a file called "tmp" either alongside the
  *     argument (if it's a file path) or inside it (if it's a dir).
  * - QueryFile.ListContents and .FileExists respond based on a map of directory
  *     paths to sets of content nodes. `rootDir` implicitly exists (even if empty),
  *     otherwise `ls` gives a pathError for any dir that does not appear in the map.
  */
object TraceFS {
  val FsType = FileSystemType("trace")

  type Trace[A] = Writer[Vector[RenderedTree], A]

  def qfTrace(paths: Map[ADir, Set[PathSegment]]) = new (QueryFile ~> Trace) {
    import QueryFile._

    def ls(dir: ADir) =
      paths.get(dir).cata(
        _.right,
        if (dir === rootDir) Set[PathSegment]().right
        else FileSystemError.pathErr(PathError.pathNotFound(dir)).left)

    def apply[A](qf: QueryFile[A]): Trace[A] =
      WriterT.writer((Vector(qf.render),
        qf match {
          case ExecutePlan(lp, out) => (Vector.empty, \/-(out))
          case EvaluatePlan(lp)     => (Vector.empty, \/-(ResultHandle(0)))
          case More(handle)         => \/-(Vector.empty)
          case Close(handle)        => ()
          case Explain(lp)          => (Vector.empty, \/-(ExecutionPlan(FsType, lp.toString, ISet.empty)))
          case ListContents(dir)    => ls(dir)
          case FileExists(file)     =>
            ls(fileParent(file)).fold(
              κ(false),
              _.contains(fileName(file).right))
        }))
  }

  def rfTrace = new (ReadFile ~> Trace) {
    import ReadFile._

    def apply[A](rf: ReadFile[A]): Trace[A] =
      WriterT.writer((Vector(rf.render),
        rf match {
          case Open(file, off, lim) => \/-(ReadHandle(file, 0))
          case Read(handle)         => \/-(Vector.empty)
          case Close(handle)        => ()
        }))
  }

  def wfTrace = new (WriteFile ~> Trace) {
    import WriteFile._

    def apply[A](wf: WriteFile[A]): Trace[A] =
      WriterT.writer((Vector(wf.render),
        wf match {
          case Open(file)           => \/-(WriteHandle(file, 0))
          case Write(handle, chunk) => Vector.empty
          case Close(handle)        => ()
        }))
  }

  def mfTrace = new (ManageFile ~> Trace) {
    import ManageFile._

    def apply[A](mf: ManageFile[A]): Trace[A] =
      WriterT.writer((Vector(mf.render),
        mf match {
          case Move(scenario, semantics) => \/-(())
          case Delete(path)              => \/-(())
          case TempFile(near) =>
            \/-(refineType(near).fold(ι, fileParent) </> file("tmp"))
        }))
  }

  def traceFs(paths: Map[ADir, Set[PathSegment]]): FileSystem ~> Trace =
    interpretFileSystem[Trace](qfTrace(paths), rfTrace, wfTrace, mfTrace)

  def traceInterp[A](t: Free[FileSystem, A], paths: Map[ADir, Set[PathSegment]]): (Vector[RenderedTree], A) = {
    new free.Interpreter(traceFs(paths)).interpret(t).run
  }
}
