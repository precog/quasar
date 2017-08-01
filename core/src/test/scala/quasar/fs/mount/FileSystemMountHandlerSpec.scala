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

import slamdata.Predef.String
import quasar.contrib.pathy.{ADir}
import quasar.effect._
import quasar.fp.liftMT
import quasar.fp.free, free._
import quasar.fs.{BackendEffect, FileSystemType, PathError}

import monocle.function.Field1
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._

class FileSystemMountHandlerSpec extends quasar.Qspec {
  import BackendDef._

  type Abort[A]  = Failure[String, A]
  type AbortM[A] = Free[Abort, A]

  type ResMnts     = Mounts[DefinitionResult[AbortM]]
  type ResMntsS[A] = State[ResMnts, A]

  type MountedFs[A]  = AtomicRef[ResMnts, A]

  type Eff0[A] = Coproduct[Abort, MountedFs, A]
  type Eff[A]  = Coproduct[AbortM, Eff0, A]
  type EffM[A] = Free[Eff, A]

  type EffR[A] = (ResMnts, String \/ A)

  val abort = Failure.Ops[String, Abort]

  def eval(rms: ResMnts): EffM ~> EffR =
    new (EffM ~> EffR) {
      type MT[F[_], A] = EitherT[F, String, A]
      type M[A]        = MT[ResMntsS, A]
      import EitherT.eitherTMonad

      val evalAbort: Abort ~> M =
        Failure.toError[M, String]

      val evalMnts: MountedFs ~> ResMntsS =
        AtomicRef.toState[ResMntsS, ResMnts]

      val evalEff: Eff ~> M =
        free.foldMapNT(evalAbort) :+:
        evalAbort                 :+:
        (liftMT[ResMntsS, MT] compose evalMnts)

      def apply[A](ma: EffM[A]) =
        ma.foldMap(evalEff).run(rms)
    }


  val abortFs: BackendEffect ~> AbortM =
    new (BackendEffect ~> AbortM) {
      def apply[A](fs: BackendEffect[A]) =
        abort.fail("FileSystem")
    }

  def fsResult(sig: String): DefinitionResult[AbortM] =
    DefinitionResult[AbortM](abortFs, abort.fail(sig))

  val fsDef = BackendDef.fromPF {
    case (typ, _) if typ == testType =>
      type X[A] = DefErrT[AbortM, A]
      fsResult("DEF").point[X]
  }

  val invalidPath =
    MountingError.pathError composePrism
    PathError.invalidPath  composeLens
    Field1.first

  val fsMounter = FileSystemMountHandler(fsDef)
  val testType = FileSystemType("test")
  val testUri = ConnectionUri("https://test.example.com")
  def mount(d: ADir) = fsMounter.mount[Eff](d, testType, testUri)
  val unmount = fsMounter.unmount[Eff] _

  "FileSystemMountHandler" should {
    "mounting" >> {
      "fails when mounting above an exsiting mount" >> {
        val d1 = rootDir </> dir("foo")
        val d2 = d1 </> dir("bar")

        eval(Mounts.singleton(d2, fsResult("A")))(mount(d1))._2 must beLike {
          case \/-(-\/(err)) => invalidPath.getOption(err) must beSome(d1)
        }
      }

      "fails when mounting below an existing mount" >> {
        val d1 = rootDir </> dir("foo")
        val d2 = d1 </> dir("bar")

        eval(Mounts.singleton(d1, fsResult("B")))(mount(d2))._2 must beLike {
          case \/-(-\/(err)) => invalidPath.getOption(err) must beSome(d2)
        }
      }

      "fails when filesystem creation fails" >> {
        val d = rootDir </> dir("create") </> dir("fails")
        val fsd = Monoid[BackendDef[AbortM]].zero
        val fsm = FileSystemMountHandler(fsd)

        eval(Mounts.empty)(fsm.mount[Eff](d, testType, testUri))._2 must beLike {
          case \/-(-\/(MountingError.InvalidConfig(_, _))) => ok
        }
      }

      "stores the interpreter and cleanup effect when successful" >> {
        val d = rootDir </> dir("success")
        eval(Mounts.empty)(mount(d))._1.toMap.isEmpty must beFalse
      }

      "cleans up previous filesystem when mount is replaced" >> {
        val d = rootDir </> dir("replace") </> dir("cleanup")
        val cln = "CLEAN"
        val (rmnts, signal) = eval(Mounts.singleton(d, fsResult(cln)))(mount(d))

        (rmnts.toMap.isEmpty must beFalse) and (signal must_=== \/.left(cln))
      }
    }

    "unmounting" >> {
      "cleanup a filesystem when unmounted" >> {
        val d = rootDir </> dir("unmount") </> dir("cleanup")
        val undo = "UNDO"

        eval(Mounts.singleton(d, fsResult(undo)))(unmount(d))._2 must_= \/.left(undo)
      }

      "deletes the entry from mounts" >> {
        val d = rootDir </> dir("unmount") </> dir("deletes")

        eval(Mounts.singleton(d, fsResult("C")))(unmount(d))._1.toMap.isEmpty must beTrue
      }
    }
  }
}
