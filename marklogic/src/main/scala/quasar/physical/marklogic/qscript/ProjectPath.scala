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

package quasar.physical.marklogic.qscript

import slamdata.Predef._

import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.fp.free._
import quasar.fp.ski.κ
import quasar.qscript._
import quasar.qscript.{MapFuncsCore => MFCore, MFC => _, _}
import quasar.{RenderTree, NonTerminal, Terminal}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.patterns._
import matryoshka.implicits._
import pathy._, Path._
import scalaz._, Scalaz._

/* TODO switch from ADir to AFile
 *  @tparam A recursive position */
final case class ProjectPath[A](src: A, path: ADir)

object PathProject {
  def unapply[T[_[_]], A](pr: PathMapFunc[T, A]): Option[ProjectPath[A]] =
    Inject[ProjectPath, PathMapFunc[T, ?]].prj(pr)
}

object MFPath {
  def unapply[T[_[_]], A](pr: PathMapFunc[T, A]): Option[MapFuncCore[T, A]] =
    Inject[MapFuncCore[T, ?], PathMapFunc[T, ?]].prj(pr)
}

object ProjectPath extends ProjectPathInstances {
  def elideGuards[T[_[_]]: RecursiveT](fpm: FreePathMap[T]): FreePathMap[T] = {
    val alg: CoPathMapFunc[T, FreePathMap[T]] => CoPathMapFunc[T, FreePathMap[T]] = totally {
      case CoEnv(\/-(MFPath(MFCore.Guard(Embed(CoEnv(\/-(PathProject(_)))), _, cont, _)))) =>
        CoEnv(cont.resume.swap)
    }

    fpm.transCata[FreePathMap[T]](alg)
  }

  def foldProjectField[T[_[_]]: RecursiveT](fm: FreeMap[T]): FreePathMap[T] = {
    val alg: AlgebraicGTransform[(FreeMap[T], ?), FreePathMap[T], CoMapFunc[T, ?], CoPathMapFunc[T, ?]] = {
      case CoEnv(\/-(MFC(MFCore.ProjectField((_, Embed(CoEnv(\/-(PathProject(path))))), (MFCore.StrLit(field), _))))) => {
        val dir0 = path.path </> dir(field)
        val pp   = ProjectPath(path.src, dir0)

        CoEnv(Inject[ProjectPath, PathMapFunc[T, ?]].inj(pp).right)
      }
      case CoEnv(\/-(MFC(MFCore.ProjectField((Embed(CoEnv(src)), _), (MFCore.StrLit(field), _))))) => {
        val dir0 = rootDir[Sandboxed] </> dir(field)
        val desc = src.fold(κ(Free.point[PathMapFunc[T, ?],  Hole](SrcHole)),
          Free.roll(_).mapSuspension(injectNT[MapFunc[T, ?], PathMapFunc[T, ?]]))
        val pp   = ProjectPath(desc, dir0)

        CoEnv(Inject[ProjectPath, PathMapFunc[T, ?]].inj(pp).right)
      }
      case CoEnv(\/-(other)) =>
        CoEnv(Inject[MapFunc[T, ?], PathMapFunc[T, ?]].inj(other.map(_._2)).right)
      case CoEnv(-\/(h)) => CoEnv(h.left)
    }

    fm.transPara[FreePathMap[T]](alg)
  }
}

sealed abstract class ProjectPathInstances {
  implicit def functor: Functor[ProjectPath] =
    new Functor[ProjectPath] {
      def map[A, B](fa: ProjectPath[A])(f: A => B) = ProjectPath(f(fa.src), fa.path)
    }

  implicit def show[A]: Delay[Show, ProjectPath] = new Delay[Show, ProjectPath] {
    def apply[A](sh: Show[A]): Show[ProjectPath[A]] = Show.show { pp =>
      Cord.fromStrings(List("ProjectPath(", sh.shows(pp.src), ")"))
    }
  }

  implicit def equal[A]: Delay[Equal, ProjectPath] = new Delay[Equal, ProjectPath] {
    def apply[A](eq: Equal[A]) = Equal.equalBy(_.path)
  }

  implicit def renderTree[A]: Delay[RenderTree, ProjectPath] =
    Delay.fromNT(λ[RenderTree ~> (RenderTree ∘ ProjectPath)#λ](rt =>
      RenderTree.make(pp =>
        NonTerminal(List("ProjectPath"), none,
          List(rt.render(pp.src), Terminal(List("Path"), prettyPrint(pp.path).some))))))
}
