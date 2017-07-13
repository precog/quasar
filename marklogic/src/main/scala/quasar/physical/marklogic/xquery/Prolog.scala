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

package quasar.physical.marklogic.xquery

import slamdata.Predef._

import monocle.{Getter, Prism}
import scalaz.{Show, Order}
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.show._

sealed abstract class Prolog {
  import Prolog._

  def render: String = this match {
    case DefColl(dc)   => dc.render
    case FuncDecl(fd)  => fd.render
    case ModImport(mi) => mi.render
    case NSDecl(ns)    => ns.render
  }
}

object Prolog {
  final case class DefColl(defaultCollation: DefaultCollationDecl) extends Prolog
  final case class FuncDecl(functionDecl: FunctionDecl) extends Prolog
  final case class ModImport(moduleImport: ModuleImport) extends Prolog
  final case class NSDecl(namespaceDecl: NamespaceDecl) extends Prolog

  val Separator: String = ";"

  val defColl = Prism.partial[Prolog, DefaultCollationDecl] {
    case DefColl(dc) => dc
  } (DefColl)

  val funcDecl = Prism.partial[Prolog, FunctionDecl] {
    case FuncDecl(fd) => fd
  } (FuncDecl)

  val modImport = Prism.partial[Prolog, ModuleImport] {
    case ModImport(mi) => mi
  } (ModImport)

  val nsDecl = Prism.partial[Prolog, NamespaceDecl] {
    case NSDecl(ns) => ns
  } (NSDecl)

  val render: Getter[Prolog, String] =
    Getter(_.render)

  implicit val order: Order[Prolog] =
    Order.orderBy(p => (funcDecl.getOption(p), defColl.getOption(p), modImport.getOption(p), nsDecl.getOption(p)))

  implicit val show: Show[Prolog] =
    Show.shows {
      case DefColl(dc)   => dc.shows
      case FuncDecl(fd)  => fd.shows
      case ModImport(mi) => mi.shows
      case NSDecl(ns)    => ns.shows
    }
}
