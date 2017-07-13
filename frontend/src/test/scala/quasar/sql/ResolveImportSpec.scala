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

package quasar.sql

import slamdata.Predef._
import quasar.contrib.pathy.ADir
import quasar.fp.ski.κ
import quasar.SemanticError._

import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz._

class ResolveImportSpec extends quasar.Qspec {
  "Import resolution" >> {
    "simple case" >> {
      val scopedExpr = sqlB"import `/mymodule/`; TRIVIAL(`/foo`)"
      val trivial = FunctionDecl(CIName("Trivial"), List(CIName("from")), sqlE"select * FROM :from")
      val mymodule = rootDir </> dir("mymodule")
      val retrieve: ADir => List[FunctionDecl[Fix[Sql]]] = {
        case `mymodule` => List(trivial)
        case _          => Nil
      }
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, retrieve).run must_=== sqlE"select * from `/foo`".right
    }
    "multiple imports" >> {
      val scopedExpr = sqlB"import `/mymodule/`; import `/othermodule/`; FOO(1) + Bar(2)"
      val foo = FunctionDecl(CIName("foo"), List(CIName("a")), sqlE":a + 1")
      val bar = FunctionDecl(CIName("bar"), List(CIName("a")), sqlE":a + 2")
      val mymodule    = rootDir </> dir("mymodule")
      val otherModule = rootDir </> dir("othermodule")
      val retrieve: ADir => List[FunctionDecl[Fix[Sql]]] = {
        case `mymodule`    => List(foo)
        case `otherModule` => List(bar)
        case _             => Nil
      }
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, retrieve).run must_=== sqlE"(1 + 1) + (2 + 2)".right
    }
    "multiple functions within one import" >> {
      val scopedExpr = sqlB"import `/mymodule/`; FOO(1) + Bar(2)"
      val foo = FunctionDecl(CIName("foo"), List(CIName("a")), sqlE":a + 1")
      val bar = FunctionDecl(CIName("bar"), List(CIName("a")), sqlE":a + 2")
      val mymodule    = rootDir </> dir("mymodule")
      val retrieve: ADir => List[FunctionDecl[Fix[Sql]]] = {
        case `mymodule` => List(foo, bar)
        case _          => Nil
      }
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, retrieve).run must_=== sqlE"(1 + 1) + (2 + 2)".right
    }
    "multiple functions within one import and unused imports" >> {
      val scopedExpr = sqlB"import `/mymodule/`; import `/othermodule/`; FOO(1) + Bar(2)"
      val foo = FunctionDecl(CIName("foo"), List(CIName("a")), sqlE":a + 1")
      val bar = FunctionDecl(CIName("bar"), List(CIName("a")), sqlE":a + 2")
      val mymodule    = rootDir </> dir("mymodule")
      val otherModule = rootDir </> dir("othermodule")
      val retrieve: ADir => List[FunctionDecl[Fix[Sql]]] = {
        case `mymodule`    => List(foo, bar)
        case `otherModule` => Nil
        case _             => Nil
      }
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, retrieve).run must_=== sqlE"(1 + 1) + (2 + 2)".right
    }
    "a function that depends on another" >> {
      val scopedExpr = sqlB"""
        CREATE FUNCTION FOO(:a)
          BEGIN
            tmp := Bar(:a);
            SELECT * FROM tmp
          END;
        CREATE FUNCTION BAR(:a)
          BEGIN
            select * from :a
          END;
        FOO(`/people`)
      """
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, κ(Nil)).run must_=== sqlE"tmp := select * from `/people`; select * from tmp".right
    }
    "multiple use of the same var name" >> {
      val scopedExpr = sqlB"""
        CREATE FUNCTION FOO(:a)
          BEGIN
            COUNT(:a, :a, :a)
          End;
        FOO(1)
      """
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, κ(Nil)).run must_=== sqlE"COUNT(1,1,1)".right
    }
    "multiple arguments" >> {
      val scopedExpr = sqlB"""
        CREATE FUNCTION FOO(:a, :b, :c)
          BEGIN
            COUNT(:a, :b, :c)
          End;
        FOO(1,2,3)
      """
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, κ(Nil)).run must_=== sqlE"COUNT(1,2,3)".right
    }
    "error out if same function defined twice" >> {
      val scopedExpr = sqlB"""
        CREATE FUNCTION FOO(:a)
          BEGIN
            COUNT(:a)
          End;
        CREATE FUNCTION FOO(:a)
          BEGIN
            FLOOR(:a)
          END;
        FOO(1)
      """
      // This error is slightly bizarre because they are both defined in the same scope, but I don't think it's
      // worth creating a special error type for this particular case
      // Besides, this is the kind of thing that should be caught when compiling to LogicalPlan once
      // importing resolution is done at that layer
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, κ(Nil)).run must_===
        AmbiguousFunctionInvoke(CIName("foo"), List((CIName("foo"), rootDir), (CIName("foo"), rootDir))).left
    }
    // This should not be the kind of thing caught by import resolution,
    // but it's a stop gap solution until LogicalPlan has user functions
    "error out if same var name appears multiple times in function signature" >> {
      val func = FunctionDecl(CIName("FOO"), List(CIName("a"), CIName("a")), sqlE"1")
      val scopedExpr = ScopedExpr(sqlE"FOO(1,2)", List(func))
      resolveImportsImpl[Id, Fix](scopedExpr, rootDir, κ(Nil)).run must_===
        InvalidFunctionDefinition(func, "parameter :a is defined multiple times").left
    }
  }
}