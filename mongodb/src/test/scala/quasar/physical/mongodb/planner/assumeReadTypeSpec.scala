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

package quasar.physical.mongodb.planner

import quasar.common.SortDir
import quasar.contrib.pathy._
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.physical.mongodb._
import quasar.qscript._
import quasar.{Qspec, TreeMatchers, Type}
import slamdata.Predef._

import eu.timepit.refined.auto._
import matryoshka.data._
import matryoshka.{Hole => _, _}
import org.scalacheck._
import pathy.Path._
import scalaz._

class assumeReadTypeSpec extends Qspec with TTypes[Fix] with TreeMatchers {
  val MAX_DEPTH = 4

  val dsl =
    quasar.qscript.construction.mkDefaults[Fix, fs.MongoQScript[Fix, ?]]

  import dsl._

  val json = Fixed[Fix[EJson]]

  def assumeReadTp(qs: Fix[fs.MongoQScript[Fix, ?]]): Either[quasar.fs.FileSystemError, Fix[fs.MongoQScript[Fix, ?]]] =
    Trans(assumeReadType[Fix, fs.MongoQScript[Fix, ?], quasar.fs.FileSystemError \/ ?](Type.AnyObject), qs).toEither

  val rewriteSrcFree0 = free.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), ExcludeId)
  val rewriteSrcFix0 = fix.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), ExcludeId)
  val noRewriteSrcFree0 = free.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), IncludeId)
  val noRewriteSrcFix0 = fix.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), IncludeId)

  def nextFix(s: Free[fs.MongoQScript[Fix, ?], Hole]) = Gen.const(
    fix.Subset(fix.Unreferenced, s, Take, free.Map(free.Unreferenced, recFunc.Constant(json.int(1)))))

  def nextFree(s: Free[fs.MongoQScript[Fix, ?], Hole]) = Gen.oneOf(
    free.Sort(s, Nil, NonEmptyList((func.Hole, SortDir.Ascending))),
    free.Subset(free.Unreferenced, s, Take, free.Map(free.Unreferenced, recFunc.Constant(json.int(1)))),
    free.Subset(s, free.Hole, Take, free.Map(free.Unreferenced, recFunc.Constant(json.int(1)))))

  def genRewriteSrcFree(maxDepth: Int = MAX_DEPTH): Gen[Free[fs.MongoQScript[Fix, ?], Hole]] =
    if (maxDepth <= 0)
      Gen.const(rewriteSrcFree0)
    else
      for {
        s <- genRewriteSrcFree(maxDepth - 1)
        n <- nextFree(s)
      } yield n

  def genRewriteSrcFix(maxDepth: Int = MAX_DEPTH): Gen[Fix[fs.MongoQScript[Fix, ?]]] =
    if (maxDepth <= 0)
      Gen.const(rewriteSrcFix0)
    else
      for {
        s <- genRewriteSrcFree(maxDepth - 1)
        n <- nextFix(s)
      } yield n

  def genNoRewriteSrcFix(maxDepth: Int = MAX_DEPTH): Gen[Fix[fs.MongoQScript[Fix, ?]]] =
    Gen.const(noRewriteSrcFix0)

  def guard(mf: RecFreeMap) = recFunc.Guard(recFunc.Hole, Type.AnyObject, mf, recFunc.Undefined)

  val fun = recFunc.ProjectKeyS(recFunc.Hole, "_id")

  def assertElide(qs: RecFreeMap => Fix[fs.MongoQScript[Fix, ?]]) =
    assumeReadTp(qs(guard(fun))) must beRight(beTreeEqual(qs(fun)))
  def assertNoElide(qs: RecFreeMap => Fix[fs.MongoQScript[Fix, ?]]) =
    assumeReadTp(qs(guard(fun))) must beRight(beTreeEqual(qs(guard(fun))))

  def elideProps(qs: Fix[fs.MongoQScript[Fix, ?]] => RecFreeMap => Fix[fs.MongoQScript[Fix, ?]]) = {
    "elide" >> Prop.forAll(genRewriteSrcFix()) { src =>
      assertElide(qs(src))
    }
    "not elide" >> Prop.forAll(genNoRewriteSrcFix()) { src =>
      assertNoElide(qs(src))
    }
  }

  "assumeReadType" >> {
    "Filter condition" >> elideProps(src => fm => fix.Filter(src, fm))
    "Leftshift struct" >> elideProps(src => fm => fix.LeftShift(src, fm, IncludeId, ShiftType.Array, OnUndefined.Omit, func.LeftSide))
    "Subset from" >> elideProps(src => fm => fix.Subset(src, free.Map(free.Hole, fm), Take, free.Hole))
    "Subset count" >> elideProps(src => fm => fix.Subset(src, free.Hole, Take, free.Map(free.Hole, fm)))
    "Subset both" >> elideProps(src => fm => fix.Subset(src, free.Map(free.Hole, fm), Take, free.Map(free.Hole, fm)))
    "Union left" >> elideProps(src => fm => fix.Union(src, free.Map(free.Hole, fm), free.Hole))
    "Union right" >> elideProps(src => fm => fix.Union(src, free.Hole, free.Map(free.Hole, fm)))
    "Union both" >> elideProps(src => fm => fix.Union(src, free.Map(free.Hole, fm), free.Map(free.Hole, fm)))
  }
}
