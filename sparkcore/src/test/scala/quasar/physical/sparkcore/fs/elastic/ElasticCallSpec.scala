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

package quasar.physical.sparkcore.fs.elastic

import slamdata.Predef._
import com.sksamuel.elastic4s.testkit.AlwaysNewLocalNodeProvider
import com.sksamuel.elastic4s.embedded.LocalNode
import scalaz._, Scalaz._, concurrent.Task
import org.specs2.specification.BeforeAfterEach

class ElasticCallSpec
    extends quasar.Qspec
    with AlwaysNewLocalNodeProvider
    with BeforeAfterEach {

  sequential

  java.lang.System.setProperty("es.set.netty.runtime.available.processors", "false")

  val elastic = new ElasticCall.Ops[ElasticCall]

  val interpreter = ElasticCall.interpreter("localhost", 9200)

  var node: Option[LocalNode] = None

  def healthCheck(tried: Int): Task[Boolean] =
    if (tried > 5)
      false.point[Task]
    else {
      val attemptedConnection = elastic.listIndices.foldMap(interpreter).void.attempt
      attemptedConnection >>= (_.fold(
        _ => Task.delay(java.lang.Thread.sleep(200)) *> healthCheck(tried + 1),
        _ => true.point[Task]
      ))
    }

  def before = {
    node = getNode.some
    healthCheck(0).unsafePerformSync
  }

  def after = {
    node.map(_.stop(removeData = true))
    node = None
  }

  def execute[A](program: Free[ElasticCall, A]): A =
    program.foldMap(interpreter).unsafePerformSync

  "CopyType" should {
    "copy content of existing type to non-existing type" in {
      val program = for {
        _ <- elastic.createIndex("foo")
        _ <- elastic.indexInto(IndexType("foo", "bar"), List(("key" -> "value")))
        _ <- elastic.indexInto(IndexType("foo", "baz"), List(("key" -> "value")))
        _ <- elastic.createIndex("foo2")
        _ <- elastic.copyType(IndexType("foo", "bar"), IndexType("foo2", "bar2"))
      } yield ()

      val checkProgram = for {
        bar2Exists <- elastic.typeExists(IndexType("foo2", "bar2"))
        bazExists <- elastic.typeExists(IndexType("foo2", "baz"))
      } yield (bar2Exists, bazExists)

      execute(program)
      java.lang.Thread.sleep(2000) // test needs to wait for the views to be updated
      val (bar2Exists, bazExists) = execute(checkProgram)
      bar2Exists must_== true
      bazExists must_== false
    }
  }

  "CreateIndex" should {
    "create new index" in {
      val program = for {
        _ <- elastic.createIndex("hello")
        exists <- elastic.indexExists("hello")
      } yield exists

      val created = execute(program)
      created must_== true
    }

    "not fail if index already exitst" in {
      val program = for {
        _ <- elastic.createIndex("hello")
        _ <- elastic.createIndex("hello")
        exists <- elastic.indexExists("hello")
      } yield exists

      val created = execute(program)
      created must_== true
    }
  }

  "DeleteIndex" should {
    "delete existing index" in {
      val program = for {
        _ <- elastic.createIndex("hello")
        _ <- elastic.deleteIndex("hello")
        exists <- elastic.indexExists("hello")
      } yield exists

      val exists = execute(program)
      exists must_== false
    }

    "not fail if index does not exists" in {
      val program = for {
        _ <- elastic.deleteIndex("hello")
        exists <- elastic.indexExists("hello")
      } yield exists

      val exists = execute(program)
      exists must_== false
    }

    "not fail if index contains types with documents" in {
      val program = for {
        _ <- elastic.createIndex("foo")
        _ <- elastic.indexInto(IndexType("foo", "bar"), List(("key" -> "value")))
        _ <- elastic.deleteIndex("hello")
        exists <- elastic.indexExists("hello")
      } yield exists

      val exists = execute(program)
      exists must_== false
    }
  }

  "ListIndices" should {
    "list all existing indices" in {
      val program = for {
        _ <- elastic.createIndex("bar")
        _ <- elastic.createIndex("baz")
        indices <- elastic.listIndices
      } yield indices

      val indices = execute(program)
      indices must contain("bar")
      indices must contain("baz")
    }

    "return empty list if there are no indices in the system" in {
      val program = for {
        indices <- elastic.listIndices
      } yield indices

      val indices = execute(program)
      indices must_== List.empty[String]
    }
  }

  "TypeExists" should {
    "return true if type exists" in {
      val program = for {
        _ <- elastic.createIndex("foo")
        _ <- elastic.indexInto(IndexType("foo", "bar"), List(("key" -> "value")))
        exists <- elastic.typeExists(IndexType("foo", "bar"))
      } yield exists

      val exists = execute(program)
      exists must_== true
    }

    "return false if type does not exist" in {
      val program = for {
        _ <- elastic.createIndex("foo")
        exists <- elastic.typeExists(IndexType("foo", "bar"))
      } yield exists

      val exists = execute(program)
      exists must_== false
    }

    "return false if even indec does not exist" in {
      val program = for {
        exists <- elastic.typeExists(IndexType("foo", "bar"))
      } yield exists

      val exists = execute(program)
      exists must_== false
    }
  }

  "ListTypes" should {
    "list all types for given index" in {
      val program = for {
        _ <- elastic.createIndex("foo")
        _ <- elastic.indexInto(IndexType("foo", "bar"), List(("key" -> "value")))
        _ <- elastic.indexInto(IndexType("foo", "baz"), List(("key" -> "value")))
        types <- elastic.listTypes("foo")
      } yield types

      val types = execute(program)
      types must contain("bar")
      types must contain("baz")
    }

    "return empty List if index has no types" in {
      val program = for {
        _ <- elastic.createIndex("foo")
        types <- elastic.listTypes("foo")
      } yield types

      val types = execute(program)
      types must_== List.empty[String]
    }

    "return empty List if index has no types" in {
      val program = for {
        _ <- elastic.createIndex("foo")
        types <- elastic.listTypes("foo")
      } yield types

      val types = execute(program)
      types must_== List.empty[String]
    }

    "return empty List if index does not exist" in {
      val program = for {
        types <- elastic.listTypes("foo")
      } yield types

      val types = execute(program)
      types must_== List.empty[String]
    }
  }

  "DeleteType" should {
    "delete existing type from existsing index" in {
      val program = for {
        _ <- elastic.createIndex("foo")
        _ <- elastic.indexInto(IndexType("foo", "bar"), List(("key" -> "value")))
        _ <- elastic.deleteType(IndexType("foo", "bar"))
        exists <- elastic.typeExists(IndexType("foo", "bar"))
      } yield exists

      val exists = execute(program)
      exists must_== false
    }
  }
}
