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

package quasar.mimir

import quasar.blueeyes._, json._

import quasar.precog.TestSupport._
import quasar.precog.common._
import quasar.precog.common.accounts._
import quasar.precog.common.security._
import quasar.precog.util._

import quasar.yggdrasil._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.execution.EvaluationContext
import quasar.yggdrasil.table._
import quasar.yggdrasil.vfs._
import quasar.yggdrasil.util._

import scalaz._, Scalaz._, Validation._

trait EvaluatorSpecification[M[+_]] extends Specification with EvaluatorTestSupport[M] {
  def M = Need.need.asInstanceOf[scalaz.Monad[M] with scalaz.Comonad[M]]
}

trait EvaluatorTestSupport[M[+_]] extends StdLibEvaluatorStack[M]
    with BaseBlockStoreTestModule[M]
    with IdSourceScannerModule
    with SpecificationHelp { outer =>

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M) =
    new Evaluator[N](N0)(mn,nm) {
      val report = new LoggingQueryLogger[N, instructions.Line] with ExceptionQueryLogger[N, instructions.Line] with TimingQueryLogger[N, instructions.Line] {
        val M = N0
      }
      def freshIdScanner = outer.freshIdScanner
    }

  private val groupId = new java.util.concurrent.atomic.AtomicInteger

  def newGroupId = groupId.getAndIncrement

  def testAccount = AccountDetails("00001", "test@email.com", dateTime.now, "testAPIKey", Path.Root, AccountPlan.Free)

  val defaultEvaluationContext = EvaluationContext("testAPIKey", testAccount, Path.Root, Path.Root, dateTime.now)

  val defaultMorphContext = MorphContext(defaultEvaluationContext, new MorphLogger {
    def info(msg: String): M[Unit] = M.point(())
    def warn(msg: String): M[Unit] = M.point(())
    def error(msg: String): M[Unit] = M.point(())
    def die(): M[Unit] = M.point(sys.error("MorphContext#die()"))
  })

  val projections = Map.empty[Path, Projection]
  def vfs = sys.error("VFS metadata not supported in test.")

  trait TableCompanion extends BaseBlockStoreTestTableCompanion {
    override def load(table: Table, apiKey: APIKey, jtpe: JType) = EitherT {
      table.toJson map { events =>
        val eventsV = events.toStream.traverse[Validation[ResourceError, ?], Stream[JValue]] {
          case JString(pathStr) => Validation.success {
            indexLock synchronized {      // block the WHOLE WORLD
              val path = Path(pathStr)

              val index = initialIndices get path getOrElse {
                initialIndices += (path -> currentIndex)
                currentIndex
              }

              val prefix = "filesystem"
              val target = path.path.replaceAll("/$", ".json").replaceAll("^/" + prefix, prefix)

              val src = if (target startsWith prefix)
                io.Source.fromFile(new File(target.substring(prefix.length)))
              else
                io.Source.fromInputStream(getClass.getResourceAsStream(target))

              val parsed: Stream[JValue] = src.getLines map JParser.parseUnsafe toStream

              currentIndex += parsed.length

              parsed zip (Stream from index) map {
                case (value, id) => JObject(JField("key", JArray(JNum(id) :: Nil)) :: JField("value", value) :: Nil)
              }
            }
          }

          case x =>
            Validation.failure(ResourceError.corrupt("Attempted to load JSON as a table from something that wasn't a string: " + x))
        }

        eventsV.disjunction.map(ss => fromJson(ss.flatten))
      }
    }
  }

  object Table extends TableCompanion

  private var initialIndices = scmMap[Path, Int]()    // if we were doing this for real: j.u.c.HashMap
  private var currentIndex   = 0                      // if we were doing this for real: j.u.c.a.AtomicInteger
  private val indexLock      = new AnyRef             // if we were doing this for real: DIE IN A FIRE!!!
}

trait EvaluatorSpecs[M[+_]] extends EvaluatorSpecification[M]
    with LongIdMemoryDatasetConsumer[M] { self =>

  import dag._
  import instructions._

  import library._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph, path: Path = Path.Root, scriptPath: Path = Path.Root, optimize: Boolean = true)(test: Set[SEvent] => SpecsResult): SpecsResult = {
    val ctx = defaultEvaluationContext.copy(basePath = path, scriptPath = scriptPath)
    (consumeEval(graph, ctx, optimize) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    })/* and
    (consumeEval(testAPIKey, graph, ctx, path, false) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    })*/
  }

  "evaluator" should {
    "evaluate simple two-value multiplication" in {
      val line = Line(1, 1, "")

      val input = Join(Mul, Cross(None),
        Const(CLong(6))(line),
        Const(CLong(7))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.isEmpty => d.toInt
        }

        result2.toSet must_== Set(42)
      }
    }

    "evaluate single value roots" >> {
      "push_string" >> {
        val line = Line(1, 1, "")
        val input = Const(CString("daniel"))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SString(str)) if ids.isEmpty => str
          }

          result2.toSet must_== Set("daniel")
        }
      }

      "push_num" >> {
        val line = Line(1, 1, "")
        val input = Const(CLong(42))(line)
        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.isEmpty => d.toInt
          }

          result2.toSet must_== Set(42)
        }
      }

      "push_true" >> {
        val line = Line(1, 1, "")
        val input = Const(CTrue)(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SBoolean(b)) if ids.isEmpty => b
          }

          result2.toSet must_== Set(true)
        }
      }

      "push_false" >> {
        val line = Line(1, 1, "")
        val input = Const(CFalse)(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SBoolean(b)) if ids.isEmpty => b
          }

          result2.toSet must_== Set(false)
        }
      }

      "push_null" >> {
        val line = Line(1, 1, "")
        val input = Const(CNull)(line)

        testEval(input) { result =>
          result must haveSize(1)
          result.map(_._2) must contain(SNull)
        }
      }

      "push_object" >> {
        val line = Line(1, 1, "")
        val input = Const(RObject.empty)(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SObject(obj)) if ids.isEmpty => obj
          }

          result2.toSet must_== Set(Map())
        }
      }

      "push_array" >> {
        val line = Line(1, 1, "")
        val input = Const(RArray.empty)(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.isEmpty => arr
          }

          result2.toList must_== List(Vector())
        }
      }
    }

    "evaluate a absolute_load" in {
      val line = Line(1, 1, "")
      val input = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "evaluate a absolute_load with a base path and not script path" in {
      val line = Line(1, 1, "")
      val input = dag.AbsoluteLoad(Const(CString("/numbers"))(line))(line)

      testEval(input, Path("/hom"), Path("/foo")) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "evaluate a relative_load with just a script path" in {
      val line = Line(1, 1, "")
      val input = dag.RelativeLoad(Const(CString("numbers"))(line))(line)

      testEval(input, Path.Root, Path("/hom")) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "evaluate a relative_load with both a base path and a script path" in {
      val line = Line(1, 1, "")
      val input = dag.RelativeLoad(Const(CString("numbersdiff"))(line))(line)

      testEval(input, Path("/hom"), Path("/stuff")) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "evaluate a join given a relative load" in {
      val line = Line(1, 1, "")

      val numbers = dag.RelativeLoad(Const(CString("numbers"))(line))(line)

      val input = Join(Add, IdentitySort, numbers, numbers)(line)

      testEval(input, Path.Root, Path("/hom")) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(84, 24, 154, 2, 26)
      }
    }

    "evaluate a join given a relative load with two different JTypes" in {
      val line = Line(1, 1, "")

      val numbers = dag.RelativeLoad(Const(CString("numbers"))(line))(line)
      val numbers0 = dag.RelativeLoad(Const(CString("numbers"))(line), JNumberT)(line)

      val input = Join(Add, IdentitySort, numbers, numbers0)(line)

      testEval(input, Path.Root, Path("/hom")) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(84, 24, 154, 2, 26)
      }
    }

    "evaluate a join given a relative load with two different datasets" in {
      val line = Line(1, 1, "")

      val numbers = dag.RelativeLoad(Const(CString("numbers"))(line))(line)
      val numbers2 = dag.RelativeLoad(Const(CString("numbers2"))(line))(line)

      val input = Join(Add, Cross(None), numbers, numbers2)(line)

      testEval(input, Path.Root, Path("/hom")) { result =>
        result must haveSize(30)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 2 => d.toInt
        }

        result2.toSet must_== Set(84,54,119,43,55,43,54,24,89,13,25,13,119,89,154,78,90,78,43,13,78,2,14,2,55,25,90,14,26,14)
      }
    }

    "evaluate a negation mapped over numbers" in {
      val line = Line(1, 1, "")

      val input = Operate(Neg,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(-42, -12, -77, -1, -13)
      }
    }

    "evaluate a new mapped over numbers as no-op" in {
      val line = Line(1, 1, "")

      val input = dag.New(
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "evaluate a new literal" in {
      val line = Line(1, 1, "")

      val input = dag.New(Const(CString("foo"))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SString(s)) if ids.size == 1 => s
        }

        result2.toSet must_== Set("foo")
      }
    }

    "evaluate a join of two reductions on the same dataset" in {
      val line = Line(1, 1, "")

      val parent = dag.AbsoluteLoad(Const(CString("/hom/numbers7"))(line))(line)

      val input = Join(Add, Cross(None),
        dag.Reduce(Count, parent)(line),
        dag.Reduce(Sum, parent)(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2.toSet must_== Set(259)
      }
    }

    "evaluate a join of two reductions on the same dataset using a MegaReduce" in {
      val line = Line(1, 1, "")

      val parent = dag.AbsoluteLoad(Const(CString("/hom/numbers7"))(line))(line)

      val spec = trans.Leaf(trans.Source)
      val reds = List(Count, Sum)
      val mega = dag.MegaReduce(
        List((spec, reds)),
        parent)

      val input = Join(Add, Cross(None),
        joinDeref(mega, 0, 0, line),
        joinDeref(mega, 0, 1, line))(line)

      // We don't optimize since MegaReduce can only be created through an optimization.
      testEval(input, optimize = false) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2.toSet must_== Set(259)
      }
    }

    "MegaReduce of two tuples must return an array" in {
      val line = Line(1, 1, "")

      val parent = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val height = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("height"))
      val weight = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("weight"))
      val mean = List(Mean)
      val max = List(Max)

      val input = dag.MegaReduce(
        List((weight, mean), (height, max)),
        parent)

      // We don't optimize since MegaReduce can only be created through an optimization.
      testEval(input, optimize = false) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SArray(arr)) if ids.size == 0 => arr
        }

        result2.toSet must_== Set(Vector(SArray(Vector(SDecimal(104))), SArray(Vector(SDecimal(138)))))
      }
    }

    "evaluate a join of two reductions on two datasets with the same parent using a MegaReduce" in {
      val line = Line(1, 1, "")

      val parent = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val height = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("height"))
      val weight = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("weight"))
      val mean = List(Mean)
      val max = List(Max)

      val mega = dag.MegaReduce(
        List((weight, mean), (height, max)),
        parent)

      val input = Join(Add, Cross(None),
        joinDeref(mega, 0, 0, line),
        joinDeref(mega, 1, 0, line))(line)

      // We don't optimize since MegaReduce can only be created through an optimization.
      testEval(input, optimize = false) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2.toSet must_== Set(138 + 104)
      }
    }

    "evaluate a join of three reductions on the same dataset using a MegaReduce" in {
      val line = Line(1, 1, "")

      val parent = dag.AbsoluteLoad(Const(CString("/hom/numbers7"))(line))(line)

      val mega = dag.MegaReduce(List((trans.Leaf(trans.Source), List(Count, Sum, Mean))), parent)

      val input = Join(Add, Cross(None),
        joinDeref(mega, 0, 0, line),
        Join(Add, Cross(None),
          joinDeref(mega, 0, 1, line),
          joinDeref(mega, 0, 2, line))(line))(line)

      // We don't optimize since MegaReduce can only be created through an optimization.
      testEval(input, optimize = false) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (Seq(), SDecimal(d)) => d
        }

        result2.toSet must_== Set(237 + 22 + BigDecimal(237.0) / 22)
      }
    }

    "evaluate a rewrite/eval of a 3-way mega reduce" in {
      val line = Line(1, 1, "")

      val load = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val id = Join(DerefObject, Cross(None), load, Const(CString("userId"))(line))(line)
      val height = Join(DerefObject, Cross(None), load, Const(CString("height"))(line))(line)
      val weight = Join(DerefObject, Cross(None), load, Const(CString("weight"))(line))(line)

      val r1 = dag.Reduce(Min, id)(line)
      val r2 = dag.Reduce(Max, height)(line)
      val r3 = dag.Reduce(Mean, weight)(line)

      val input = Join(Sub, Cross(None), r1, Join(Add, Cross(None), r2, r3)(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2.toSet must_== Set(1 - (104 + 138))
      }
    }

    "evaluate a rewrite/eval of reductions" in {
      val line = Line(1, 1, "")

      val load = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val height = Join(DerefObject, Cross(None), load, Const(CString("height"))(line))(line)
      val weight = Join(DerefObject, Cross(None), load, Const(CString("weight"))(line))(line)

      val r1 = dag.Reduce(Min, height)(line)
      val r2 = dag.Reduce(Max, height)(line)
      val r3 = dag.Reduce(Mean, weight)(line)

      val input = Join(Sub, Cross(None), r1, Join(Add, Cross(None), r2, r3)(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2.toSet must_== Set(30 - (104 + 138))
      }
    }

    "three reductions on the same dataset" in {
      val line = Line(1, 1, "")

      val load = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val weight = Join(DerefObject, Cross(None), load, Const(CString("weight"))(line))(line)

      val r1 = dag.Reduce(Min, weight)(line)
      val r2 = dag.Reduce(Max, weight)(line)
      val r3 = dag.Reduce(Mean, weight)(line)

      val input = Join(Sub, Cross(None), r1, Join(Add, Cross(None), r2, r3)(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2.toSet must_== Set(29 - (231 + 138))
      }
    }

    "the same reduction on three datasets" in {
      val line = Line(1, 1, "")

      val load = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val id = Join(DerefObject, Cross(None), load, Const(CString("userId"))(line))(line)
      val height = Join(DerefObject, Cross(None), load, Const(CString("height"))(line))(line)
      val weight = Join(DerefObject, Cross(None), load, Const(CString("weight"))(line))(line)

      val r1 = dag.Reduce(Max, id)(line)
      val r2 = dag.Reduce(Max, height)(line)
      val r3 = dag.Reduce(Max, weight)(line)

      val input = Join(Sub, Cross(None), r1, Join(Add, Cross(None), r2, r3)(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(num)) if ids.size == 0 => num
        }

        result2.toSet must_== Set(22 - (104 + 231))
      }
    }

    "join two sets" >> {
      "from different paths" >> {
        val line = Line(1, 1, "")

        val input = Join(Add, Cross(None),
          Join(DerefObject, Cross(None),
            dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
            Const(CString("time"))(line))(line),
          Join(DerefObject, Cross(None),
            dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(500)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 2 => ids
          }

          result2 must haveSize(500)
        }
      }

      "from the same path" >> {
        val line = Line(1, 1, "")
        val heightWeight = dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line))(line)

        val input = Join(Add, IdentitySort,
          Join(DerefObject, Cross(None),
            heightWeight,
            Const(CString("weight"))(line))(line),
          Join(DerefObject, Cross(None),
            heightWeight,
            Const(CString("height"))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2.toSet must_== Set(218, 147, 118, 172, 224)
        }
      }

      "from the same path (with a relative load)" >> {
        val line = Line(1, 1, "")
        val heightWeight = dag.RelativeLoad(Const(CString("heightWeight"))(line))(line)

        val input = Join(Add, IdentitySort,
          Join(DerefObject, Cross(None),
            heightWeight,
            Const(CString("weight"))(line))(line),
          Join(DerefObject, Cross(None),
            heightWeight,
            Const(CString("height"))(line))(line))(line)

        testEval(input, Path.Root, Path("/hom")) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2.toSet must_== Set(218, 147, 118, 172, 224)
        }
      }
    }

    "evaluate a binary numeric operation mapped over homogeneous numeric set" >> {
      "addition" >> {
        val line = Line(1, 1, "")

        val input = Join(Add, Cross(None),
          dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
          Const(CLong(5))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(47, 17, 82, 6, 18)
        }
      }

      "subtraction" >> {
        val line = Line(1, 1, "")

        val input = Join(Sub, Cross(None),
          dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
          Const(CLong(5))(line))(line)

        testEval(input) { result =>

          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(37, 7, 72, -4, 8)
        }
      }

      "multiplication" >> {
        val line = Line(1, 1, "")

        val input = Join(Mul, Cross(None),
          dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
          Const(CLong(5))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(210, 60, 385, 5, 65)
        }
      }

      "division" >> {
        val line = Line(1, 1, "")

        val input = Join(Div, Cross(None),
          dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
          Const(CLong(5))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
          }

          result2.toSet must_== Set(8.4, 2.4, 15.4, 0.2, 2.6)
        }
      }

      "mod both positive" >> {
        val line = Line(1, 1, "")

        val input = Join(Mod, Cross(None),
          Const(CLong(11))(line),
          Const(CLong(4))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }

          result2.toSet must_== Set(3)
        }
      }
      "mod both negative" >> {
        val line = Line(1, 1, "")

        val input = Join(Mod, Cross(None),
          Const(CLong(-11))(line),
          Const(CLong(-4))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }

          result2.toSet must_== Set(-3)
        }
      }
      "mod negative left" >> {
        val line = Line(1, 1, "")

        val input = Join(Mod, Cross(None),
          Const(CLong(-11))(line),
          Const(CLong(4))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }

          result2.toSet must_== Set(1)
        }
      }
      "mod" >> {
        val line = Line(1, 1, "")

        val input = Join(Mod, Cross(None),
          Const(CLong(11))(line),
          Const(CLong(-4))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }

          result2.toSet must_== Set(-1)
        }
      }

      "pow" >> {
        val line = Line(1, 1, "")

        val input = Join(Pow, Cross(None),
          Const(CLong(11))(line),
          Const(CLong(3))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }

          result2.toSet must_== Set(1331)
        }
      }
    }

    "evaluate a binary numeric operation mapped over heterogeneous numeric set" >> {
      "addition" >> {
        val line = Line(1, 1, "")

        val input = Join(Add, Cross(None),
          dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line),
          Const(CLong(5))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(47, 17, 82, 6, 18)
        }
      }

      "subtraction" >> {
        val line = Line(1, 1, "")

        val input = Join(Sub, Cross(None),
          dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line),
          Const(CLong(5))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(37, 7, 72, -4, 8)
        }
      }

      "multiplication" >> {
        val line = Line(1, 1, "")

        val input = Join(Mul, Cross(None),
          dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line),
          Const(CLong(5))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(210, 60, 385, 5, 65)
        }
      }

      "division" >> {
        val line = Line(1, 1, "")

        val input = Join(Div, Cross(None),
          dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line),
          Const(CLong(5))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
          }

          result2.toSet must_== Set(8.4, 2.4, 15.4, 0.2, 2.6)
        }
      }
    }

    "count a filtered dataset" in {
      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val input = dag.Reduce(Count,
        Filter(IdentitySort,
          clicks,
          Join(Gt, Cross(None),
            Join(DerefObject, Cross(None),
              clicks,
              Const(CString("time"))(line))(line),
            Const(CLong(0))(line))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.isEmpty => d.toInt
        }

        result2.toSet must_== Set(100)
      }
    }

    "produce a non-doubled result when counting the union of new sets" in {
      /*
       * clicks := //clicks
       * clicks' := new clicks
       *
       * count(clicks' union clicks')
       */

      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val clicksP = dag.New(clicks)(line)
      val input = dag.Reduce(Count,
        dag.IUI(true, clicksP, clicksP)(line))(line)

      testEval(input) { resultE =>
        val result = resultE collect {
          case (ids, SDecimal(d)) => d
        }

        result must contain(100)
      }
    }

    "produce a non-zero result when counting the intersect of new sets" in {
      /*
       * clicks := //clicks
       * clicks' := new clicks
       *
       * count(clicks' intersect clicks')
       */

      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val clicksP = dag.New(clicks)(line)
      val input = dag.Reduce(Count,
        dag.IUI(false, clicksP, clicksP)(line))(line)

      testEval(input) { resultE =>
        val result = resultE collect {
          case (ids, SDecimal(d)) => d
        }

        result must contain(100)
      }
    }

    "filter a dataset to return a set of boolean" in {
      val line = Line(1, 1, "")

      val input = Join(Gt, Cross(None),
            Join(DerefObject, Cross(None),
              dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
              Const(CString("time"))(line))(line),
            Const(CLong(0))(line))(line)

      testEval(input) { result =>
        result must haveSize(100)

        val result2 = result collect {
          case (ids, SBoolean(d)) if ids.size == 1 => d
        }

        result2.toSet must_== Set(true)
      }
    }

    "reduce a derefed object" in {
      val line = Line(1, 1, "")

      val input = dag.Reduce(Count,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
          Const(CString("time"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.isEmpty => d.toInt
        }

        result2.toSet must_== Set(100)
      }
    }

    "evaluate cross when one side is a singleton" >> {
      "a reduction on the right side of the cross" >> {
        val line = Line(1, 1, "")

        val input = Join(Add, Cross(None),
          dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
          dag.Reduce(Count,
            Const(CLong(42))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2.toSet must_== Set(43, 13, 78, 2, 14)
        }
      }

      "a reduction on the left side of the cross" >> {
        val line = Line(1, 1, "")

        val input = Join(Add, Cross(None),
          dag.Reduce(Count,
            Const(CLong(42))(line))(line),
          dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2.toSet must_== Set(43, 13, 78, 2, 14)
        }
      }

      "a root on the right side of the cross" >> {
        val line = Line(1, 1, "")

        val input = Join(Add, Cross(None),
          dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
          Const(CLong(3))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2.toSet must_== Set(45, 15, 80, 4, 16)
        }
      }

      "a root on the left side of the cross" >> {
        val line = Line(1, 1, "")

        val input = Join(Add, Cross(None),
          Const(CLong(3))(line),
          dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2.toSet must_== Set(45, 15, 80, 4, 16)
        }
      }
    }

    "evaluate wrap_object on a single numeric value" in {
      val line = Line(1, 1, "")

      val input = Join(WrapObject, Cross(None),
        Const(CString("answer"))(line),
        Const(CLong(42))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val optObj = result find {
          case (ids, SObject(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SObject(obj)) => obj
        }

        optObj must beSome
        val obj = optObj.get

        obj must haveKey("answer")
        obj("answer") must beLike {
          case SDecimal(d) => d must_== 42
        }
      }
    }

    "evaluate wrap_object on an object" in {
      val line = Line(1, 1, "")

      val input = Join(WrapObject, Cross(None),
        Const(CString("answer"))(line),
        Join(WrapObject, Cross(None),
          Const(CString("question"))(line),
          Const(CNull)(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val optObj = result find {
          case (ids, SObject(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SObject(obj)) => obj
        }

        optObj must beSome
        val obj = optObj.get

        obj must haveKey("answer")
        obj("answer") must beLike {
          case SObject(obj) => {
            obj must haveKey("question")
            obj("question") must_== SNull
          }
        }
      }
    }

    "evaluate wrap_object on clicks dataset" in {
      val line = Line(1, 1, "")

      val input = Join(WrapObject, Cross(None),
        Const(CString("aa"))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
          Const(CString("user"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(100)

        result must haveAllElementsLike {
          case (ids, SObject(obj)) =>
            ids must haveSize(1)
            obj must haveSize(1)
            obj must haveKey("aa")
        }
      }
    }

    "evaluate wrap_array on a single numeric value" in {
      val line = Line(1, 1, "")

      val input = Operate(WrapArray,
        Const(CLong(42))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val optArr = result find {
          case (ids, SArray(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }

        optArr must beSome
        val arr = optArr.get

        arr must haveSize(1)
        arr.head must beLike {
          case SDecimal(d) => d must_== 42
        }
      }
    }

    "evaluate wrap_array on a single null value" in {
      val line = Line(1, 1, "")

      val input = Operate(WrapArray,
        Const(CNull)(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val optArr = result find {
          case (ids, SArray(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }

        optArr must beSome
        val arr = optArr.get

        arr must haveSize(1)
        arr.head must_== SNull
      }
    }

    "evaluate join_object on single values" in {
      val line = Line(1, 1, "")

      val input = Join(JoinObject, Cross(None),
        Join(WrapObject, Cross(None),
          Const(CString("question"))(line),
          Const(CString("What is six times seven?"))(line))(line),
        Join(WrapObject, Cross(None),
          Const(CString("answer"))(line),
          Const(CLong(42))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val optObj = result find {
          case (ids, SObject(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SObject(obj)) => obj
        }

        optObj must beSome
        val obj = optObj.get

        obj must haveKey("answer")
        obj("answer") must beLike {
          case SDecimal(d) => d must_== 42
        }

        obj must haveKey("question")
        obj("question") must beLike {
          case SString(str) => str must_== "What is six times seven?"
        }
      }
    }

    "evaluate join_array on single values" in {
      val line = Line(1, 1, "")

      val input = Join(JoinArray, Cross(None),
        Operate(WrapArray,
          Const(CLong(24))(line))(line),
        Operate(WrapArray,
          Const(CLong(42))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val optArr = result find {
          case (ids, SArray(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }

        optArr must beSome
        val arr = optArr.get

        arr must beLike {
          case Vector(SDecimal(d1), SDecimal(d2)) => {
            d1 must_== 24
            d2 must_== 42
          }
        }
      }
    }

    "create an array" >> {
      val line = Line(1, 1, "")

      val input =
        Join(JoinArray, Cross(None),
          Operate(WrapArray,
            Const(CLong(12))(line))(line),
          Join(JoinArray, Cross(None),
            Operate(WrapArray,
              Const(CLong(24))(line))(line),
            Operate(WrapArray,
              Const(CLong(42))(line))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)

        val optArr = result find {
          case (ids, SArray(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }

        optArr must beSome
        val arr = optArr.get

        arr must beLike {
          case Vector(SDecimal(d1), SDecimal(d2), SDecimal(d3)) => {
            d1 must_== 12
            d2 must_== 24
            d3 must_== 42
          }
        }
      }
    }

    "evaluate array_swap on single values" >> {
      "at start" >> {
        val line = Line(1, 1, "")

        val input = Join(ArraySwap, Cross(None),
          Join(JoinArray, Cross(None),
            Operate(WrapArray,
              Const(CLong(12))(line))(line),
            Join(JoinArray, Cross(None),
              Operate(WrapArray,
                Const(CLong(24))(line))(line),
              Operate(WrapArray,
                Const(CLong(42))(line))(line))(line))(line),
          Const(CLong(1))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val optArr = result find {
            case (ids, SArray(_)) if ids.isEmpty => true
            case _ => false
          } collect {
            case (_, SArray(arr)) => arr
          }

          optArr must beSome
          val arr = optArr.get

          arr must beLike {
            case Vector(SDecimal(d1), SDecimal(d2), SDecimal(d3)) => {
              d1 must_== 24
              d2 must_== 12
              d3 must_== 42
            }
          }
        }
      }

      "at end" >> {
        val line = Line(1, 1, "")

        val input = Join(ArraySwap, Cross(None),
          Join(JoinArray, Cross(None),
            Operate(WrapArray,
              Const(CLong(12))(line))(line),
            Join(JoinArray, Cross(None),
              Operate(WrapArray,
                Const(CLong(24))(line))(line),
              Operate(WrapArray,
                Const(CLong(42))(line))(line))(line))(line),
          Const(CLong(2))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val optArr = result find {
            case (ids, SArray(_)) if ids.isEmpty => true
            case _ => false
          } collect {
            case (_, SArray(arr)) => arr
          }

          optArr must beSome
          val arr = optArr.get

          arr must beLike {
            case Vector(SDecimal(d1), SDecimal(d2), SDecimal(d3)) => {
              d1 must_== 42
              d2 must_== 24
              d3 must_== 12
            }
          }
        }
      }
    }

    "evaluate descent on a homogeneous set" in {
      val line = Line(1, 1, "")

      val input = Join(DerefObject, Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/pairs"))(line))(line),
        Const(CString("first"))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "evaluate descent on a heterogeneous set" in {
      val line = Line(1, 1, "")

      val input = Join(DerefObject, Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/pairs"))(line))(line),
        Const(CString("first"))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          case (ids, SNull) if ids.size == 1 => SNull
        }

        result2.toSet must_== Set(42, 12, 1, 13, SNull)
      }
    }

    "evaluate descent producing a heterogeneous set" in {
      val line = Line(1, 1, "")

      val input = Join(DerefObject, Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/het-pairs"))(line))(line),
        Const(CString("first"))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          case (ids, SString(str)) if ids.size == 1 => str
          case (ids, SBoolean(b)) if ids.size == 1 => b
          case (ids, SNull) if ids.size == 1 => SNull
        }

        result2.toSet must_== Set(42, true, "daniel", 1, SNull)
      }
    }

    "an array must return an array" in {
      val line = Line(1, 1, "")

      val input = dag.AbsoluteLoad(Const(CString("/hom/arrays"))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SArray(arr)) if ids.size == 1 => arr
        }

        result2 must contain(Vector(SDecimal(-9), SDecimal(-42), SDecimal(42), SDecimal(87), SDecimal(4)))
      }
    }

    "MegaReduce must return an array" in {
      val line = Line(1, 1, "")

      val parent = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)
      val input = dag.MegaReduce(List((trans.Leaf(trans.Source), List(Count, Sum))), parent)

      // We don't optimize since MegaReduce can only be created through an optimization.
      testEval(input, optimize = false) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SArray(arr)) if ids.size == 0 => arr
        }

        result2.toSet must_== Set(Vector(SArray(Vector(SDecimal(145), SDecimal(5)))))
      }
    }

    "evaluate array dereference on a MegaReduce" in {
      val line = Line(1, 1, "")

      val parent = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)
      val red = Sum

      val mega = dag.MegaReduce(List((trans.Leaf(trans.Source), List(red))), parent)
      val input = Join(DerefArray, Cross(None), Join(DerefArray, Cross(None), mega, Const(CLong(0))(line))(line), Const(CLong(0))(line))(line)

      // We don't optimize since MegaReduce can only be created through an optimization.
      testEval(input, optimize = false) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d.toInt
        }

        result2.toSet must_== Set(145)
      }
    }

    "evaluate array dereference on a homogeneous set" in {
      val line = Line(1, 1, "")

      val input = Join(DerefArray, Cross(None),
        dag.AbsoluteLoad(Const(CString("/hom/arrays"))(line))(line),
        Const(CLong(2))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "evaluate array dereference on a heterogeneous set" in {
      val line = Line(1, 1, "")

      val input = Join(DerefArray, Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/arrays"))(line))(line),
        Const(CLong(2))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "evaluate array dereference producing a heterogeneous set" in {
      val line = Line(1, 1, "")

      val input = Join(DerefArray, Cross(None),
        dag.AbsoluteLoad(Const(CString("/het/het-arrays"))(line))(line),
        Const(CLong(2))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          case (ids, SString(str)) if ids.size == 1 => str
          case (ids, SBoolean(b)) if ids.size == 1 => b
          case (ids, SNull) if ids.size == 1 => SNull
        }

        result2.toSet must_== Set(42, true, "daniel", 1, SNull)
      }
    }

    "evaluate matched binary numeric operation" in {
      val line = Line(1, 1, "")
      val pairs = dag.AbsoluteLoad(Const(CString("/hom/pairs"))(line))(line)

      val input = Join(Sub, IdentitySort,
        Join(DerefObject, Cross(None),
          pairs,
          Const(CString("first"))(line))(line),
        Join(DerefObject, Cross(None),
          pairs,
          Const(CString("second"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(36, 12, 115, -165)
      }
    }

    "evaluate matched binary numeric operation dropping undefined result" in {
      val line = Line(1, 1, "")
      val pairs = dag.AbsoluteLoad(Const(CString("/hom/pairs"))(line))(line)

      val input = Join(Div, IdentitySort,
        Join(DerefObject, Cross(None),
          pairs,
          Const(CString("first"))(line))(line),
        Join(DerefObject, Cross(None),
          pairs,
          Const(CString("second"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(4)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }

        result2.toSet must_== Set(7, -2.026315789473684, 0.006024096385542169, 13)
      }
    }

    "evaluate matched binary numeric operation dropping undefined result (with relative load)" in {
      val line = Line(1, 1, "")
      val pairs = dag.RelativeLoad(Const(CString("pairs"))(line))(line)

      val input = Join(Div, IdentitySort,
        Join(DerefObject, Cross(None),
          pairs,
          Const(CString("first"))(line))(line),
        Join(DerefObject, Cross(None),
          pairs,
          Const(CString("second"))(line))(line))(line)

      testEval(input, Path.Root, Path("/hom")) { result =>
        result must haveSize(4)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }

        result2.toSet must_== Set(7, -2.026315789473684, 0.006024096385542169, 13)
      }
    }

    "produce a result from the body of a passed assertion" in {
      val line = Line(1, 1, "")

      val input = dag.Assert(
        Const(CTrue)(line),
        dag.AbsoluteLoad(Const(CString("clicks"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(100)
      }
    }

    "throw an exception from a failed assertion" in {
      val line = Line(1, 1, "")

      val input = dag.Assert(
        Const(CFalse)(line),
        dag.AbsoluteLoad(Const(CString("clicks"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(100)
      } must throwA[FatalQueryException]
    }

    "fail an assertion according to forall semantics" in {
      val line = Line(1, 1, "")

      val input = dag.Assert(
        dag.IUI(
          true,
          Const(CFalse)(line),
          Const(CTrue)(line))(line),
        dag.AbsoluteLoad(Const(CString("clicks"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(100)
      } must throwA[FatalQueryException]
    }

    "compute the set difference of two sets" in {
      val line = Line(1, 1, "")
      val clicks2 = dag.AbsoluteLoad(Const(CString("/clicks2"))(line))(line)

      val input = Diff(
        clicks2,
        Filter(IdentitySort,
          clicks2,
          Join(Gt, Cross(None),
            Join(DerefObject, Cross(None),
              clicks2,
              Const(CString("time"))(line))(line),
            Const(CLong(0))(line))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(6)

        result must haveAllElementsLike {
          case (ids, SObject(obj)) =>
            ids must haveSize(1)
            obj must not haveKey("time")

          case (ids, SString(s)) =>
            ids must haveSize(1)
            s must_== "string cheese"
        }
      }
    }

    "compute the set difference of the set difference" in {
      val line = Line(1, 1, "")
      val clicks2 = dag.AbsoluteLoad(Const(CString("/clicks2"))(line))(line)

      val input = Diff(
        clicks2,
        Diff(
          clicks2,
          Filter(IdentitySort,
            clicks2,
            Join(Gt, Cross(None),
              Join(DerefObject, Cross(None),
                clicks2,
                Const(CString("time"))(line))(line),
              Const(CLong(0))(line))(line))(line))(line))(line)


      testEval(input) { result =>
        result must haveSize(101)

        result must haveAllElementsLike {
          case (ids, SObject(obj)) =>
            ids must haveSize(1)
            obj must haveKey("time")
        }
      }
    }

    "compute the iunion of a set with itself" in {
      val line = Line(1, 1, "")

      val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

      val input = IUI(true, numbers, numbers)(line)

      testEval(input) { result =>
        result must haveSize(5)
      }
    }

    "compute the iunion of two homogeneous sets" in {
      val line = Line(1, 1, "")

      val input = IUI(true,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
        dag.AbsoluteLoad(Const(CString("/hom/numbers3"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(10)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }
        result2.toSet must_== Set(42, 12, 77, 1, 13, 14, -1, 0)

        val result3 = result collect {
          case (ids, _) => ids
        }
        result3 must haveSize(10)
      }
    }

    "compute the iunion of two homogeneous sets (with relative load)" in {
      val line = Line(1, 1, "")

      val input = IUI(true,
        dag.RelativeLoad(Const(CString("numbers"))(line))(line),
        dag.RelativeLoad(Const(CString("numbers3"))(line))(line))(line)

      testEval(input, Path.Root, Path("/hom")) { result =>
        result must haveSize(10)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13, 14, -1, 0)
      }
    }

    "compute the iunion of two datasets, one with objects" in {
      val line = Line(1, 1, "")

      val input = IUI(true,
        dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
        dag.AbsoluteLoad(Const(CString("/hom/numbers3"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(105)
      }
    }

    "compute the iintersect of two nonintersecting sets of numbers" in {
      val line = Line(1, 1, "")

      val input = IUI(false,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
        dag.AbsoluteLoad(Const(CString("/hom/numbers3"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(0)
      }
    }

    "compute the iintersect of two nonintersecting datasets" in {
      val line = Line(1, 1, "")

      val input = IUI(false,
        dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
        dag.AbsoluteLoad(Const(CString("/hom/numbers3"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(0)
      }
    }

    "compute the iintersect of mod2 and mod3" in {
      val line = Line(1, 1, "")
      val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbersmod"))(line))(line)

      val input = IUI(false,
        Filter(IdentitySort,
          numbers,
          Join(Eq, Cross(None),
            Join(Mod, Cross(None),
              numbers,
              Const(CLong(2))(line))(line),
            Const(CLong(0))(line))(line))(line),
        Filter(IdentitySort,
          numbers,
          Join(Eq, Cross(None),
            Join(Mod, Cross(None),
              numbers,
              Const(CLong(3))(line))(line),
            Const(CLong(0))(line))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(3)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(6, 12, 24)
      }
    }

    "filter homogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Lt, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(2)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(1, 12)
        }
      }

      "less-than (with relative load)" >> {
        val line = Line(1, 1, "")
        val numbers = dag.RelativeLoad(Const(CString("numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Lt, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input, Path.Root, Path("/hom")) { result =>
          result must haveSize(2)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(1, 12)
        }
      }

      "less-than-equal" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(LtEq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(12, 1, 13)
        }
      }

      "greater-than" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Gt, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(2)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(42, 77)
        }
      }

      "greater-than-equal" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(GtEq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(42, 77, 13)
        }
      }

      "equal with a number literal" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Eq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(13)
        }
      }

      "equal without a filter" >> {
        val line = Line(1, 1, "")

        val input = Join(Eq, Cross(None),
            dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line),
            Const(CLong(13))(line))(line)

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SBoolean(d)) if ids.size == 1 => d
          }

          result2.toSet must_== Set(true, false)
        }
      }

      "not-equal" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(NotEq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(42, 12, 77, 1)
        }
      }

      "and" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(And, IdentitySort,
            Join(NotEq, Cross(None),
              numbers,
              Const(CLong(77))(line))(line),
            Join(NotEq, Cross(None),
              numbers,
              Const(CLong(13))(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(42, 12, 1)
        }
      }

      "and (with relative load)" >> {
        val line = Line(1, 1, "")
        val numbers = dag.RelativeLoad(Const(CString("numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(And, IdentitySort,
            Join(NotEq, Cross(None),
              numbers,
              Const(CLong(77))(line))(line),
            Join(NotEq, Cross(None),
              numbers,
              Const(CLong(13))(line))(line))(line))(line)

        testEval(input, Path.Root, Path("/hom")) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(42, 12, 1)
        }
      }

      "or" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Or, IdentitySort,
            Join(Eq, Cross(None),
              numbers,
              Const(CLong(77))(line))(line),
            Join(Eq, Cross(None),
              numbers,
              Const(CLong(13))(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(2)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(77, 13)
        }
      }

      "complement of equality" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Operate(Comp,
            Join(Eq, Cross(None),
              numbers,
              Const(CLong(13))(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(4)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(42, 12, 77, 1)
        }
      }
    }

    "filter heterogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Lt, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(2)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(1, 12)
        }
      }

      "less-than-equal" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(LtEq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(12, 1, 13)
        }
      }

      "less-than-equal (with relative load)" >> {
        val line = Line(1, 1, "")
        val numbers = dag.RelativeLoad(Const(CString("numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(LtEq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input, Path.Root, Path("/het")) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(12, 1, 13)
        }
      }

      "greater-than" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Gt, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(2)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(42, 77)
        }
      }

      "greater-than-equal" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(GtEq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(42, 77, 13)
        }
      }

      "equal with boolean set as the source" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          Join(Eq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line),
          Join(Eq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SBoolean(d)) if ids.size == 1 => d
          }

          result2.toSet must_== Set(true)
        }
      }

      "equal" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Eq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(13)
        }
      }

      "equal with empty array" >> {
        val line = Line(1, 1, "")
        val numbers9 = dag.AbsoluteLoad(Const(CString("/het/numbers9"))(line))(line)

        val input = Filter(IdentitySort,
          numbers9,
          Join(Eq, Cross(None),
            numbers9,
            Const(RArray.empty)(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }

          result2.toList must_== List(Vector())
        }
      }

      "equal with empty object" >> {
        val line = Line(1, 1, "")
        val numbers9 = dag.AbsoluteLoad(Const(CString("/het/numbers9"))(line))(line)

        val input = Filter(IdentitySort,
          numbers9,
          Join(Eq, Cross(None),
            numbers9,
            Const(RObject.empty)(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }

          result2.toSet must_== Set(Map())
        }
      }

      "equal with an array" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Eq, Cross(None),
            numbers,
            Join(JoinArray, Cross(None),
              Join(JoinArray, Cross(None),
                Operate(WrapArray, Const(CLong(9))(line))(line),
                Operate(WrapArray, Const(CLong(10))(line))(line))(line),
              Operate(WrapArray, Const(CLong(11))(line))(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }

          result2.toSet must_== Set(Vector(SDecimal(9), SDecimal(10), SDecimal(11)))
        }
      }

      "equal with a singleton array" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/array"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Eq, Cross(None),
            numbers,
            Join(JoinArray, Cross(None),
              Join(JoinArray, Cross(None),
                Operate(WrapArray, Const(CLong(9))(line))(line),
                Operate(WrapArray, Const(CLong(10))(line))(line))(line),
              Operate(WrapArray, Const(CLong(11))(line))(line))(line))(line))(line)


        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }

          result2.toSet must_== Set(Vector(SDecimal(9), SDecimal(10), SDecimal(11)))
        }
      }

      "equal with an object" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Eq, Cross(None),
            numbers,
              Join(WrapObject, Cross(None),
                Const(CString("foo"))(line),
                Const(CString("bar"))(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(1)

          result must haveAllElementsLike {
            case (ids, SObject(obj)) =>
              ids must haveSize(1)
              obj must haveKey("foo")
              obj must haveValue(SString("bar"))
          }
        }
      }

      "equal without a filter" >> {
        val line = Line(1, 1, "")

        val input = Join(Eq, Cross(None),
            dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line),
            Const(CLong(13))(line))(line)

        testEval(input) { result =>
          result must haveSize(10)

          val result2 = result.groupBy {
            case (ids, SBoolean(d)) if ids.size == 1 => Some(d)
            case _                                   => None
          }

          result2.keySet must contain(Some(true), Some(false))
          result2(Some(true)).size must_== 1
          result2(Some(false)).size must_== 9
        }
      }

      "not equal without a filter" >> {
        val line = Line(1, 1, "")

        val input = Join(NotEq, Cross(None),
            dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line),
            Const(CLong(13))(line))(line)

        testEval(input) { result =>
          result must haveSize(10)

          val result2 = result.groupBy {
            case (ids, SBoolean(d)) if ids.size == 1 => Some(d)
            case _                                   => None
          }

          result2.keySet must contain(Some(true), Some(false))
          result2(Some(true)).size must_== 9
          result2(Some(false)).size must_== 1
        }
      }

      "not-equal" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(NotEq, Cross(None),
            numbers,
            Const(CLong(13))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(9)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
            case (ids, SBoolean(b)) if ids.size == 1 => b
            case (ids, SString(str)) if ids.size == 1 => str
            case (ids, SObject(obj)) if ids.size == 1 => obj
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }

          result2.toSet must_== Set(42, 12, 77, 1, true, false, "daniel",
            Map("test" -> SString("fubar")), Vector())
        }
      }

      "not-equal with empty array" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers10"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(NotEq, Cross(None),
            numbers,
            Const(RArray.empty)(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }

          result2.toSet must_== Set(Vector(SDecimal(9), SDecimal(10)), Map.empty[String, SValue], Map("foo" -> SNull))
        }
      }

      "not-equal with empty object" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers10"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(NotEq, Cross(None),
            numbers,
            Const(RObject.empty)(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }

          result2.toSet must_== Set(Vector.empty[SValue], Vector(SDecimal(9), SDecimal(10)), Map("foo" -> SNull))
        }
      }

      "not-equal with an array" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers10"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(NotEq, Cross(None),
            numbers,
            Join(JoinArray, Cross(None),
              Operate(WrapArray, Const(CLong(9))(line))(line),
              Operate(WrapArray, Const(CLong(10))(line))(line))(line))(line))(line)


        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }

          result2.toSet must_== Set(Map.empty[String, SValue], Vector.empty[SValue], Map("foo" -> SNull))

        }
      }

      "not-equal with an object" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers10"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(NotEq, Cross(None),
            numbers,
            Join(WrapObject, Cross(None),
              Const(CString("foo"))(line),
              Const(CNull)(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }

          result2.toSet must_== Set(Vector.empty[SValue], Vector(SDecimal(9), SDecimal(10)), Map.empty[String, SValue])

        }
      }

      "and" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(And, IdentitySort,
            Join(NotEq, Cross(None),
              numbers,
              Const(CLong(77))(line))(line),
            Join(NotEq, Cross(None),
              numbers,
              Const(CLong(13))(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(8)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
            case (ids, SBoolean(b)) if ids.size == 1 => b
            case (ids, SString(str)) if ids.size == 1 => str
            case (ids, SObject(obj)) if ids.size == 1 => obj
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }

          result2.toSet must_== Set(42, 12, 1, true, false, "daniel",
            Map("test" -> SString("fubar")), Vector())
        }
      }

      "or" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Join(Or, IdentitySort,
            Join(Eq, Cross(None),
              numbers,
              Const(CLong(77))(line))(line),
            Join(Eq, Cross(None),
              numbers,
              Const(CLong(13))(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(2)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }

          result2.toSet must_== Set(77, 13)
        }
      }

      "complement of equality" >> {
        val line = Line(1, 1, "")
        val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

        val input = Filter(IdentitySort,
          numbers,
          Operate(Comp,
            Join(Eq, Cross(None),
              numbers,
              Const(CLong(13))(line))(line))(line))(line)

        testEval(input) { result =>
          result must haveSize(9)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1  => d.toInt
            case (ids, SBoolean(b)) if ids.size == 1  => b
            case (ids, SString(str)) if ids.size == 1 => str
            case (ids, SObject(obj)) if ids.size == 1 => obj
            case (ids, SArray(arr)) if ids.size == 1  => arr
          }

          result2.toSet must_== Set(42, 12, 77, 1, true, false, "daniel", Map("test" -> SString("fubar")), Vector())
        }
      }
    }

    "correctly order a match following a cross" in {
      val line = Line(1, 1, "")

      val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

      val input = Join(Mul, IdentitySort,
        numbers,
        Join(Sub, Cross(None),
          numbers,
          dag.AbsoluteLoad(Const(CString("/hom/numbers3"))(line))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(25)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 2 => d.toInt
        }

        result2 must haveSize(23)

        result2.toSet must_== Set(0, -377, -780, 6006, -76, 5929, 1, 156, 169, 2, 1764, 2695, 144, 1806, -360, 1176, -832, 182, 4851, -1470, -13, -41, -24)
      }
    }

    "correctly evaluate a match following a cross with equality" in {
      val line = Line(1, 1, "")

      val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)
      val numbers3 = dag.AbsoluteLoad(Const(CString("/hom/numbers3"))(line))(line)

      val input = Join(And, IdentitySort,
        Join(And, Cross(None),
          Join(Eq, IdentitySort, numbers, numbers)(line),
          Join(Eq, IdentitySort, numbers3, numbers3)(line))(line),
        Join(Eq, IdentitySort, numbers3, numbers3)(line))(line)

      testEval(input) { _ must not(beEmpty) }
    }

    "correctly order a match following a cross within a new" in {
      val line = Line(1, 1, "")
      val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

      val input = Join(Mul, IdentitySort,
        numbers,
        Join(Sub, Cross(None),
          numbers,
          dag.New(numbers)(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(25)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 2 => d.toInt
        }

        result2 must haveSize(20)

        result2.toSet must_== Set(0, 1260, -1470, 1722, 1218, -360, -780, 132, -12,
          2695, 5005, 5852, 4928, -41, -11, -76, -377, 13, -832, 156)
      }
    }

    "split on a homogeneous set" in {
      val line = Line(1, 1, "")

      //
      // nums := dataset(//hom/numbers)
      // solve 'n
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      //

      val nums = dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(Add, Cross(None),
          SplitGroup(1, nums.identities, id)(line),
          dag.Reduce(Max,
            Filter(IdentitySort,
              nums,
              Join(Lt, Cross(None),
                nums,
                SplitParam(0, id)(line))(line))(line))(line))(line), id)(line)

      testEval(input) { result =>
        result must haveSize(4)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(55, 13, 119, 25)
      }
    }

    "split where commonalities are determined through object deref" in {
      // clicks := //clicks
      //
      // solve 'userId
      //   clicks.time where clicks.userId = 'userId

      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1,
          Join(DerefObject, Cross(None), clicks, Const(CString("time"))(line))(line),
          UnfixedSolution(0, Join(DerefObject, Cross(None), clicks, Const(CString("user"))(line))(line))),
        SplitGroup(1, clicks.identities, id)(line), id)(line)

      testEval(input) { result =>
        result must haveSize(100)
      }
    }

    "split where commonalities are determined through object deref across extras" in {
        // clicks := //clicks
        //
        // solve 'time
        //   count(clicks.page where clicks.page = "/sign-up.html" & clicks.time = 'time)

      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1,
          Join(DerefObject, Cross(None), clicks, Const(CString("page"))(line))(line),
          IntersectBucketSpec(
            dag.Extra(
              Join(Eq, Cross(None),
                Join(DerefObject, Cross(None), clicks, Const(CString("page"))(line))(line),
                Const(CString("/sign-up.html"))(line))(line)),
            UnfixedSolution(0,
              Join(DerefObject, Cross(None), clicks, Const(CString("time"))(line))(line)))),
        dag.Reduce(Count, SplitGroup(1, clicks.identities, id)(line))(line), id)(line)

      testEval(input) { results =>
        results must not(beEmpty)
      }
    }

    "split where the commonality is an object concat" in {
      /*
       * clicks := //clicks
       * data := { user: clicks.user, page: clicks.page }
       *
       * solve 'bins = data
       *   'bins
       */

      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val data = Join(JoinObject, IdentitySort,
        Join(DerefObject, Cross(None),
          clicks,
          Const(CString("user"))(line))(line),
        Join(DerefObject, Cross(None),
          clicks,
          Const(CString("page"))(line))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1, data, UnfixedSolution(0, data)),
        SplitParam(0, id)(line), id)(line)

      // XXX ???
      ok
    }

    "split where the commonality is a union" in {
      // clicks := //clicks
      // data := clicks union clicks
      //
      // solve 'page
      //   data where data.page = 'page

      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)
      val data = dag.IUI(true, clicks, clicks)(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1, data, UnfixedSolution(0, Join(DerefObject, Cross(None), data, Const(CString("page"))(line))(line))),
        SplitGroup(1, data.identities, id)(line), id)(line)

      testEval(input) { results =>
        results must not(beEmpty)
      }
    }

    "memoize properly in a load" in {
      val line = Line(1, 1, "")

      val input0 = dag.Memoize(dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line), 1)
      val input1 = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      testEval(input0) { result0 => {
        testEval(input1) { result1 =>
          result0.map({case (ids, v) => (ids.toSeq, v)}) must_== result1.map({case (ids, v) => (ids.toSeq, v)})
        }
      }}
    }

    "memoize properly in an add" in {
      val line = Line(1, 1, "")

      val input0 = dag.Memoize(
        dag.Join(Add, Cross(None),
          dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
          Const(CLong(5))(line))(line),
        1)

      val input1 = dag.Join(Add, Cross(None),
          dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
          Const(CLong(5))(line))(line)

      testEval(input0) { result0 => {
        testEval(input1) { result1 =>
          result0.map({case (ids, v) => (ids.toSeq, v)}) must_== result1.map({case (ids, v) => (ids.toSeq, v)})
        }
      }}
    }

    "evaluate a histogram function" in {
      val Expected = Map("daniel" -> 9, "kris" -> 8, "derek" -> 7, "nick" -> 17,
        "john" -> 13, "alissa" -> 7, "franco" -> 13, "matthew" -> 10, "jason" -> 13, SNull -> 3)

      val line = Line(1, 1, "")

      //
      // clicks := dataset(//clicks)
      // histogram('user) :=
      //   { user: 'user, num: count(clicks where clicks.user = 'user) }
      // histogram
      //
      //
      val clicks = dag.AbsoluteLoad(dag.Morph1(expandGlob, Const(CString("/clicks"))(line))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1,
          clicks,
          UnfixedSolution(0,
            Join(DerefObject, Cross(None),
              clicks,
              Const(CString("user"))(line))(line))),
        Join(JoinObject, Cross(None),
          Join(WrapObject, Cross(None),
            Const(CString("user"))(line),
            SplitParam(0, id)(line))(line),
          Join(WrapObject, Cross(None),
            Const(CString("num"))(line),
            dag.Reduce(Count,
              SplitGroup(1, clicks.identities, id)(line))(line))(line))(line), id)(line)

      testEval(input) { result =>
        result must haveSize(10)

        result must haveAllElementsLike {
          case (ids, SObject(obj)) =>
            ids must haveSize(1)
            obj must haveKey("user")
            obj must haveKey("num")

            obj("user") must beLike {
              case SString(str) => {
                str must beOneOf("daniel", "kris", "derek", "nick", "john",
                  "alissa", "franco", "matthew", "jason")
              }
              case SNull => ok
            }

            val user = (obj("user"): @unchecked) match {
              case SString(user) => user
              case SNull => SNull
            }

            obj("num") must beLike {
              case SDecimal(d) => d must_== Expected(user)
            }
        }
      }
    }

    "evaluate a histogram function against relative path" in {
      val Expected = Map("daniel" -> 9, "kris" -> 8, "derek" -> 7, "nick" -> 17,
        "john" -> 13, "alissa" -> 7, "franco" -> 13, "matthew" -> 10, "jason" -> 13, SNull -> 3)

      val line = Line(1, 1, "")

      //
      // clicks := dataset(//clicks)
      // histogram('user) :=
      //   { user: 'user, num: count(clicks where clicks.user = 'user) }
      // histogram
      //
      //
      val clicks = dag.RelativeLoad(dag.Morph1(expandGlob, Const(CString("clicks"))(line))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1,
          clicks,
          UnfixedSolution(0,
            Join(DerefObject, Cross(None),
              clicks,
              Const(CString("user"))(line))(line))),
        Join(JoinObject, Cross(None),
          Join(WrapObject, Cross(None),
            Const(CString("user"))(line),
            SplitParam(0, id)(line))(line),
          Join(WrapObject, Cross(None),
            Const(CString("num"))(line),
            dag.Reduce(Count,
              SplitGroup(1, clicks.identities, id)(line))(line))(line))(line), id)(line)

      testEval(input) { result =>
        result must haveSize(10)

        result must haveAllElementsLike {
          case (ids, SObject(obj)) =>
            ids must haveSize(1)
            obj must haveKey("user")
            obj must haveKey("num")

            obj("user") must beLike {
              case SString(str) => {
                str must beOneOf("daniel", "kris", "derek", "nick", "john",
                  "alissa", "franco", "matthew", "jason")
              }
              case SNull => ok
            }

            val user = (obj("user"): @unchecked) match {
              case SString(user) => user
              case SNull => SNull
            }

            obj("num") must beLike {
              case SDecimal(d) => d must_== Expected(user)
            }
        }
      }
    }

    "evaluate with on the clicks dataset" in {
      val line = Line(1, 1, "")

      val input = Join(JoinObject, Cross(None),
        dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line),
        Join(WrapObject, Cross(None),
          Const(CString("t"))(line),
          Const(CLong(42))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(100)
        result must haveAllElementsLike {
          case (ids, SObject(obj)) =>
            ids must haveSize(1)
            obj must haveKey("user")
            obj must haveKey("time")
            obj must haveKey("page")
            obj must haveKey("t")

            obj("t") must_== SDecimal(42)

          case _ => fail("Result has wrong shape")
        }
      }
    }

    "evaluate `with` with inner join semantics" in {
      /*
       * clicks := //clicks
       * a := {dummy: if clicks.time < 1000 then 1 else 0}
       * clicks with {a:a}
       */

      val line = Line(1, 1, "")

      val clicks =
        Join(WrapObject, Cross(None),
          Const(CString("time"))(line),
          Const(CLong(42))(line))(line)

      val predicate = Join(Lt, Cross(None),
        Join(DerefObject, Cross(None),
          clicks,
          Const(CString("time"))(line))(line),
        Const(CLong(1000))(line))(line)

      val a = dag.IUI(true,
        dag.Filter(Cross(None),
          Const(CLong(1))(line),
          predicate)(line),
        dag.Filter(Cross(None),
          Const(CLong(0))(line),
          Operate(Comp, predicate)(line))(line))(line)

      val input = Join(JoinObject, Cross(None),    // TODO Cross(None) breaks even more creatively!
        clicks,
        Join(WrapObject, Cross(None),
          Const(CString("a"))(line),
          a)(line))(line)

      testEval(input) { result =>
        result must haveAllElementsLike {
          case (ids, SObject(fields)) => fields must haveKey("a")
        }
      }
    }

    "evaluate filter with null" in {
      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      //
      // //clicks where //clicks.user = null
      //
      //
      val input = Filter(IdentitySort,
        clicks,
        Join(Eq, Cross(None),
          Join(DerefObject, Cross(None),
            clicks,
            Const(CString("user"))(line))(line),
          Const(CNull)(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(3)

        result must haveAllElementsLike {
          case (ids, SObject(obj)) =>
            ids must haveSize(1)
            obj must haveKey("user")
            obj("user") must_== SNull
        }
      }
    }

    "evaluate filter with non-boolean where clause (with empty result)" in {
      val line = Line(1, 1, "")

      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val input = Filter(IdentitySort,
        clicks,
        clicks)(line)

      testEval(input) { result =>
        result must haveSize(0)
      }
    }

    "evaluate filter on the results of a histogram function" in {
      val line = Line(1, 1, "")

      //
      // clicks := //clicks
      // histogram('user) :=
      //   { user: 'user, num: count(clicks where clicks.user = 'user) }
      // histogram where histogram.num = 9
      //
      //
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      val histogram = dag.Split(
        dag.Group(1,
          clicks,
          UnfixedSolution(0,
            Join(DerefObject, Cross(None),
              clicks,
              Const(CString("user"))(line))(line))),
        Join(JoinObject, Cross(None),
          Join(WrapObject, Cross(None),
            Const(CString("user"))(line),
            SplitParam(0, id)(line))(line),
          Join(WrapObject, Cross(None),
            Const(CString("num"))(line),
            dag.Reduce(Count,
              SplitGroup(1, clicks.identities, id)(line))(line))(line))(line), id)(line)

      val input = Filter(IdentitySort,
        histogram,
        Join(Eq, Cross(None),
          Join(DerefObject, Cross(None),
            histogram,
            Const(CString("num"))(line))(line),
          Const(CLong(9))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(1)
        result.toList.head must beLike {
          case (ids, SObject(obj)) if ids.size == 1 => {
            obj must haveKey("user")
            obj("user") must beLike { case SString("daniel") => ok }

            obj must haveKey("num")
            obj("num") must beLike { case SDecimal(d) => d must_== 9 }
          }
        }
      }
    }

    "evaluate with on the results of a histogram function" in {
      val line = Line(1, 1, "")
      //
      // clicks := //clicks
      // histogram('user) :=
      //   { user: 'user, num: count(clicks where clicks.user = 'user) }
      // histogram with {rank: std::stats::rank(histogram.num)}
      //

      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      val histogram = dag.Split(
        dag.Group(1,
          clicks,
          UnfixedSolution(0,
            Join(DerefObject, Cross(None),
              clicks,
              Const(CString("user"))(line))(line))),
        Join(JoinObject, Cross(None),
          Join(WrapObject, Cross(None),
            Const(CString("user"))(line),
            SplitParam(0, id)(line))(line),
          Join(WrapObject, Cross(None),
            Const(CString("num"))(line),
            dag.Reduce(Count,
              SplitGroup(1, clicks.identities, id)(line))(line))(line))(line), id)(line)

      val input = Join(JoinObject, IdentitySort,
        histogram,
        Join(WrapObject, Cross(None),
          Const(CString("rank"))(line),
          dag.Morph1(Rank,
            Join(DerefObject, Cross(None),
              histogram,
              Const(CString("num"))(line))(line))(line))(line))(line)

      testEval(input) { resultsE =>
        resultsE must haveSize(10)

        val results = resultsE collect {
          case (ids, sv) if ids.length == 1 => sv
        }

        results must contain(SObject(Map("user" -> SString("daniel"), "num" -> SDecimal(BigDecimal("9")), "rank" -> SDecimal(BigDecimal("4")))))
        results must contain(SObject(Map("user" -> SString("kris"), "num" -> SDecimal(BigDecimal("8")), "rank" -> SDecimal(BigDecimal("3")))))
        results must contain(SObject(Map("user" -> SString("derek"), "num" -> SDecimal(BigDecimal("7")), "rank" -> SDecimal(BigDecimal("1")))))
        results must contain(SObject(Map("user" -> SString("nick"), "num" -> SDecimal(BigDecimal("17")), "rank" -> SDecimal(BigDecimal("9")))))
        results must contain(SObject(Map("user" -> SString("john"), "num" -> SDecimal(BigDecimal("13")), "rank" -> SDecimal(BigDecimal("6")))))
        results must contain(SObject(Map("user" -> SString("alissa"), "num" -> SDecimal(BigDecimal("7")), "rank" -> SDecimal(BigDecimal("1")))))
        results must contain(SObject(Map("user" -> SString("franco"), "num" -> SDecimal(BigDecimal("13")), "rank" -> SDecimal(BigDecimal("6")))))
        results must contain(SObject(Map("user" -> SString("matthew"), "num" -> SDecimal(BigDecimal("10")), "rank" -> SDecimal(BigDecimal("5")))))
        results must contain(SObject(Map("user" -> SString("jason"), "num" -> SDecimal(BigDecimal("13")), "rank" -> SDecimal(BigDecimal("6")))))
        results must contain(SObject(Map("user" -> SNull, "num" -> SDecimal(BigDecimal("3")), "rank" -> SDecimal(BigDecimal("0")))))
      }
    }

    "perform a naive cartesian product on the clicks dataset" in {
      val line = Line(1, 1, "")
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val input = Join(JoinObject, Cross(None),
        Join(WrapObject, Cross(None),
          Const(CString("aa"))(line),
          Join(DerefObject, Cross(None),
            clicks,
            Const(CString("user"))(line))(line))(line),
        Join(WrapObject, Cross(None),
          Const(CString("bb"))(line),
          Join(DerefObject, Cross(None),
            dag.New(clicks)(line),
            Const(CString("user"))(line))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(10000)

        result must haveAllElementsLike {
          case (ids, SObject(obj)) =>
            ids.size must_==(2)
            obj must haveSize(2)
            obj must haveKey("aa")
            obj must haveKey("bb")
        }
      }
    }

    "distinct homogenous set of numbers" in {
      val line = Line(1, 1, "")

      val input = dag.Distinct(
        dag.AbsoluteLoad(Const(CString("/hom/numbers2"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13)
      }
    }

    "distinct heterogenous sets" in {
      val line = Line(1, 1, "")

      val input = dag.Distinct(
        dag.AbsoluteLoad(Const(CString("/het/numbers2"))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(10)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          case (ids, SBoolean(b)) if ids.size == 1 => b
          case (ids, SString(s)) if ids.size == 1  => s
          case (ids, SArray(a)) if ids.size == 1   => a
          case (ids, SObject(o)) if ids.size == 1  => o
        }

        result2.toSet must_== Set(42, 12, 77, 1, 13, true, false, "daniel", Map("test" -> SString("fubar")), Vector())
      }
    }

    "join two sets according to a value sort" in {
      val line = Line(1, 1, "")

      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)
      val clicks2 = dag.AbsoluteLoad(Const(CString("/clicks2"))(line))(line)

      val input = dag.Join(
        Add,
        ValueSort(0),
        AddSortKey(clicks, "time", "time", 0),
        AddSortKey(clicks2, "time", "time", 0))(line)

      testEval(dag.Join(DerefObject, Cross(None), clicks, Const(CString("time"))(line))(line)) { expected =>
        val decimalValues = expected.toList collect {
          case (_, SDecimal(d)) => d
        }

        val cross =
          for {
            x <- decimalValues
            y <- decimalValues
          } yield {
            (x, y)
          }

        val expectedResult = cross collect { case (x, y) if x == y => x + y }

        testEval(input) { result =>
          result must haveSize(expectedResult.size)

          val decis = result.toList collect {
            case (_, SDecimal(d)) => d
          }
          decis.sorted must_== expectedResult.sorted

          result must haveAllElementsLike {
            case (ids, SDecimal(d)) =>
              ids must haveSize(2)
              expectedResult must contain(d)
          }
        }
      }
    }

    "join two sets according to a value sort and then an identity sort" in {
      val line = Line(1, 1, "")

      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)
      val clicks2 = dag.AbsoluteLoad(Const(CString("/clicks2"))(line))(line)

      val input = dag.Join(Eq, IdentitySort,
        dag.Join(Add, ValueSort(0),
          AddSortKey(clicks, "time", "time", 0),
          AddSortKey(clicks2, "time", "time", 0))(line),
        dag.Join(Mul, Cross(None),
          dag.Join(DerefObject, Cross(None), clicks, Const(CString("time"))(line))(line),
          Const(CLong(2))(line))(line))(line)

      testEval(input) { result =>
        result must haveSize(106)

        val result2 = result collect {
          case (ids, SBoolean(b)) if ids.size == 2 => b
        }

        result2.toSet must_== Set(true)
        result2 must not(contain(false))
      }
    }

    "filter two sets according to a value sort" in {
      val line = Line(1, 1, "")

      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)
      val clicks2 = dag.AbsoluteLoad(Const(CString("/clicks2"))(line))(line)

      val input = dag.Filter(
        ValueSort(0),
        AddSortKey(clicks, "time", "time", 0),
        dag.Join(
          Gt,
          Cross(None),
          AddSortKey(clicks2, "time", "time", 0),
          Const(CLong(500))(line))(line))(line)

      testEval(dag.Join(DerefObject, Cross(None), clicks, Const(CString("time"))(line))(line)) { expected =>
        val decimalValues = expected.toList collect {
          case (_, SDecimal(d)) => d
        }

        val cross =
          for {
            x <- decimalValues
            y <- decimalValues
          } yield {
            (x, y)
          }

        val expectedResult = cross collect { case (x, y) if x > 500 && x == y  => x }

        testEval(input) { result =>
          result must haveSize(expectedResult.size)

          val decis = result.toList collect {
            case (_, SDecimal(d)) => d
          }
          decis.sorted must_== expectedResult.sorted


          result must haveAllElementsLike {
            case (ids, SDecimal(d)) =>
              ids must haveSize(2)
              expectedResult must contain(d)
          }
        }
      }
    }

    "produce a preemptive error when crossing enormous sets" in {
      val line = Line(1, 1, "")

      val tweets = dag.AbsoluteLoad(Const(CString("/election/tweets"))(line))(line)

      val input = dag.Join(Add, Cross(None),
        dag.Join(Add, Cross(None),
          tweets,
          tweets)(line),
        tweets)(line)

      testEval(input) { _ => failure } must throwAn[EnormousCartesianException]
    }

    "correctly perform a cross-filter" in {
      /*
       * t1 := //clicks
       * t2 := //views
       *
       * t1 ~ t2
       *   t1 where t1.userId = t2.userId
       */

      val line = Line(1, 1, "")

      val t1 = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)
      val t2 = dag.AbsoluteLoad(Const(CString("/clicks2"))(line))(line)

      val input = dag.Filter(IdentitySort,
        t1,
        Join(Eq, Cross(None),
          Join(DerefObject, Cross(None),
            t1,
            Const(CString("time"))(line))(line),
          Join(DerefObject, Cross(None),
            t2,
            Const(CString("time"))(line))(line))(line))(line)

      testEval(input) { _ must not(beEmpty) }
    }

    "correctly evaluate a constant array" in {
      // No Quirrel for this - only used for Evaluator rewrites

      val line = Line(1, 1, "")

      val input = dag.Const(RArray(CNum(1), CTrue, CString("three")))(line)

      testEval(input) { _ must haveSize(1) }
    }

    "correctly evaluate a constant object" in {
      // No Quirrel for this - only used for Evaluator rewrites

      val line = Line(1, 1, "")

      val input = dag.Const(RObject("a" -> CNum(1), "b" -> CTrue, "c" -> CString("true")))(line)

      testEval(input) { _ must haveSize(1) }
    }

    "evaluate as a transspec a cond on a single source" in {
      val line = Line(1, 1, "")

      val source = dag.New(
        dag.Const(RObject(
          "a" -> CBoolean(true),
          "b" -> CNum(1),
          "c" -> CNum(2)))(line))(line)

      val input = dag.Cond(
        Join(DerefObject, Cross(None),
          source,
          dag.Const(CString("a"))(line))(line),
        Join(DerefObject, Cross(None),
          source,
          dag.Const(CString("b"))(line))(line),
        IdentitySort,
        Join(DerefObject, Cross(None),
          source,
          dag.Const(CString("c"))(line))(line),
        IdentitySort)(line)

      testEval(input) { resultsE =>
        resultsE must haveSize(1)

        val results = resultsE collect {
          case (ids, sv) if ids.length == 1 => sv
        }

        results.head must_== SDecimal(1)
      }
    }

    "evaluate as a transspec a cond on a const left source" in {
      val line = Line(1, 1, "")

      val source = dag.New(
        dag.Const(RObject(
          "a" -> CBoolean(true),
          "c" -> CNum(2)))(line))(line)

      val input = dag.Cond(
        Join(DerefObject, Cross(None),
          source,
          dag.Const(CString("a"))(line))(line),
        dag.Const(CNum(1))(line),
        Cross(None),
        Join(DerefObject, Cross(None),
          source,
          dag.Const(CString("c"))(line))(line),
        IdentitySort)(line)

      testEval(input) { resultsE =>
        resultsE must haveSize(1)

        val results = resultsE collect {
          case (ids, sv) if ids.length == 1 => sv
        }

        results.head must_== SDecimal(1)
      }
    }

    "evaluate as a transspec a cond on a const right source" in {
      val line = Line(1, 1, "")

      val source = dag.New(
        dag.Const(RObject(
          "a" -> CBoolean(false),
          "c" -> CNum(2)))(line))(line)

      val input = dag.Cond(
        Join(DerefObject, Cross(None),
          source,
          dag.Const(CString("a"))(line))(line),
        Join(DerefObject, Cross(None),
          source,
          dag.Const(CString("c"))(line))(line),
        IdentitySort,
        dag.Const(CNum(1))(line),
        Cross(None))(line)

      testEval(input) { resultsE =>
        resultsE must haveSize(1)

        val results = resultsE collect {
          case (ids, sv) if ids.length == 1 => sv
        }

        results.head must_== SDecimal(1)
      }
    }

    "allow a cond to be used in the definition of a critical condition" in {
      val line = Line(1, 1, "")

      val clicks = dag.AbsoluteLoad(dag.Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1,
          clicks,
          UnfixedSolution(2,
            Cond(
              Join(Eq, Cross(None),
                clicks,
                dag.Const(CNull)(line))(line),
              clicks, IdentitySort,
              clicks, IdentitySort)(line))),
        SplitGroup(1, clicks.identities, id)(line), id)(line)

      testEval(input) { resultsE =>
        resultsE must haveSize(100)
      }
    }

    "assign identities to the results of flatten" in {
      val line = Line(1, 1, "")

      /*
       * flatten([{ a: 1, b: 2 }, { a: 3, b: 4 }])
       */

      val input = dag.Morph1(Flatten,
        Join(JoinArray, Cross(None),
          Operate(WrapArray,
            Join(JoinObject, Cross(None),
              Join(WrapObject, Cross(None),
                dag.Const(CString("a"))(line),
                dag.Const(CLong(1))(line))(line),
              Join(WrapObject, Cross(None),
                dag.Const(CString("b"))(line),
                dag.Const(CLong(2))(line))(line))(line))(line),
          Operate(WrapArray,
            Join(JoinObject, Cross(None),
              Join(WrapObject, Cross(None),
                dag.Const(CString("a"))(line),
                dag.Const(CLong(3))(line))(line),
              Join(WrapObject, Cross(None),
                dag.Const(CString("b"))(line),
                dag.Const(CLong(4))(line))(line))(line))(line))(line))(line)

      testEval(input) { resultsE =>
        resultsE must haveSize(2)

        forall(resultsE) {
          case (ids, sv) => {
            ids must haveSize(1)

            sv must beLike {
              case SObject(obj) => {
                obj must haveSize(2)
                obj must haveKey("a")
                obj must haveKey("b")
              }
            }
          }
        }
      }
    }

    "reduce the size of a filtered flattened array" in {
      val line = Line(1, 1, "")

      /*
       * foo := flatten([{ a: 1, b: 2 }, { a: 3, b: 4 }])
       * foo where foo.a = 1
       */
     (None)
      val foo = dag.Morph1(Flatten,
        Join(JoinArray, Cross(None),
          Operate(WrapArray,
            Join(JoinObject, Cross(None),
              Join(WrapObject, Cross(None),
                dag.Const(CString("a"))(line),
                dag.Const(CLong(1))(line))(line),
              Join(WrapObject, Cross(None),
                dag.Const(CString("b"))(line),
                dag.Const(CLong(2))(line))(line))(line))(line),
          Operate(WrapArray,
            Join(JoinObject, Cross(None),
              Join(WrapObject, Cross(None),
                dag.Const(CString("a"))(line),
                dag.Const(CLong(3))(line))(line),
              Join(WrapObject, Cross(None),
                dag.Const(CString("b"))(line),
                dag.Const(CLong(4))(line))(line))(line))(line))(line))(line)

      val input = dag.Filter(IdentitySort,
        foo,
        Join(Eq, Cross(None),
          Join(DerefObject, Cross(None),
            foo,
            dag.Const(CString("a"))(line))(line),
          dag.Const(CLong(1))(line))(line))(line)

      testEval(input) { resultsE =>
        resultsE must haveSize(1)
      }
    }
  }

  def joinDeref(left: DepGraph, first: Int, second: Int, line: Line): DepGraph =
    Join(DerefArray, Cross(None),
      Join(DerefArray, Cross(None),
        left,
        Const(CLong(first))(line))(line),
      Const(CLong(second))(line))(line)
}

object EvaluatorSpecs extends EvaluatorSpecs[Need]
