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

import quasar.precog._
import quasar.yggdrasil._
import quasar.yggdrasil.table._

import quasar.blueeyes._
import quasar.precog.common._
import quasar.yggdrasil.bytecode._
import quasar.precog.util._
import spire.implicits._

import scalaz._, Scalaz._
import scala.util.Random.nextInt
import scala.collection.JavaConverters._

private object MissingSpireOps {
  // TODO: Use incremental mean, or switch back to Spire's when fixed.
  def meanSeq(xs: List[Double]): Double = {
    def loop(sum: Double, count: Double, acc: List[Double]): Double = acc match {
      case x :: tail => loop(sum + x, count + 1, tail)
      case Nil       => sum / count
    }

    loop(0D, 0D, xs)
  }
}

import MissingSpireOps._

/**
  * A simple decision tree. Each internal node is assigned an axis aligned
  * boundary which divides the space in 2 (left and right). To determine the
  * value of an input point, we simple determine which side of the boundary line
  * the input lies on, then recurse on that side. When we reach a leaf node, we
  * output its value.
  */
sealed trait DecisionTree[ /*@specialized(Double) */ A] {
  def apply(v: Array[Double]): A = {
    @tailrec def loop(tree: DecisionTree[A]): A = tree match {
      case Split(i, boundary, left, right) =>
        if (v(i) <= boundary) loop(left) else loop(right)
      case Leaf(k) =>
        k
    }

    loop(this)
  }
}

case class Split[ /*@specialized(Double) */ A](variable: Int, boundary: Double, left: DecisionTree[A], right: DecisionTree[A]) extends DecisionTree[A]

case class Leaf[ /*@specialized(Double) */ A](value: A) extends DecisionTree[A]

case class TreeMakerOptions(features: Int, featuresSampled: Int, minSplitSize: Int) {
  // Probability that we'll see a feature at least once if we sample features maxTries times.
  private val q = 0.95
  private val p = featuresSampled / features.toDouble

  val maxTries = math.ceil(math.log(1 - q) / math.log(p))
}

trait TreeMaker[ /*@specialized(Double) */ A] {

  protected trait RegionLike {
    def +=(k: A): Unit
    def -=(k: A): Unit
    def error: Double
    def value: A
    def copy(): Region
  }

  protected trait RegionCompanion {
    def empty: Region
  }

  protected type Region <: RegionLike
  protected def Region: RegionCompanion

  def makeTreeInitial(dependent: Array[A], independent: Array[Array[Double]], opts: TreeMakerOptions): DecisionTree[A] = {
    require(independent.length > 0, "Cannot make a decision without points.")

    def predictors(): Array[Int] = {
      val indices: Array[Int] = (0 until opts.featuresSampled).toArray
      var i = indices.length
      while (i < opts.features) {
        val j = nextInt(i + 1)
        if (j < indices.length)
          indices(j) = i
        i += 1
      }
      indices
    }

    def region(members: Array[Int]): Region = {
      val region = Region.empty
      var i = 0
      while (i < members.length) {
        region += dependent(members(i))
        i += 1
      }
      region
    }

    // case class Subset(left: Int, right: Int)

    val bitset: BitSet = BitSetUtil.create()

    // This creates a new `order` with only members from `split` retained.
    // The order of the elements in the returned array will be the same as they
    // were in `order`.
    def splitOrder(split: Array[Int], order: Array[Int]): Array[Int] = {
      var i = 0
      while (i < split.length) {
        bitset.set(split(i))
        i += 1
      }

      val newOrder = new Array[Int](split.length)
      var j = 0
      i = 0
      while (i < newOrder.length && j < order.length) {
        val m = order(j)
        if (bitset(m)) {
          bitset.clear(m)
          newOrder(i) = m
          i += 1
        }
        j += 1
      }

      newOrder
    }

    // members are the indices into the dependent & independent arrays
    def growTree(orders: Array[Array[Int]], tries: Int = 0): DecisionTree[A] = {
      val members = orders(0)

      if (members.size < opts.minSplitSize || tries > opts.maxTries) {
        Leaf(region(members).value)
      } else {
        val vars    = predictors()
        val region0 = region(members)
        var minError = region0.error
        var minVar   = -1 // index of the feature that so far has the lowest cost
        var minIdx   = -1 // index of the split point in the feature array

        var f = 0
        while (f < vars.length) {
          var leftRegion  = Region.empty
          var rightRegion = region0.copy()
          val axis  = vars(f)
          val order = orders(axis)

          var j = 0
          while (j < order.length - 1) {
            leftRegion += dependent(order(j))
            rightRegion -= dependent(order(j))
            val error = (leftRegion.error * (j + 1) +
                    rightRegion.error * (order.length - j - 1)) / order.length
            if (error < minError) {
              minError = error
              minVar = axis
              minIdx = j
            }

            j += 1
          }

          f += 1
        }

        if (minIdx < 0) {
          growTree(orders, tries + 1)
        } else {
          val featureOrder = orders(minVar)
          val leftSplit    = featureOrder take (minIdx + 1)
          val rightSplit   = featureOrder drop (minIdx + 1)

          var leftOrders  = new Array[Array[Int]](orders.length)
          var rightOrders = new Array[Array[Int]](orders.length)

          leftOrders(minVar) = leftSplit
          rightOrders(minVar) = rightSplit

          var i = 0
          while (i < orders.length) {
            if (i != minVar) {
              leftOrders(i) = splitOrder(leftSplit, orders(i))
              rightOrders(i) = splitOrder(rightSplit, orders(i))
            }
            i += 1
          }

          // We split the region directly between the left's furthest right point
          // and the right's furthest left point.

          val boundary = (independent(featureOrder(minIdx))(minVar) +
                  independent(featureOrder(minIdx + 1))(minVar)) / 2
          Split(minVar, boundary, growTree(leftOrders), growTree(rightOrders))
        }
      }
    }

    val orders = (0 until opts.features).map { i =>
      val order = (0 until dependent.length).toArray
      arrayOps(order).qsortBy(independent(_)(i))
      order
    }.toArray
    growTree(orders)
  }
}

class RegressionTreeMaker extends TreeMaker[Double] {

  // TODO: Use incremental mean.

  protected final class SquaredError(var sum: Double, var sumSq: Double, var count: Int) extends RegionLike {
    def +=(k: Double) {
      sum += k
      sumSq += (k * k)
      count += 1
    }

    def -=(k: Double) {
      sum -= k
      sumSq -= (k * k)
      count -= 1
    }

    def error: Double = sumSq / count - math.pow((sum / count), 2) // Error = variance.
    def value: Double = sum / count

    def copy(): SquaredError = new SquaredError(sum, sumSq, count)
  }

  protected type Region = SquaredError
  object Region extends RegionCompanion {
    def empty = new SquaredError(0D, 0D, 0)
  }
}

class ClassificationTreeMaker[K] extends TreeMaker[K] {

  protected final class GiniIndex(m: java.util.HashMap[K, Int], var n: Int) extends RegionLike {
    def +=(k: K) {
      val cnt = if (m containsKey k) m.get(k) + 1 else 1
      m.put(k, cnt)
      n += 1
    }

    def -=(k: K) {
      if (m containsKey k) {
        val cnt = m.get(k) - 1
        if (cnt == 0) {
          m.remove(k)
        } else {
          m.put(k, cnt)
        }
        n -= 1
      }
    }

    def error: Double = {
      val iter = m.values().iterator()
      var sumSqP = 0D
      while (iter.hasNext) {
        val p = iter.next().toDouble / n
        sumSqP += p * p
      }
      1D - sumSqP
    }

    def value: K = m.asScala.maxBy(_._2)._1

    def copy(): GiniIndex = new GiniIndex(new java.util.HashMap(m), n)
  }

  protected type Region = GiniIndex

  object Region extends RegionCompanion {
    def empty = new GiniIndex(new java.util.HashMap[K, Int](), 0)
  }
}

sealed trait Forest[A] {
  def trees: List[DecisionTree[A]]
  def predict(v: Array[Double]): A
  def ++(that: Forest[A]): Forest[A]
}

case class RegressionForest(trees: List[DecisionTree[Double]]) extends Forest[Double] {
  def predict(v: Array[Double]): Double = meanSeq(trees map (_ (v)))
  def ++(that: Forest[Double])          = RegressionForest(trees ++ that.trees)
}

object RegressionForest {
  implicit object RegressionForestMonoid extends Monoid[RegressionForest] {
    def zero: RegressionForest                              = RegressionForest(Nil)
    def append(x: RegressionForest, y: => RegressionForest) = x ++ y
  }
}

case class ClassificationForest[K](trees: List[DecisionTree[K]]) extends Forest[K] {
  def predict(v: Array[Double]): K = {
    trees
      .foldLeft(Map.empty[K, Int]) { (acc, classify) =>
        val k = classify(v)
        acc + (k -> (acc.getOrElse(k, 0) + 1))
      }
      .maxBy(_._2)
      ._1
  }
  def ++(that: Forest[K]) = ClassificationForest(trees ++ that.trees)
}

object ClassificationForest {
  implicit def ClassificationForestMonoid[A] = new Monoid[ClassificationForest[A]] {
    def zero: ClassificationForest[A]                                     = ClassificationForest[A](Nil)
    def append(x: ClassificationForest[A], y: => ClassificationForest[A]) = x ++ y
  }
}

trait RandomForestLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  private def makeArrays(table: Table): M[Array[Array[Double]]] = {
    extract(table) {
      case (c: HomogeneousArrayColumn[_]) => { (row: Int) =>
        c(row).asInstanceOf[Array[Double]]
      }
    }
  }

  private def collapse[@specialized(Double) A: CTag](chunks0: List[Array[A]]): Array[A] = {
    val len   = chunks0.foldLeft(0)(_ + _.length)
    val array = new Array[A](len)
    def mkArray(init: Int, chunks: List[Array[A]]): Unit = chunks match {
      case chunk :: tail =>
        var i = 0
        var j = init
        while (j < array.length && i < chunk.length) {
          array(j) = chunk(i)
          i += 1
          j += 1
        }
        mkArray(j, tail)

      case Nil =>
    }
    mkArray(0, chunks0)
    array
  }

  private def sliceToArray[@specialized(Double) A: CTag](slice: Slice, zero: => A)(pf: PartialFunction[Column, Int => A]): Option[Array[A]] = {
    slice.columns.values.collectFirst {
      case c if pf.isDefinedAt(c) => {
        val extract = pf(c)
        val xs      = new Array[A](slice.size)
        var i = 0
        while (i < xs.length) {
          if (c.isDefinedAt(i)) {
            xs(i) = extract(i)
          } else {
            xs(i) = zero
          }
          i += 1
        }
        xs
      }
    }
  }

  private def extract[A: CTag](table: Table)(pf: PartialFunction[Column, Int => A]): M[Array[A]] = {
    def die = sys.error("Cannot handle undefined rows. Expected dense column.")

    def loop(stream: StreamT[M, Slice], acc: List[Array[A]]): M[Array[A]] = {
      stream.uncons flatMap {
        case Some((head, tail)) =>
          val valuesOpt: Option[Array[A]] = sliceToArray[A](head, die)(pf)
          loop(tail, valuesOpt map (_ :: acc) getOrElse acc)

        case None =>
          M.point(collapse(acc.reverse))
      }
    }

    loop(table.slices, Nil)
  }

  trait RandomForestLib extends ColumnarTableLib {

    override def _libMorphism2 = super._libMorphism2 ++ Set(RandomForestClassification, RandomForestRegression)

    object RandomForestClassification extends RandomForest[RValue, ClassificationForest[RValue]](Vector("std", "stats"), "rfClassification") {
      def extractDependent(table: Table): M[Array[RValue]] = {
        def loop(stream: StreamT[M, Slice], acc: List[Array[RValue]]): M[Array[RValue]] = {
          stream.uncons flatMap {
            case Some((head, tail)) =>
              loop(tail, Array.tabulate(head.size)(head.toRValue(_)) :: acc)

            case None =>
              M.point(collapse(acc.reverse))
          }
        }

        loop(table.slices, Nil)
      }

      def makeTree(dependent: Array[RValue], independent: Array[Array[Double]]): DecisionTree[RValue] = {
        val treeMaker       = new ClassificationTreeMaker[RValue]()
        val dimension       = independent.headOption map (_.length) getOrElse 0
        val featuresSampled = math.max(2, math.sqrt(dimension).toInt)

        val opts = TreeMakerOptions(dimension, featuresSampled, 3)

        treeMaker.makeTreeInitial(dependent, independent, opts)
      }

      def forest(trees: Seq[DecisionTree[RValue]]) = ClassificationForest(trees.toList)

      def makeColumns(defined: BitSet, values: Array[RValue]): Map[ColumnRef, Column] = {
        var i = 0
        while (i < values.length) {
          if (!defined(i)) {
            values(i) = CUndefined
          }
          i += 1
        }

        val slice = Slice.fromRValues(values.toStream)
        slice.columns
      }

      def findError(actual: Array[RValue], predicted: Array[RValue]): Double = {
        var correct = 0D
        var i       = 0
        while (i < actual.length) {
          correct += (if (actual(i) == predicted(i)) 1D else 0D)
          i += 1
        }
        correct / actual.length
      }
    }

    object RandomForestRegression extends RandomForest[Double, RegressionForest](Vector("std", "stats"), "rfRegression") {
      import trans._

      def extractDependent(table: Table): M[Array[Double]] = {
        val spec   = DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble)
        val table0 = table.transform(spec)

        extract[Double](table0) {
          case col: DoubleColumn =>
            (row: Int) =>
              col(row)
        }
      }

      def makeTree(dependent: Array[Double], independent: Array[Array[Double]]): DecisionTree[Double] = {
        val treeMaker       = new RegressionTreeMaker()
        val dimension       = independent.headOption map (_.length) getOrElse 0
        val featuresSampled = math.max(2, (dimension / 3).toInt)

        val opts = TreeMakerOptions(dimension, featuresSampled, 5)

        treeMaker.makeTreeInitial(dependent, independent, opts)
      }

      def forest(trees: Seq[DecisionTree[Double]]) = RegressionForest(trees.toList)

      def makeColumns(defined: BitSet, values: Array[Double]): Map[ColumnRef, Column] = {
        val col = new ArrayDoubleColumn(defined, values)
        Map(ColumnRef(CPath.Identity, CDouble) -> col)
      }

      def findError(actual: Array[Double], predicted: Array[Double]): Double = {
        val actualMean = arrayOps(actual) qmean

        val diffs = actual - predicted
        val num   = diffs dot diffs

        var den = 0D
        var i   = 0
        while (i < actual.length) {
          val dx = actual(i) - actualMean
          den += dx * dx
          i += 1
        }

        1D - num / den
      }
    }

    abstract class RandomForest[A: CTag, F <: Forest[A]: Monoid](namespace: Vector[String], name: String) extends Morphism2(namespace, name) {
      import trans._
      import TransSpecModule._

      val tpe = BinaryOperationType(JType.JUniverseT, JType.JUniverseT, JObjectUnfixedT)

      val independent       = "predictors"
      val dependent         = "dependent"
      val chunkSize         = 3
      val numChunks         = 10
      val varianceThreshold = 0.001
      val sampleSize        = 10000
      val maxForestSize     = 2000

      val independentSpec = trans.DerefObjectStatic(TransSpec1.Id, CPathField(independent))
      val dependentSpec   = trans.DerefObjectStatic(TransSpec1.Id, CPathField(dependent))

      override val idPolicy = IdentityPolicy.Retain.Merge
      lazy val alignment    = MorphismAlignment.Custom(IdentityPolicy.Retain.Right, alignCustom _)

      def extractDependent(table: Table): M[Array[A]]

      def makeTree(dependent: Array[A], independent: Array[Array[Double]]): DecisionTree[A]

      def forest(trees: Seq[DecisionTree[A]]): F

      def makeColumns(defined: BitSet, values: Array[A]): Map[ColumnRef, Column]

      def findError(actual: Array[A], predicted: Array[A]): Double

      def makeForests(table0: Table): M[Seq[(JType, F)]] = {
        val spec0 = trans.DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble)
        val spec  = makeTableTrans(Map(CPathField(independent) -> spec0))

        val table = table0.transform(spec)

        val schemas: M[List[JType]] =
          table.transform(independentSpec).schemas map { _.toList }

        schemas flatMap (_ traverse { tpe =>
              makeForest(table, tpe) map (tpe -> _)
            })
      }

      def makeForest(table: Table, tpe: JType, prev: F = Monoid[F].zero): M[F] = {
        if (prev.trees.size > maxForestSize) {
          M.point(prev)
        } else {
          val numTrainingSamples = chunkSize * numChunks
          val numOfSamples       = numTrainingSamples + numChunks

          val jtype = JObjectFixedT(Map(independent -> tpe, dependent -> JType.JUniverseT))
          val specs = (0 until numOfSamples) map { _ =>
            trans.Typed(TransSpec1.Id, jtype)
          }

          table.sample(sampleSize, specs) map (_.toList) flatMap { samples =>
            val trainingSamples: List[Table]   = (samples take numTrainingSamples).toList
            val validationSamples: List[Table] = (samples drop numTrainingSamples).toList

            def withData[B](table: Table)(f: (Array[A], Array[Array[Double]]) => B): M[B] = {
              val indepTable = table.transform(independentSpec).toArray[Double]
              val depTable   = table.transform(dependentSpec)

              for {
                features <- makeArrays(indepTable)
                prediction <- extractDependent(depTable)
              } yield f(prediction, features)
            }

            val treesM: M[List[DecisionTree[A]]] = trainingSamples traverse { table =>
              withData(table)(makeTree)
            }

            def variance(values: List[Double]): Double = {
              val mean = meanSeq(values)
              val sumSq = values.foldLeft(0D) { (acc, x) =>
                val dx = x - mean
                acc + dx * dx
              }
              sumSq / (values.length - 1)
            }

            treesM flatMap { trees =>
              val forests: List[F] = (1 to numChunks)
                .foldLeft(Nil: List[F]) { (acc, i) =>
                  (forest(trees take (i * chunkSize)) |+| prev) :: acc
                }
                .reverse

              val errors: M[List[Double]] = (forests zip validationSamples) traverse {
                case (forest, table) =>
                  withData(table) { (actual, features) =>
                    val predicted = features map (forest.predict) // TODO: Unbox me!
                    val error     = findError(actual, predicted)
                    error
                  }
              }

              errors map (variance) flatMap { s2 =>
                val forest = forests.last
                if (s2 < varianceThreshold) {
                  M.point(forest)
                } else {
                  makeForest(table, tpe, forest)
                }
              }
            }
          }
        }
      }

      def morph1Apply(forests: Seq[(JType, F)]) = new Morph1Apply {
        import TransSpecModule._

        def apply(table: Table, ctx: MorphContext): M[Table] = {

          lazy val models: Map[String, (JType, F)] = forests.zipWithIndex.map({
            case (elem, i) =>
              ("model" + (i + 1)) -> elem
          })(collection.breakOut)

          lazy val specs: Seq[TransSpec1] = models.map({
            case (modelId, (jtype, _)) =>
              trans.WrapObject(trans.TypedSubsumes(TransSpec1.Id, jtype), modelId)
          })(collection.breakOut)

          lazy val spec: TransSpec1 = liftToValues(OuterObjectConcat(specs: _*))

          lazy val objectTable: Table = table.transform(spec)

          def predict(stream: StreamT[M, Slice]): StreamT[M, Slice] = {
            StreamT(stream.uncons map {
              case Some((head, tail)) => {
                val valueColumns = models.foldLeft(Map.empty[ColumnRef, Column]) {
                  case (acc, (modelId, (_, forest))) =>
                    val modelSlice = head.deref(paths.Value).deref(CPathField(modelId)).mapColumns(cf.util.CoerceToDouble).toArray[Double]
                    val vecsOpt = sliceToArray[Array[Double]](modelSlice, null) {
                      case (c: HomogeneousArrayColumn[_]) => { (row: Int) =>
                        c(row).asInstanceOf[Array[Double]]
                      }
                    }

                    val defined: BitSet  = BitSetUtil.create()
                    val values: Array[A] = new Array[A](head.size)

                    vecsOpt map { vectors =>
                      var i = 0
                      while (i < vectors.length) {
                        val v = vectors(i)
                        if (v != null) {
                          defined.set(i)
                          values(i) = forest.predict(v)
                        }
                        i += 1
                      }
                    }

                    val cols = makeColumns(defined, values)
                    acc ++ cols map {
                      case (ColumnRef(cpath, ctype), col) =>
                        ColumnRef(CPath(paths.Value, CPathField(modelId)) \ cpath, ctype) -> col
                    }
                }
                val keyColumns = head.deref(paths.Key).wrap(paths.Key).columns
                val columns    = keyColumns ++ valueColumns
                StreamT.Yield(Slice(columns, head.size), predict(tail))
              }

              case None =>
                StreamT.Done
            })
          }

          val predictions = if (forests.isEmpty) {
            Table.empty
          } else {
            Table(predict(objectTable.slices), objectTable.size)
          }

          M.point(predictions)
        }
      }

      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] = {
        val trainingTable = t1.transform(trans.DerefObjectStatic(TransSpec1.Id, paths.Value))
        val forestsM      = makeForests(trainingTable)

        forestsM map { forests =>
          (t2, morph1Apply(forests))
        }
      }
    }
  }
}
