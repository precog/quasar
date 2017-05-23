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

import quasar.yggdrasil.bytecode._
import quasar.yggdrasil._
import quasar.yggdrasil.table._
import quasar.precog.common._
import quasar.blueeyes._
import scalaz._, Scalaz._

trait KMediansCoreSetClustering {
  type CoreSet = (Array[Array[Double]], Array[Long])

  object CoreSet {
    def fromWeightedPoints(points: Array[Array[Double]], weights: Array[Long], k: Int, epsilon: Double): CoreSet = {
      val threshold = (k / epsilon) * math.log(points.length)

      if (points.length < threshold) {
        (points, weights)
      } else {
        val centers = createCenters(points, weights)
        //System.err.println("*** centers.length=%s" format centers.length)

        val (centers0, weights0) = makeCoreSet(points, weights, centers)
        //System.err.println("*** centers0.length=%s" format centers0.length)

        val (_, clusters, _) = approxKMedian(centers0, weights0, k)

        val (_, centers1) = localSearch(centers0, weights0, clusters, epsilon)

        makeCoreSet(points, weights, centers1)
      }
    }
  }

  def epsilon: Double

  case class CoreSetTree(tree: List[(Int, CoreSet)], k: Int) {
    def coreSet: CoreSet = {
      val coresets = tree map {
        case (_, coreset) =>
          CoreSet.fromWeightedPoints(coreset._1, coreset._2, k, epsilon / 6.0)
      }

      coresets.foldLeft((new Array[Array[Double]](0), new Array[Long](0))) {
        case ((centers0, weights0), (centers1, weights1)) =>
          (centers0 ++ centers1, weights0 ++ weights1)
      }
    }

    def mergeCoreSets(c1: CoreSet, c2: CoreSet, level: Int): CoreSet = {
      import spire.syntax.all._
      val c                    = 1
      val (centers1, weights1) = c1
      val (centers2, weights2) = c2
      val epsilon0             = epsilon / (c * ((level + 1) ** 2))

      CoreSet.fromWeightedPoints(centers1 ++ centers2, weights1 ++ weights2, k, epsilon0)
    }

    private def insertCoreSet(coreset: CoreSet, level: Int): CoreSetTree = {
      val (prefix, suffix) = tree partition { case (idx, _) => idx < level }

      def rec(tree0: List[(Int, CoreSet)], coreset0: CoreSet, level0: Int): List[(Int, CoreSet)] = {
        tree0 match {
          case (`level0`, coreset1) :: tail =>
            rec(tail, mergeCoreSets(coreset0, coreset1, level0), level0 + 1)
          case _ =>
            (level0, coreset0) :: tree0
        }
      }

      CoreSetTree(prefix ++ rec(suffix, coreset, level), k)
    }

    def ++(that: CoreSetTree): CoreSetTree = (
      if (that.k < k)
        that ++ this
      else {
        var acc: CoreSetTree = this
        for ((level, coreset) <- that.tree) acc = acc.insertCoreSet(coreset, level)
        acc
      }
    )
  }

  object CoreSetTree {
    def empty: CoreSetTree = CoreSetTree(Nil, Int.MaxValue)

    def apply(coreSet: CoreSet, k: Int): CoreSetTree = CoreSetTree((0, coreSet) :: Nil, k)

    def fromPoints(points: Array[Array[Double]], k: Int): CoreSetTree = {
      val weights = new Array[Long](points.length)
      java.util.Arrays.fill(weights, 1L)

      val cs = CoreSet.fromWeightedPoints(points, weights, k, epsilon)
      //System.err.println("CoreSetTree.fromPoints with points=%d k=%d" format (points.length, k))
      //System.err.println("cs._1.length=%s" format cs._1.length)
      CoreSetTree(cs, k)
    }

    implicit def CoreSetMonoid: Monoid[CoreSetTree] = new Monoid[CoreSetTree] {
      def zero                                        = CoreSetTree.empty
      def append(c1: CoreSetTree, c2: => CoreSetTree) = c1 ++ c2
    }
  }

  def weightArray(xs: Array[Double], ws: Array[Long]) {
    var i = 0
    while (i < xs.length) {
      xs(i) *= ws(i)
      i += 1
    }
  }

  /**
    * This returns the cost of the k-medians clustering given by `centers`. The
    * points must also be associated with a set of weights.
    */
  def kMediansCost(points: Array[Array[Double]], weights: Array[Long], centers: Array[Array[Double]], threshold: Double): Double = {
    var i     = 0
    var total = 0.0
    while (i < points.length && total < threshold) {
      var minDistSq = Double.PositiveInfinity
      var j         = 0
      while (j < centers.length) {
        val dsq = distSq(points(i), centers(j))
        if (dsq < minDistSq) minDistSq = dsq
        j += 1
      }
      total += math.sqrt(minDistSq) * weights(i)
      i += 1
    }
    total
  }

  /**
    * This finds a good approximation to the best possible points in `points` to
    * use for the centers for k-Medians.
    *
    * @note This algorithm is specific to the k-Medians version of the coreset
    *       algorithm.
    *
    * @link http://www.cs.ucla.edu/~awm/papers/lsearch.ps
    */
  private def localSearch(points: Array[Array[Double]],
                          weights: Array[Long],
                          centers0: Array[Array[Double]],
                          epsilon: Double): (Double, Array[Array[Double]]) = {

    val minCost0 = kMediansCost(points, weights, centers0, Double.PositiveInfinity)
    val centers  = java.util.Arrays.copyOf(centers0, centers0.length)

    var minCost = minCost0
    var i       = 0
    val clen = centers.length
    val plen = points.length
    while (i < plen) {
      val threshold = minCost * (1 - epsilon) / clen
      var j = 0
      while (j < clen) {
        val prevCenter = centers(j)
        centers(j) = points(i)
        val cost = kMediansCost(points, weights, centers, threshold)
        if (cost < threshold) {
          minCost = cost
          i = -1
          j = centers.length
        } else {
          centers(j) = prevCenter
        }
        j += 1
      }
      i += 1
    }

    (minCost, centers)
  }

  /**
    * This finds a candidate set of center points from `points`. It does this by
    * clustering points, taking all points in `points` that mess up the cost of
    * the clustering, then recursing and finding the centers of these *bad*
    * points and adding them to our other centers.
    *
    * @note This is the algorithm described in Section 4 of the Coresets paper.
    * @link http://valis.cs.uiuc.edu/~sariel/papers/03/kcoreset/kcoreset.pdf
    */
  private def createCenters(points: Array[Array[Double]], weights: Array[Long]): Array[Array[Double]] = {
    import spire.implicits._

    if (points.length < 100) {
      points
    } else {
      val k      = math.max(4, math.pow(points.length, 0.25).toInt + 1)
      val weight = weights.qsum

      val (cost, clustering, isCenter) = approxKMedian(points, weights, k)

      if (cost == 0) {
        clustering
      } else {
        var radius = cost / weight

        val sampleSize = math.min(k * math.log(points.length), points.length / 10d)
        //System.err.println("k=%s sampleSize=%s" format (k, sampleSize))

        val samples = points.take(sampleSize.toInt)

        var i = samples.length
        while (i < points.length) {
          val idx = scala.util.Random.nextInt(i + 1)
          if (idx < samples.length) samples(idx) = points(i)
          i += 1
        }

        val centers                  = clustering ++ samples
        val (distances, assignments) = assign(points, centers)

        val logRadius = math.log(radius)
        val logWeight = math.log(weight)
        val log2      = math.log(2)

        val klassCounts = new Array[Long](2 * math.ceil(logWeight).toInt + 3)

        i = 0
        while (i < distances.length) {
          val relPos = (math.log(distances(i)) - logRadius + logWeight) / log2
          val klass  = math.max(math.floor(relPos).toInt + 1, 0)
          assignments(i) = klass
          if (klass < klassCounts.length) {
            klassCounts(klass) += weights(i)
          }
          i += 1
        }

        val thresholdCount = weight / (10 * logWeight)
        i = klassCounts.length - 1
        while (i >= 0 && klassCounts(i) < thresholdCount) {
          i -= 1
        }
        val alpha = i
        //System.err.println("thresholdCount=%s alpha=%s" format (thresholdCount, alpha))

        @inline def isBad(idx: Int) = assignments(idx) > alpha && !isCenter(idx)

        // Remove all points whose klass <= i || cluster
        var keepLength = 0
        i = 0
        while (i < assignments.length) {
          if (isBad(i)) keepLength += 1
          i += 1
        }

        //System.err.println("badPoints.length=%s and centers.length=%s" format (keepLength, centers.length))
        val badPoints  = new Array[Array[Double]](keepLength)
        val badWeights = new Array[Long](keepLength)
        i = 0
        var j = 0
        while (i < points.length) {
          if (isBad(i)) {
            badPoints(j) = points(i)
            badWeights(j) = weights(i)
            j += 1
          }
          i += 1
        }

        centers ++ createCenters(badPoints, badWeights)
      }
    }
  }

  /**
    * Returns a clustering that is within 2 times the cost of the optimal k-medians clustering.
    *
    * The algorithm is fairly simple. It starts with a ranomd seed cluster. It then adds a new
    * cluster by finding the point that is farthest away from its nearest cluster. This point is
    * the seed for a new cluster. We repeat until we have `k` clusters.
    *
    * @note Clustering to Minimize the Maximum Intercluster Distance, Gonzalez 1984
    * @link http://www.cs.ucsb.edu/~TEO/papers/Ktmm.pdf
    */
  def approxKMedian(points: Array[Array[Double]], weights: Array[Long], k: Int): (Double, Array[Array[Double]], Array[Boolean]) = { // (cost, centers, isCenter)

    import spire.implicits._

    val reps = new Array[Array[Double]](k)
    reps(0) = points(0)

    def weight(pointIdx: Int, clusterIdx: Int): Double = {
      dist(points(pointIdx), reps(clusterIdx)) * weights(pointIdx)
    }

    val isCenter = new Array[Boolean](points.length)
    isCenter(0) = true

    val distances = new Array[Double](points.length)

    var i = 0
    while (i < distances.length) {
      distances(i) = weight(i, 0)
      i += 1
    }

    i = 0
    while (i < k - 1) {
      var maxWeight = 0.0
      var maxIdx    = 0

      var j = 0
      while (j < distances.length) {
        if (maxWeight < distances(j)) {
          maxWeight = distances(j)
          maxIdx = j
        }
        j += 1
      }

      reps(i + 1) = points(maxIdx)
      isCenter(maxIdx) = true
      distances(maxIdx) = 0.0

      j = 0
      while (j < points.length) {
        val w = weight(j, i + 1)
        if (w < distances(j)) {
          distances(j) = w
        }
        j += 1
      }

      i += 1
    }

    (distances.qsum, reps, isCenter)
  }

  /**
    * This returns a 2-tuple of an array of distances of each point to their nearest center
    * and an array of cluster indexes each point belongs to.
    */
  def assign(points: Array[Array[Double]], clustering: Array[Array[Double]]): (Array[Double], Array[Int]) = {
    val distances   = new Array[Double](points.size)
    val assignments = new Array[Int](points.size)

    var i = 0

    while (i < points.length) {
      var minDist = Double.PositiveInfinity
      var j       = 0
      while (j < clustering.length) {
        val d = dist(points(i), clustering(j))
        if (d < minDist) {
          assignments(i) = j
          minDist = d
        }
        j += 1
      }

      distances(i) = minDist

      i += 1
    }

    (distances, assignments)
  }

  /**
    * Given a possible centers, this finds a coreset for those centers.
    *
    * @note This is the algorithm described in Section 3 of the coresets paper.
    * @link http://valis.cs.uiuc.edu/~sariel/papers/03/kcoreset/kcoreset.pdf
    */
  private def makeCoreSet(points: Array[Array[Double]], weights: Array[Long], clustering: Array[Array[Double]]): CoreSet = {
    import spire.implicits._
    val (distance, assignments) = assign(points, clustering)

    weightArray(distance, weights)

    val cost   = distance.qsum
    val weight = weights.qsum
    //val c = 4d
    val c = 0.125
    val n = points.length

    val radiusGLB     = cost / (c * weight)
    val maxResolution = math.max(0, math.ceil(2 * math.log(c * weight))).toInt
    //System.err.println("maxResolution=%s" format maxResolution)
    val logRadiusGLB = math.log(radiusGLB)
    val log2         = math.log(2d)

    def grid(center: Array[Double]): Array[Double] => GridPoint = {
      val sideLengths: Array[Double] = (0 to maxResolution).map({ j =>
        epsilon * radiusGLB * math.pow(2d, j) / (10 * c * center.length)
      })(collection.breakOut)

      { (point: Array[Double]) =>
        val minx = distMin(point, center)
        val j    = math.max(0, math.ceil((math.log(minx) - logRadiusGLB) / log2).toInt)

        require(j < sideLengths.length, "Point (%d) found outside of grid (%d). What to do..." format (j, sideLengths.length))

        val sideLength = sideLengths(j)
        val scaledPoint = {
          if (sideLength == 0) (point - center)
          else (point - center) :/ sideLength
        }

        var i = 0
        while (i < scaledPoint.length) {
          scaledPoint(i) = center(i) + math.floor(scaledPoint(i)) * sideLength + (sideLength / 2)
          i += 1
        }
        new GridPoint(scaledPoint)
      }
    }

    val grids = clustering map grid

    var weightMap: Map[GridPoint, Long] = Map.empty
    var i                               = 0
    while (i < points.length) {
      val point      = points(i)
      val assignment = assignments(i)
      val weight     = weights(i)
      val gridPoint  = grids(assignment)(point)

      weightMap += (gridPoint -> (weightMap.getOrElse(gridPoint, 0L) + weight))
      i += 1
    }

    var coreset0: Array[Array[Double]] = new Array[Array[Double]](weightMap.size)
    var weights0: Array[Long]          = new Array[Long](coreset0.length)

    weightMap.zipWithIndex foreach {
      case ((gridPoint, weight), i) =>
        coreset0(i) = gridPoint.point
        weights0(i) = weight
    }

    (coreset0, weights0)
  }

  def distSq(x: Array[Double], y: Array[Double]): Double = {
    var i   = 0
    var acc = 0d
    val len = math.min(x.length, y.length)
    while (i < len) {
      val delta = x(i) - y(i)
      acc += delta * delta
      i += 1
    }
    acc
  }

  def dist(x: Array[Double], y: Array[Double]): Double = math.sqrt(distSq(x, y))

  def distMin(x: Array[Double], y: Array[Double]): Double = {
    var minx = Double.PositiveInfinity
    var i    = 0
    val len = math.min(x.length, y.length)
    while (i < len) {
      val dx = math.abs(x(i) - y(i))
      if (dx < minx) minx = dx
      i += 1
    }
    minx
  }

  case class GridPoint(point: Array[Double]) {
    override def hashCode: Int = {
      var hash: Int = point.length * 17
      var i         = 0

      while (i < point.length) {
        hash += point(i).toInt * 23 //todo is toInt correct
        i += 1
      }
      hash
    }

    override def equals(that: Any): Boolean = that match {
      case GridPoint(thatPoint) => //Eq[Array[Double]].eqv(this.point, thatPoint)
        if (this.point.length != thatPoint.length) return false
        var i = 0
        while (i < this.point.length) {
          if (this.point(i) != thatPoint(i)) return false
          i += 1
        }
        true
      case _ => false
    }
  }
}

trait ClusteringLibModule[M[+ _]] extends ColumnarTableModule[M] with AssignClusterModule[M] {
  import spire.implicits._

  trait ClusteringLib extends ColumnarTableLib with AssignClusterSupport {
    import trans._
    import TransSpecModule._

    override def _libMorphism2 = super._libMorphism2 ++ Set(KMediansClustering, AssignClusters)
    val Stats4Namespace = Vector("std", "stats")

    object KMediansClustering extends Morphism2(Stats4Namespace, "kMedians") with KMediansCoreSetClustering {
      val tpe = BinaryOperationType(JType.JUniverseT, JNumberT, JObjectUnfixedT)

      lazy val alignment = MorphismAlignment.Custom(IdentityPolicy.Retain.Cross, alignCustom _)

      type KS = List[Int]
      val epsilon = 0.1

      implicit def monoidKS: Monoid[KS] = new Monoid[KS] {
        def zero: KS                    = List.empty[Int]
        def append(ks1: KS, ks2: => KS) = ks1 ++ ks2
      }

      def reducerKS: CReducer[KS] = new CReducer[KS] {
        def reduce(schema: CSchema, range: Range): KS = {
          val columns = schema.columns(JObjectFixedT(Map("value" -> JNumberT)))
          val cols: List[Int] = (columns flatMap {
                case lc: LongColumn =>
                  range collect {
                    case i if lc.isDefinedAt(i) && lc(i) > 0 => lc(i).toInt
                  }

                case dc: DoubleColumn =>
                  range flatMap { i =>
                    if (dc.isDefinedAt(i)) {
                      val n = dc(i)
                      if (n.isValidInt && n > 0) {
                        Some(n.toInt)
                      } else {
                        None
                      }
                    } else {
                      None
                    }
                  }

                case nc: NumColumn =>
                  range flatMap { i =>
                    if (nc.isDefinedAt(i)) {
                      val n = nc(i)
                      if (n.isValidInt && n > 0) {
                        Some(n.toInt)
                      } else {
                        None
                      }
                    } else {
                      None
                    }
                  }

                case _ => List.empty[Int]
              }).toList
          cols
        }
      }

      def reducerFeatures(k: Int): CReducer[CoreSetTree] = new CReducer[CoreSetTree] {
        def reduce(schema: CSchema, range: Range): CoreSetTree = {
          val features = schema.columns(JArrayHomogeneousT(JNumberT))

          // we know that there is only one item in `features`
          val values: Option[Array[Array[Double]]] = features collectFirst {
            case c: HomogeneousArrayColumn[_] if c.tpe.classTag.erasure == classOf[Array[Double]] =>
              val mapped = range.toArray filter { r =>
                c.isDefinedAt(r)
              } map { i =>
                c.asInstanceOf[HomogeneousArrayColumn[Double]](i)
              }
              mapped
          }

          values map { v =>
            CoreSetTree.fromPoints(v, k)
          } getOrElse CoreSetTree.empty
        }
      }

      def extract(coreSetTree: CoreSetTree, k: Int, jtype: JType, modelId: Int): Table = {
        val (centers, weights) = coreSetTree.coreSet

        // TODO for a better approximation, instead use algorithm outlined here
        // http://valis.cs.uiuc.edu/~sariel/papers/03/kcoreset/kcoreset.pdf
        val (_, points, _) = approxKMedian(centers, weights, k)

        val cpaths = Schema.cpath(jtype)

        val tree = CPath.makeTree(cpaths, Range(0, points.head.length).toSeq)

        val spec = TransSpec.concatChildren(tree)

        val tables = points map { pt =>
          Table.fromRValues(Stream(RArray(pt.map(CNum(_)).toList)))
        }

        val transformedTables = tables map { table =>
          table.transform(spec)
        }

        val wrappedTables = transformedTables.zipWithIndex map {
          case (tbl, idx) => tbl.transform(trans.WrapObject(TransSpec1.Id, "cluster" + (idx + 1)))
        }

        val table = wrappedTables reduce { (t1, t2) =>
          t1.cross(t2)(trans.InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
        }

        val result = table.transform(trans.WrapObject(TransSpec1.Id, "model" + modelId))

        val valueTable = result.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
        val keyTable   = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

        valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
      }

      def morph1Apply(ks: List[Int]): Morph1Apply = new Morph1Apply {
        def apply(table0: Table, ctx: MorphContext): M[Table] = {
          val table = table0.transform(DerefObjectStatic(trans.DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble), paths.Value))

          val defaultNumber = new java.util.concurrent.atomic.AtomicInteger(1)

          val res = ks map { k =>
            val schemas: M[Seq[JType]] = table.schemas map { _.toSeq }

            val specs: M[Seq[(TransSpec1, JType)]] = schemas map {
              _ map { jtype =>
                (trans.Typed(TransSpec1.Id, jtype), jtype)
              }
            }

            val tables: StreamT[M, (Table, JType)] = StreamT.wrapEffect {
              specs map { ts =>
                StreamT.fromStream(M.point((ts map { case (spec, jtype) => (table.transform(spec), jtype) }).toStream))
              }
            }

            // arrived at semi-emprically testing 400k rows of data
            // TODO: arguably this should be a parameter we can pass in to tune things.
            val sliceSize = 4000
            val features: StreamT[M, Table] = tables flatMap {
              case (tbl, jtype) =>
                val coreSetTree = tbl.canonicalize(sliceSize).toArray[Double].normalize.reduce(reducerFeatures(k))

                StreamT(coreSetTree map { tree =>
                  StreamT.Yield(extract(tree, k, jtype, defaultNumber.getAndIncrement), StreamT.empty[M, Table])
                })
            }

            features
          }

          val tables: StreamT[M, Table] = res.foldLeft(StreamT.empty[M, Table])(_ ++ _)
          val modelConcat = buildConstantWrapSpec(
            OuterObjectConcat(DerefObjectStatic(Leaf(SourceLeft), paths.Value), DerefObjectStatic(Leaf(SourceRight), paths.Value)))

          def merge(table: Option[Table], tables: StreamT[M, Table]): OptionT[M, Table] = {
            OptionT(tables.uncons flatMap {
              case Some((head, tail)) =>
                table map { tbl =>
                  merge(Some(tbl.cross(head)(modelConcat)), tail).run
                } getOrElse {
                  merge(Some(head), tail).run
                }
              case None =>
                M.point(table)
            })
          }

          merge(None, tables) getOrElse Table.empty
        }
      }

      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] =
        t2.reduce(reducerKS) map { ks =>
          (t1, morph1Apply(ks))
        }
    }

    object AssignClusters extends Morphism2(Vector("std", "stats"), "assignClusters") with AssignClusterBase {
      val tpe = BinaryOperationType(JType.JUniverseT, JObjectUnfixedT, JObjectUnfixedT)

      override val idPolicy = IdentityPolicy.Retain.Merge

      lazy val alignment = MorphismAlignment.Custom(IdentityPolicy.Retain.Cross, alignCustom _)

      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] = {
        val spec = liftToValues(trans.DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble))
        t2.transform(spec).reduce(reducer) map { models =>
          (t1.transform(spec), morph1Apply(models))
        }
      }
    }
  }
}
