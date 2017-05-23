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

import quasar.precog.util._
import quasar.yggdrasil._, table._
import quasar.blueeyes._
import quasar.precog.common._

import scalaz._, Scalaz._
import spire.implicits._

trait AssignClusterModule[M[+ _]] extends ColumnarTableLibModule[M] with ModelLibModule[M] {
  import trans._

  trait AssignClusterSupport extends ColumnarTableLib with ModelSupport {
    trait AssignClusterBase extends ModelBase { self: Morphism2 =>
      case class ModelCluster(name: ClusterId, featureValues: Map[CPath, Double])
      case class Model(name: ModelId, clusters: Array[ModelCluster])
      object Model extends ModelCompanion

      private type ClusterId = String
      private type ModelId   = String

      protected val reducer: CReducer[Models] = new CReducer[Models] {
        private val kPath = CPath(TableModule.paths.Key)
        private val vPath = CPath(TableModule.paths.Value)

        def reduce(schema: CSchema, range: Range): Models = {

          val rowIdentities = Model.createRowIdentities(schema)

          val rowModels: Int => Set[Model] = {
            val modelTuples: Map[ModelId, Set[(ModelId, ClusterId, CPath, DoubleColumn)]] = {
              schema.columnRefs.flatMap {
                case ref @ ColumnRef(CPath(TableModule.paths.Value, CPathField(modelName), CPathField(clusterName), rest @ _ *), ctype) =>
                  Schema.mkType(ref :: Nil) flatMap {
                    case jType                                                      =>
                      schema.columns(jType) collectFirst { case (col: DoubleColumn) => col }
                  } map { col =>
                    (modelName, clusterName, CPath((TableModule.paths.Value +: rest): _*), col)
                  }

                case _ => None
              } groupBy { _._1 }
            }

            val modelsByCluster = modelTuples map {
              case (modelId, models) =>
                (modelId, models.groupBy(_._2))
            }

            { (i: Int) =>
              val models0 = modelsByCluster.map {
                case (modelId, clusters) =>
                  val modelClusters0: Array[ModelCluster] = clusters.map {
                    case (clusterId, colInfo) =>
                      val featureValues = colInfo.collect {
                        case (_, _, cpath, col) if col.isDefinedAt(i) =>
                          cpath -> col(i)
                      }.toMap

                      ModelCluster(clusterId, featureValues)
                  }.toArray

                  val modelClusters = modelClusters0 filter {
                    case ModelCluster(_, featureValues) =>
                      !featureValues.isEmpty
                  }

                  Model(modelId, modelClusters)
              }.toSet

              models0 filter { case Model(_, modelClusters) => !modelClusters.isEmpty }
            }
          }

          range.toList flatMap { i =>
            val models = rowModels(i)
            if (models.isEmpty)
              None
            else
              Some(ModelSet(rowIdentities(i), models))
          }
        }
      }

      protected def morph1Apply(models: Models): Morph1Apply = new Morph1Apply {
        def scanner(modelSet: ModelSet): CScanner = new CScanner {
          type A = Unit
          def init: A = ()

          def scan(a: A, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
            def included(model: Model): Map[ColumnRef, Column] = {
              val featurePaths = (model.clusters).flatMap { _.featureValues.keys }.toSet

              val res = cols filter {
                case (ColumnRef(cpath, ctype), col) =>
                  featurePaths.contains(cpath)
              }

              val resPaths = res map { case (ColumnRef(cpath, _), _) => cpath } toSet

              if (resPaths == featurePaths) res
              else Map.empty[ColumnRef, Column]
            }

            def defined(cols: Map[ColumnRef, Column]): BitSet = {
              val columns = cols map { case (_, col) => col }

              BitSetUtil.filteredRange(range) { i =>
                if (columns.isEmpty) false
                else columns.forall(_ isDefinedAt i)
              }
            }

            def filteredRange(cols: Map[ColumnRef, Column]) = range.filter(defined(cols).apply)

            val result: Set[Map[ColumnRef, Column]] = {

              val modelsResult: Set[Map[ColumnRef, Column]] = modelSet.models map {
                case model =>
                  val includedModel = included(model)
                  val definedModel  = defined(includedModel)

                  val clusterIds: Array[String] = model.clusters map { _.name }
                  val clusterCenters: Array[Array[Double]] = (model.clusters).map {
                    _.featureValues.toArray.sortBy { case (path, _) => path }.map { case (_, col) => col }.toArray
                  }

                  val centerPaths: Array[CPath] = model.clusters collectFirst {
                    case (m: ModelCluster) => m.featureValues.keys.toArray.sorted
                  } getOrElse Array.empty[CPath]

                  val featureColumns0 = includedModel.collect {
                    case (ref, col: DoubleColumn)                 => (ref, col)
                  }.toArray sortBy { case (ColumnRef(path, _), _) => path }
                  val featureColumns = featureColumns0 map { case (_, col) => col }

                  val numFeatures = featureColumns.size

                  val filtered = filteredRange(includedModel).toArray
                  val len      = filtered.length

                  val resultArray = {
                    var k = range.start
                    val arr = new Array[String](range.end - range.start)

                    while (k < len) {
                      val row: Int = filtered(k)

                      val feature = new Array[Double](numFeatures)
                      var i = 0
                      while (i < feature.length) {
                        feature(i) = featureColumns(i)(row)
                        i += 1
                      }

                      var minDistSq  = Double.PositiveInfinity
                      var minCluster = -1
                      i = 0
                      while (i < clusterCenters.length) {
                        // TODO: Don't box for fancy operators...

                        val diff   = (feature - clusterCenters(i))
                        val distSq = diff dot diff
                        if (distSq < minDistSq) {
                          minDistSq = distSq
                          minCluster = i
                        }
                        i += 1
                      }

                      arr(row) = clusterIds(minCluster)
                      k += 1
                    }
                    arr
                  }

                  def transposeResults(values: Array[Array[Double]]) = {
                    var k = 0
                    val acc = Array.fill(centerPaths.length)(Array.empty[Double])

                    while (k < values.length) {
                      var i = 0
                      val li = values(k)

                      while (i < li.length) {
                        acc(i) = acc(i) :+ li(i)
                        i += 1
                      }
                      k += 1
                    }
                    acc
                  }

                  val transposed = transposeResults(clusterCenters)

                  val clusterIdWithIdx: Map[String, Int] = clusterIds.zipWithIndex.toMap

                  val colsByPath: Array[Column] = transposed map { coords =>
                    new BitsetColumn(definedModel) with DoubleColumn {
                      def apply(row: Int) = coords(clusterIdWithIdx(resultArray(row)))
                    }
                  }

                  assert(colsByPath.length == centerPaths.length)
                  val zipped: Array[(Column, CPath)] = colsByPath zip centerPaths

                  val pref = CPath(TableModule.paths.Value)

                  val centers: Map[ColumnRef, Column] = zipped.collect {
                    case (col, path) if path.hasPrefix(pref) =>
                      val path0 = CPath(TableModule.paths.Value, CPathField(model.name), CPathField("clusterCenter"))
                      ColumnRef(path0 \ path.dropPrefix(pref).get, CDouble) -> col
                  }.toMap

                  val idPath   = CPath(TableModule.paths.Value, CPathField(model.name), CPathField("clusterId"))
                  val centerId = Map(ColumnRef(idPath, CString) -> ArrayStrColumn(definedModel, resultArray))

                  centers ++ centerId
              }

              modelsResult ++ Set(Model.idRes(cols, modelSet))
            }

            implicit val semigroup = Column.unionRightSemigroup
            val monoidCols         = implicitly[Monoid[Map[ColumnRef, Column]]]

            val reduced: Map[ColumnRef, Column] = result.toSet.suml(monoidCols)

            ((), reduced)
          }
        }

        def apply(table: Table, ctx: MorphContext): M[Table] = {
          val scanners: Seq[TransSpec1] = models map { model =>
            WrapArray(Scan(TransSpec1.Id, scanner(model)))
          }
          val spec: TransSpec1 = scanners reduceOption { (s1, s2) =>
            InnerArrayConcat(s1, s2)
          } getOrElse TransSpec1.Id

          val forcedTable = table.transform(spec).force
          val tables0 = (0 until scanners.size) map { i =>
            forcedTable.map(_.transform(DerefArrayStatic(TransSpec1.Id, CPathIndex(i))))
          }
          val tables: M[Seq[Table]] = (tables0.toList).sequence[M, Table]

          tables.map(_.reduceOption { _ concat _ } getOrElse Table.empty)
        }
      }
    }
  }
}
