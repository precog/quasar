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

import quasar.precog.BitSet
import quasar.precog.common._
import quasar.precog.util._
import quasar.yggdrasil._, table._

import scalaz._, Scalaz._

trait ModelLibModule[M[+ _]] {
  trait ModelSupport {
    import TableModule.paths

    val kPath = CPath(paths.Key)
    val vPath = CPath(paths.Value)

    val coefficients          = "coefficients"
    val estimate              = "estimate"
    val residualStandardError = "residualStandardError"
    val varianceCovariance    = "varianceCovarianceMatrix"
    val degreesOfFreedom      = "degreesOfFreedom"

    trait ModelBase {
      type Model

      def Model: ModelCompanion

      type ModelIdentity = List[List[(ColumnRef, CValue)]]

      case class ModelSet(identity: ModelIdentity, models: Set[Model])
      type Models = List[ModelSet]

      trait ModelCompanion {
        def idRes(cols: Map[ColumnRef, Column], modelSet: ModelSet): Map[ColumnRef, Column] = {
          val keyCols = cols filter {
            case (ColumnRef(CPath(paths.Key, CPathIndex(_), _ @_ *), _), _) => true
            case _                                                          => false
          }

          val shift = keyCols.size

          val modelCols: Map[ColumnRef, Column] = modelSet.identity.zipWithIndex.flatMap({
            case (id, idx) =>
              id map {
                case (ColumnRef(cpath, ctype), cvalue) =>
                  val cpath0 = CPath(paths.Key :: CPathIndex(idx + shift) :: cpath.nodes)
                  (ColumnRef(cpath0, ctype), Column.const(cvalue))
              }
          })(collection.breakOut)

          val idCols = modelCols ++ keyCols
          if (idCols.isEmpty) {
            Map(ColumnRef(CPath(paths.Key), CEmptyArray) -> Column.const(CEmptyArray))
          } else {
            idCols
          }
        }

        def createRowIdentities(schema: CSchema): Int => ModelIdentity = {
          def build(acc: Map[Int, List[(ColumnRef, Column)]], refs: List[ColumnRef]): Map[Int, List[(ColumnRef, Column)]] = refs match {
            case ColumnRef(cpath @ CPath(paths.Key, CPathIndex(idx), rest @ _ *), ctype) :: tail =>
              val cols = for {
                jType <- Schema.mkType(ColumnRef(cpath, ctype) :: Nil).toSet
                col <- schema.columns(jType)
              } yield (ColumnRef(CPath(rest: _*), ctype) -> col)
              build(acc + (idx                           -> (acc.getOrElse(idx, Nil) ++ cols)), tail)

            case _ :: tail => build(acc, tail)
            case Nil       => acc
          }

          val keys = build(Map.empty, schema.columnRefs.toList).toList.sortBy(_._1).map(_._2)

          { (row: Int) =>
            keys map { cols =>
              cols map {
                case (ref, col) =>
                  (ref, if (col.isDefinedAt(row)) col.cValue(row) else CUndefined)
              }
            }
          }
        }
      }
    }

    trait RegressionModelBase extends ModelBase {
      type Model <: RegressionModelLike
      def Model: RegressionModelCompanion

      trait RegressionModelLike {
        val featureValues: Map[CPath, Double]
        val constant: Double
      }

      trait RegressionModelCompanion extends ModelCompanion {
        def included(model: Model, cols: Map[ColumnRef, Column]): Map[ColumnRef, Column] = {
          val featurePaths = model.featureValues.keySet

          val res = cols filter {
            case (ColumnRef(cpath, ctype), col) =>
              featurePaths.contains(cpath)
          }

          val resPaths = res map { case (ColumnRef(cpath, _), _) => cpath } toSet

          if (resPaths == featurePaths) res
          else Map.empty[ColumnRef, Column]
        }

        def defined(cols: Map[ColumnRef, Column], range: Range): BitSet = {
          val columns = cols map { case (_, col) => col }

          BitSetUtil.filteredRange(range) { i =>
            if (columns.isEmpty) false
            else columns.forall(_ isDefinedAt i)
          }
        }

        def filteredRange(cols: Map[ColumnRef, Column], range: Range) = range.filter(defined(cols, range).apply)

        case class ScannerPrelims(includedModel: Map[ColumnRef, Column],
                                  definedModel: BitSet,
                                  cpaths: Seq[CPath],
                                  includedCols: Map[CPath, DoubleColumn],
                                  resultArray: Array[Double])
      }
    }

    def determineColumns(schema: CSchema, cpaths: Set[CPath]): Map[CPath, DoubleColumn] = {
      cpaths.map { cpath =>
        val jtpe = Schema.mkType(Seq(ColumnRef(cpath, CDouble)))

        val col = jtpe flatMap { tpe =>
          val res = schema.columns(tpe)

          if (res.length == 1) res.head match {
            case (col: DoubleColumn) => Some(col)
            case _                   => sys.error("Expected DoubleColumn.")
          } else if (res.length == 0) {
            None
          } else {
            sys.error("Incorrect number of columns.")
          }
        }
        (cpath, col)
      }.collect {
        case (path, col) if col.isDefined =>
          (path, col.get)
      }.toMap
    }

    def alignWithModels(schema: CSchema, modelWithPaths: Map[String, Set[CPath]]): Map[String, Map[CPath, DoubleColumn]] = {
      modelWithPaths map {
        case (modelName, cpaths) =>
          (modelName, determineColumns(schema, cpaths))
      }
    }

    trait LinearModelBase extends RegressionModelBase {
      case class Model(name: String, featureValues: Map[CPath, Double], constant: Double, resStdErr: Double, varCovar: Array[Array[Double]], degOfFreedom: Int)
          extends RegressionModelLike

      object Model extends RegressionModelCompanion

      protected val reducer: CReducer[Models] = new CReducer[Models] {
        def reduce(schema: CSchema, range: Range): Models = {
          val rowIdentities = Model.createRowIdentities(schema)

          val modelNames: Set[String] = schema.columnRefs.collect {
            case ColumnRef(CPath(paths.Value, CPathField(modelName), _ @_ *), _) => modelName
          }.toSet

          val interceptPaths = modelNames.map { modelName =>
            (modelName, Set(CPath(paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(1), CPathField(`estimate`))))
          }.toMap

          val stdErrPaths = modelNames.map { modelName =>
            (modelName, Set(CPath(paths.Value, CPathField(modelName), CPathField(`residualStandardError`), CPathField(`estimate`))))
          }.toMap

          val dofPaths = modelNames.map { modelName =>
            (modelName, Set(CPath(paths.Value, CPathField(modelName), CPathField(`residualStandardError`), CPathField(`degreesOfFreedom`))))
          }.toMap

          val covarPaths = schema.columnRefs.collect {
            case ColumnRef(path @ CPath(paths.Value, CPathField(modelName), CPathField(`varianceCovariance`), _ @_ *), _) => (modelName, path)
          }.groupBy(_._1) map {
            case (modelName, paths) =>
              (modelName, paths.map(_._2))
          }

          val featuresPaths = schema.columnRefs.collect {
            case ColumnRef(path @ CPath(paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(0), rest @ _ *), _)
                if rest.length > 0 && rest.last == CPathField(`estimate`) =>
              (modelName, path)
          }.groupBy(_._1) map {
            case (modelName, paths) =>
              (modelName, paths.map(_._2))
          }

          val interceptCols = alignWithModels(schema, interceptPaths)
          val stdErrCols    = alignWithModels(schema, stdErrPaths)
          val dofCols       = alignWithModels(schema, dofPaths)
          val covarCols     = alignWithModels(schema, covarPaths)
          val featuresCols  = alignWithModels(schema, featuresPaths)

          //error prone; ideally determine common keys earlier
          val commonKeys = interceptCols.keySet & stdErrCols.keySet & dofCols.keySet & covarCols.keySet & featuresCols.keySet

          val joined0 = commonKeys map {
            case field =>
              (field, List(interceptCols(field), stdErrCols(field), dofCols(field), covarCols(field), featuresCols(field)))
          } toMap

          val rowModels: Int => Set[Model] = (i: Int) => {
            val joined = joined0 filterNot {
              case (_, cols) =>
                val definedCols = cols map { _ filter { case (_, col) => col.isDefinedAt(i) } }
                definedCols.exists(_.isEmpty)
            }

            joined.collect {
              case (field, cols @ List(constant, resStdErr, degs, varCovar, values)) =>
                val cnst = constant.map {
                  case (_, col) =>
                    col.apply(i)
                }.headOption getOrElse { sys.error("Constant term must exist") }

                val rse = resStdErr.map {
                  case (_, col) =>
                    col.apply(i)
                }.headOption getOrElse { sys.error("Error term must exist") }

                val dof = degs.map {
                  case (_, col) =>
                    col.apply(i).toInt
                }.headOption getOrElse { sys.error("DOF term must exist") }

                val fts = values map {
                  case (CPath(paths.Value, CPathField(_), CPathField(`coefficients`), CPathIndex(0), rest @ _ *), col) =>
                    val paths0 = paths.Value +: rest.take(rest.length - 1)
                    (CPath(paths0: _*), col.apply(i))
                }

                val vc: Map[CPath, Double] = varCovar map {
                  case (CPath(paths.Value, CPathField(_), CPathField(`varianceCovariance`), rest @ _ *), col) =>
                    (CPath(rest: _*), col.apply(i))
                }
                val size = fts.size + 1
                val acc  = Array.fill(size)(new Array[Double](size))

                vc foreach {
                  case (CPath(CPathIndex(i), CPathIndex(j)), value) if (i < size) && (j < size) => acc(i)(j) = value
                  case _                                                                        => sys.error("Incorrect CPath structure found.")
                }

                Model(field, fts, cnst, rse, acc, dof)
            }.toSet
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
    }

    trait LogisticModelBase extends RegressionModelBase {
      case class Model(name: String, featureValues: Map[CPath, Double], constant: Double) extends RegressionModelLike

      object Model extends RegressionModelCompanion

      protected val reducer: CReducer[Models] = new CReducer[Models] {
        def reduce(schema: CSchema, range: Range): Models = {
          val rowIdentities = Model.createRowIdentities(schema)

          val modelNames: Set[String] = schema.columnRefs.collect {
            case ColumnRef(CPath(paths.Value, CPathField(modelName), _ @_ *), _) => modelName
          }.toSet

          val interceptPaths = modelNames.map { modelName =>
            (modelName, Set(CPath(paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(1), CPathField(`estimate`))))
          }.toMap

          val featuresPaths = schema.columnRefs.collect {
            case ColumnRef(path @ CPath(paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(0), rest @ _ *), _)
                if rest.length > 0 && rest.last == CPathField(`estimate`) =>
              (modelName, path)
          }.groupBy(_._1) map {
            case (modelName, paths) =>
              (modelName, paths.map(_._2))
          }

          val interceptCols = alignWithModels(schema, interceptPaths)
          val featuresCols  = alignWithModels(schema, featuresPaths)

          //error prone; ideally determine common keys earlier
          val commonKeys = interceptCols.keySet & featuresCols.keySet

          val joined0 = commonKeys map {
            case field =>
              (field, List(interceptCols(field), featuresCols(field)))
          } toMap

          val rowModels: Int => Set[Model] = (i: Int) => {
            val joined = joined0 filterNot {
              case (_, cols) =>
                val definedCols = cols map { _ filter { case (_, col) => col.isDefinedAt(i) } }
                definedCols.exists(_.isEmpty)
            }

            joined.collect {
              case (field, cols @ List(constant, values)) =>
                val cnst = constant.map {
                  case (_, col) =>
                    col.apply(i)
                }.headOption getOrElse { sys.error("Constant term must exist") }

                val fts = values collect {
                  case (CPath(paths.Value, CPathField(_), CPathField(`coefficients`), CPathIndex(0), rest @ _ *), col) if col.isDefinedAt(i) =>
                    val paths0 = paths.Value +: rest.take(rest.length - 1)
                    (CPath(paths0: _*), col.apply(i))
                }

                Model(field, fts, cnst)
            }.toSet
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
    }
  }
}
