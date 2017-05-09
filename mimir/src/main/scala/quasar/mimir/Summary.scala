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

import quasar.yggdrasil._
import quasar.yggdrasil.table._

import quasar.precog.common._
import quasar.yggdrasil.bytecode._
import scalaz._, Scalaz._

trait SummaryLibModule[M[+ _]] extends ReductionLibModule[M] {
  trait SummaryLib extends ReductionLib {
    import trans._
    import TransSpecModule._

    override def _libMorphism1 = super._libMorphism1 ++ Set(Summary)

    val reductions           = List(Mean, StdDev, Min, Max, Count)
    val coalesced: Reduction = coalesce(reductions.map(_ -> None))

    val reductionSpecs: List[TransSpec1] = reductions.reverse.zipWithIndex map {
      case (red, idx) =>
        trans.WrapObject(trans.DerefArrayStatic(TransSpec1.Id, CPathIndex(idx)), red.name)
    }
    val reductionSpec = reductionSpecs reduce { trans.OuterObjectConcat(_, _) }

    object SingleSummary extends Reduction(Vector(), "singleSummary") {
      val tpe = coalesced.tpe

      type Result = coalesced.Result
      val monoid: Monoid[Result] = coalesced.monoid

      def reducer(ctx: MorphContext): CReducer[Result] = coalesced.reducer(ctx)

      def extract(res: Result): Table = {
        val arrayTable = coalesced.extract(res)
        arrayTable.transform(reductionSpec)
      }

      def extractValue(res: Result): Option[RValue] = coalesced.extractValue(res)
    }

    object Summary extends Morphism1(Vector(), "summary") {
      val tpe = UnaryOperationType(JType.JUniverseT, JObjectUnfixedT)

      override val idPolicy: IdentityPolicy = IdentityPolicy.Strip

      def makeReduction(jtpe: JType): Reduction = {
        val jtypes: List[Option[JType]] = {
          val grouped = Schema.flatten(jtpe, List.empty[ColumnRef]).groupBy(_.selector)
          val numerics = grouped filter {
            case (cpath, refs) =>
              refs.map(_.ctype).exists(_.isNumeric)
          }

          // handles case when we have multiple numeric columns at same path
          val singleNumerics = numerics.toList.map { case (path, _) => (path, CNum) }
          val sortedNumerics = singleNumerics.distinct.sortBy(_._1).reverse

          sortedNumerics map {
            case (cpath, ctype) =>
              Schema.mkType(Seq(ColumnRef(cpath, ctype)))
          }
        }

        val functions: List[Option[JType => JType]] =
          jtypes.distinct map (_ map { Schema.replaceLeaf })

        coalesce(functions map { SingleSummary -> _ })
      }

      def reduceTable(table: Table, jtype: JType, ctx: MorphContext): M[Table] = {
        val reduction = makeReduction(jtype)

        implicit def monoid = reduction.monoid

        val values = table.reduce(reduction.reducer(ctx))

        def extract(result: reduction.Result, jtype: JType): Table = {
          val paths = Schema.cpath(jtype)

          val tree = CPath.makeTree(paths, 0 until paths.length)
          val spec = TransSpec.concatChildren(tree)

          reduction.extract(result).transform(spec)
        }

        values map { extract(_, jtype) }
      }

      def apply(table: Table, ctx: MorphContext) = {
        val jtypes0: M[Seq[Option[JType]]] = for {
          schemas <- table.schemas
        } yield {
          schemas.toSeq map { jtype =>
            val flattened = Schema.flatten(jtype, List.empty[ColumnRef])

            val values = flattened filter {
              case ref =>
                ref.selector.hasPrefix(paths.Value) && ref.ctype.isNumeric
            }

            Schema.mkType(values.toSeq)
          }
        }

        // one JType-with-numeric-leaves per schema
        val jtypes: M[Seq[JType]] = jtypes0 map (_ collect {
              case opt if opt.isDefined => opt.get
            })

        val specs: M[Seq[TransSpec1]] = jtypes map {
          _ map { trans.Typed(TransSpec1.Id, _) }
        }

        // one table per schema
        val tables: M[Seq[Table]] = specs map (_ map { spec =>
              table.transform(spec).compact(TransSpec1.Id, AllDefined)
            })

        val tablesWithType: M[Seq[(Table, JType)]] = for {
          tbls <- tables
          schemas <- jtypes
        } yield {
          tbls zip schemas
        }

        val resultTables: M[Seq[Table]] = tablesWithType flatMap {
          _.map {
            case (table, jtype) =>
              reduceTable(table, jtype, ctx)
          }.toStream.sequence map (_.toSeq)
        }

        val objectTables: M[Seq[Table]] = resultTables map {
          _.zipWithIndex map {
            case (tbl, idx) =>
              val modelId = "model" + (idx + 1)
              tbl.transform(trans.WrapObject(DerefObjectStatic(TransSpec1.Id, paths.Value), modelId))
          }
        }

        val spec = OuterObjectConcat(Leaf(SourceLeft), Leaf(SourceRight))

        val res = objectTables map {
          _.reduceOption { (tl, tr) =>
            tl.cross(tr)(spec)
          } getOrElse Table.empty
        }

        res map { _.transform(buildConstantWrapSpec(TransSpec1.Id)) }
      }
    }
  }
}
