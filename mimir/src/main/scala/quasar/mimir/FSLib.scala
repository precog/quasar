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

import quasar.blueeyes._
import quasar.precog.common._
import quasar.precog.common.security._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil._
import quasar.yggdrasil.table._
import quasar.yggdrasil.vfs._

import java.util.regex._
import scalaz._, Scalaz._

trait FSLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  def vfs: VFSMetadata[M]

  import trans._

  trait FSLib extends ColumnarTableLib {
    import constants._

    val FSNamespace = Vector("std", "fs")
    override def _libMorphism1 = super._libMorphism1 ++ Set(expandGlob)

    object expandGlob extends Morphism1(FSNamespace, "expandGlob") {
      val tpe = UnaryOperationType(JTextT, JTextT)

      val pattern = Pattern.compile("""/?+((?:[a-zA-Z0-9\-\._~:?#@!$&'+=]+)|\*)""")

      def expand_*(apiKey: APIKey, pathString: String, pathRoot: Path): M[Stream[Path]] = {
        def walk(m: Matcher, prefixes: Stream[Path]): M[Stream[Path]] = {
          if (m.find) {
            m.group(1).trim match {
              case "*" =>
                prefixes traverse { prefix =>
                  vfs.findDirectChildren(apiKey, prefix).fold(_ => Set(), a => a) map { child =>
                    child map { prefix / _.path }
                  }
                } flatMap { paths =>
                  walk(m, paths.flatten)
                }

              case token =>
                walk(m, prefixes.map(_ / Path(token)))
            }
          } else {
            M.point(prefixes)
          }
        }

        walk(pattern.matcher(pathString), Stream(pathRoot))
      }

      def apply(input: Table, ctx: MorphContext): M[Table] = M.point {
        val result = Table(
          input.transform(SourceValue.Single).slices flatMap { slice =>
            slice.columns.get(ColumnRef.identity(CString)) collect {
              case col: StrColumn =>
                val expanded: Stream[M[Stream[Path]]] = Stream.tabulate(slice.size) { i =>
                  expand_*(ctx.evalContext.apiKey, col(i), ctx.evalContext.basePath)
                }

                StreamT wrapEffect {
                  expanded.sequence map { pathSets =>
                    val unprefixed: Stream[String] = for {
                      paths <- pathSets
                      path <- paths
                      suffix <- (path - ctx.evalContext.basePath)
                    } yield suffix.toString

                    Table.constString(unprefixed.toSet).slices
                  }
                }
            } getOrElse {
              StreamT.empty[M, Slice]
            }
          },
          UnknownSize
        )

        result.transform(WrapObject(Leaf(Source), TransSpecModule.paths.Value.name))
      }
    }
  }
}
