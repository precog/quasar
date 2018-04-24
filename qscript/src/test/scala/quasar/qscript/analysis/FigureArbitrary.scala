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

package quasar.qscript.analysis

import quasar.tpe.{CompositeType, CompositeTypeArbitrary}
import quasar.pkg.tests._

trait FigureArbitrary {
  import Outline.Figure
  import CompositeTypeArbitrary._

  implicit val figureArbitrary: Arbitrary[Figure] =
    Arbitrary(Gen.oneOf(
      Gen.const(Figure.undefined),
      Gen.const(Figure.unknown),
      arbitrary[CompositeType] map Figure.struct))
}

object FigureArbitrary extends FigureArbitrary
