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

package quasar.physical.rdbms.testutils

import org.scalacheck.{Arbitrary, Gen}
import pathy.Path.{DirName, FileName}

/**
 * Arbitrary instances for directory and file names conforming to RDBMS constraints for allowed characters
 * in identifiers.
 */
object RdbmsPathyArbitrary {

  object Generators {
    def alphaNumDirGen: Gen[DirName] =
      Gen.alphaNumStr.map(DirName.apply).suchThat(!_.value.isEmpty)

    def alphaNumFileGen: Gen[FileName] =
      Gen.alphaNumStr.map(FileName.apply).suchThat(!_.value.isEmpty)
  }

  implicit def abDirNames: Arbitrary[DirName] = Arbitrary(Generators.alphaNumDirGen)

}
