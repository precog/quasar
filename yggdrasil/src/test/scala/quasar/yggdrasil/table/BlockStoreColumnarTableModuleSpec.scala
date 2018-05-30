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

package quasar.yggdrasil.table

import quasar.yggdrasil.TableModuleSpec
import quasar.yggdrasil.TableModule._

trait BlockStoreColumnarTableModuleSpec extends TableModuleSpec with BlockLoadSpec with BlockSortSpec with BlockAlignSpec {
  type MemoId = Int

  "a block store columnar table" should {
    "load" >> {
      "a problem sample1" in testLoadSample1
      "a problem sample2" in testLoadSample2
      "a problem sample3" in testLoadSample3
      "a problem sample4" in testLoadSample4
      //"a problem sample5" in testLoadSample5 //pathological sample in the case of duplicated ids.
      //"a dense dataset" in checkLoadDense //scalacheck + numeric columns = pain
    }
    "sort" >> {
      "fully homogeneous data"        in homogeneousSortSample
      "fully homogeneous data with object" in homogeneousSortSampleWithNonexistentSortKey
      "data with undefined sort keys" in partiallyUndefinedSortSample
      "heterogeneous sort keys"       in heterogeneousSortSample
      "heterogeneous sort keys case 2" in heterogeneousSortSample2
      "heterogeneous sort keys ascending" in heterogeneousSortSampleAscending
      "heterogeneous sort keys descending" in heterogeneousSortSampleDescending
      "top-level hetereogeneous values" in heterogeneousBaseValueTypeSample
      "sort with a bad schema"        in badSchemaSortSample
      "merges over three cells"       in threeCellMerge
      "empty input"                   in emptySort
      "with uniqueness for keys"      in uniqueSort

      "arbitrary datasets" in skipped { checkSortDense(SortAscending) } // FIXME skipped per #2309
      "arbitrary datasets descending" in skipped { checkSortDense(SortDescending) } // FIXME skipped per #2309
    }
  }
}

object BlockStoreColumnarTableModuleSpec extends BlockStoreColumnarTableModuleSpec
