/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar

import Predef._
import quasar.fp._

import org.specs2.time.NoTimeConversions
import quasar.Evaluator.EvalPathError
import quasar.specs2.DisjunctionMatchers

import quasar.fs.Path
import quasar.fs.Path.NonexistentPathError

class ErrorConditions extends BackendTest with NoTimeConversions with DisjunctionMatchers {

  backendShould { (prefix, _, backend, backendName) =>
    "Backend" should {
      "error out consistently" in {
        def testQueryOnMissingCollection(produceQueryFromCollectionPath: String => String) = {
          val missingCollectionPath = prefix ++ Path("IDoNotExist")
          backend.exists(missingCollectionPath).run.run.toOption.get should beFalse
          val query = produceQueryFromCollectionPath(missingCollectionPath.simplePathname)
          val results = interactive.eval(backend, query)
          results.run.run.run should beLeftDisjunction(Backend.PEvalError(EvalPathError(NonexistentPathError(missingCollectionPath,None))))
        }
        "in the case of a query that maps to a mapReduce in MongoDB" in {
          def query(collectionPath: String) = s"""SELECT name FROM `$collectionPath` WHERE LENGTH(name) > 10"""
          testQueryOnMissingCollection(query)
        }
        "in the case of a query that maps to an aggregation in MongoDB" in {
          def query(collectionPath: String) = s"""SELECT name FROM `$collectionPath` WHERE name.field1 > 10"""
          testQueryOnMissingCollection(query)
        }
      }
    }
    ()
  }
}
