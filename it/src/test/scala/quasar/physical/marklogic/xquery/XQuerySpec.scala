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

package quasar.physical.marklogic.xquery

import slamdata.Predef._
import quasar.{BackendName, Data, TestConfig}
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.testing

import com.marklogic.xcc.ContentSource
import org.specs2.specification.core.Fragment
import scalaz._, Scalaz._, concurrent.Task

/** A base class for tests of XQuery expressions/functions. */
abstract class XQuerySpec extends quasar.Qspec {
  type M[A] = EitherT[Writer[Prologs, ?], ErrorMessages, A]

  /** Convenience function for expecting results, i.e. xqy must resultIn(Data._str("foo")). */
  def resultIn(expected: Data) = equal(expected.right[ErrorMessages])

  /** Convenience function for expecting no results. */
  def resultInNothing = equal(noResults.left[Data])

  def xquerySpec(desc: BackendName => String)(tests: (M[XQuery] => ErrorMessages \/ Data) => Fragment): Unit =
    TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
      contentSourceConnection[Task](uri).map(cs => desc(backend.name) >> tests(evaluateXQuery(cs, _))).void
    }).unsafePerformSync

  ////

  private val noResults = "No results found.".wrapNel

  private def evaluateXQuery(cs: ContentSource, xqy: M[XQuery]): ErrorMessages \/ Data = {
    val (prologs, body) = xqy.run.run
    val mainModule      = body map (MainModule(Version.`1.0-ml`, prologs, _))

    mainModule >>= (mm =>
      testing.moduleResults[ReaderT[Task, ContentSource, ?]](mm)
        .run(cs)
        .map(_ >>= (_ \/> noResults))
        .unsafePerformSync)
  }
}
