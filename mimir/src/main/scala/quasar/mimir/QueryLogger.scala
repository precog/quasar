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

import quasar.precog.common._
import quasar.precog.common.security._

import quasar.blueeyes._, json._, serialization._
import Iso8601Serialization._

import quasar.blueeyes.util.Clock
import org.slf4j.LoggerFactory

import scalaz._
import scalaz.syntax.monad._

import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentHashMap

trait QueryLogger[M[+ _], -P] { self =>
  def contramap[P0](f: P0 => P): QueryLogger[M, P0] = new QueryLogger[M, P0] {
    def die(): M[Unit]                        = self.die()
    def error(pos: P0, msg: String): M[Unit]  = self.error(f(pos), msg)
    def warn(pos: P0, msg: String): M[Unit]   = self.warn(f(pos), msg)
    def info(pos: P0, msg: String): M[Unit]   = self.info(f(pos), msg)
    def log(pos: P0, msg: String): M[Unit]    = self.log(f(pos), msg)
    def timing(pos: P0, nanos: Long): M[Unit] = self.timing(f(pos), nanos)
    def done: M[Unit]                         = self.done
  }

  def die(): M[Unit]

  /**
    * This reports a error to the user. Depending on the implementation, this may
    * also stop computation completely.
    */
  def error(pos: P, msg: String): M[Unit]

  /**
    * Report a warning to the user.
    */
  def warn(pos: P, msg: String): M[Unit]

  /**
    * Report an informational message to the user.
    */
  def info(pos: P, msg: String): M[Unit]

  /**
    * Report an information message for internal use only
    */
  def log(pos: P, msg: String): M[Unit]

  /**
    * Record timing information for a particular position.  Note that a position
    * may record multiple timing events, which should be aggregated according to
    * simple summary statistics.
    *
    * Please note the following:
    *
    * kx = 303 seconds
    *   where
    *     2^63 - 1 = sum i from 0 to k, x^2
    *     x > 0
    *     k = 10000    (an arbitrary, plausible iteration count)
    *
    * This is to say that, for a particular position which is hit 10,000 times,
    * the total time spent in that particular position must be bounded by 303
    * seconds to avoid signed Long value overflow.  Conveniently, our query timeout
    * is 300 seconds, so this is not an issue.
    */
  def timing(pos: P, nanos: Long): M[Unit]

  def done: M[Unit]
}

trait LoggingQueryLogger[M[+ _], P] extends QueryLogger[M, P] {
  implicit def M: Applicative[M]

  protected val logger = LoggerFactory.getLogger("quasar.mimir.QueryLogger")

  def die(): M[Unit] = M.point { () }

  def error(pos: P, msg: String): M[Unit] = M.point {
    logger.error(pos.toString + " - " + msg)
  }

  def warn(pos: P, msg: String): M[Unit] = M.point {
    logger.warn(pos.toString + " - " + msg)
  }

  def info(pos: P, msg: String): M[Unit] = M.point {
    logger.info(pos.toString + " - " + msg)
  }

  def debug(pos: P, msg: String): M[Unit] = M.point {
    logger.debug(pos.toString + " - " + msg)
  }

  def log(pos: P, msg: String): M[Unit] = debug(pos, msg)
}

object LoggingQueryLogger {
  def apply[M[+ _]](implicit M0: Monad[M]): QueryLogger[M, Any] = {
    new LoggingQueryLogger[M, Any] with TimingQueryLogger[M, Any] {
      val M = M0
    }
  }
}

trait TimingQueryLogger[M[+ _], P] extends QueryLogger[M, P] {
  implicit def M: Monad[M]

  private val table = new ConcurrentHashMap[P, Stats]

  def timing(pos: P, nanos: Long): M[Unit] = {
    @tailrec
    def loop() {
      val stats = table get pos

      if (stats == null) {
        val stats = Stats(1, nanos, nanos * nanos, nanos, nanos)

        if (table.putIfAbsent(pos, stats) != stats) {
          loop()
        }
      } else {
        if (!table.replace(pos, stats, stats derive nanos)) {
          loop()
        }
      }
    }

    M point {
      loop()
    }
  }

  def done: M[Unit] = {
    val logging = table.asScala map {
      case (pos, stats) =>
        log(pos, """{"count":%d,"sum":%d,"sumSq":%d,"min":%d,"max":%d}""".format(stats.count, stats.sum, stats.sumSq, stats.min, stats.max))
    }

    logging reduceOption { _ >> _ } getOrElse (M point ())
  }

  private case class Stats(count: Long, sum: Long, sumSq: Long, min: Long, max: Long) {
    final def derive(nanos: Long): Stats = {
      copy(count = count + 1, sum = sum + nanos, sumSq = sumSq + (nanos * nanos), min = min min nanos, max = max max nanos)
    }
  }
}

trait ExceptionQueryLogger[M[+ _], -P] extends QueryLogger[M, P] {
  implicit def M: Applicative[M]

  abstract override def die(): M[Unit] =
    for {
      _ <- super.die()
      _ = throw FatalQueryException("Query terminated abnormally.")
    } yield ()
}

case class FatalQueryException(msg: String) extends RuntimeException(msg)
