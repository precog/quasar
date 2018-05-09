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

package quasar.datagen

import slamdata.Predef._
import quasar.build.BuildInfo
import quasar.fp.numeric._

import java.io.File
import java.lang.IllegalArgumentException
import java.nio.file.StandardOpenOption
import scala.Console, Console.{RED, RESET}
import scala.Predef.implicitly

import fs2.util.Suspendable
import eu.timepit.refined.scalaz._
import monocle.macros.Lenses
import monocle.syntax.fields._
import scalaz.{Equal, Foldable, Show}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.apply._
import scalaz.syntax.foldable._
import scopt.{OptionParser, Read}

@Lenses
final case class CliOptions(
    sstFile: File,
    outSize: Positive,
    outFile: File,
    writeMode: StandardOpenOption,
    sstSource: SstSource) {

  def writeOptions: List[StandardOpenOption] =
    writeMode match {
      case StandardOpenOption.CREATE | StandardOpenOption.CREATE_NEW =>
        List(writeMode)

      case other =>
        List(StandardOpenOption.CREATE, other)
    }
}

object CliOptions extends CliOptionsInstances {
  val DefaultWriteMode: StandardOpenOption = StandardOpenOption.CREATE_NEW

  object parse {
    def apply[F[_]] = new PartiallyApplied[F]
    final class PartiallyApplied[F[_]] {
      def apply[C[_]: Foldable](args: C[String])(implicit F: Suspendable[F]): F[Option[CliOptions]] =
        F.delay(Parser.parse(args.toList, InitialOpts) flatMap {
          case (i, n, o, soo, w) => (i |@| n |@| o)(CliOptions(_, _, _, soo, w))
        })
    }
  }

  ////

  private type Opts = (Option[File], Option[Positive], Option[File], StandardOpenOption, SstSource)
  private val InitialOpts: Opts = (None, None, None, DefaultWriteMode, SstSource.Sample)

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  private implicit val readPositive: Read[Positive] =
    implicitly[Read[Long]].map(l => Positive(l) getOrElse {
      throw new IllegalArgumentException("Expected a positive number.")
    })

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private object Parser extends OptionParser[Opts]("datagen") {
    head("Quasar datagen", BuildInfo.version)

    note("Generates <size> random records based on the provided SST, writing the output to <outfile>.\n")

    opt[Unit]('a', "append")
      .text("Append generated records to the output file.")
      .action((_, o) => o.copy(_4 = StandardOpenOption.APPEND))

    opt[Unit]('r', "replace")
      .text("Replace the contents of the output file with the generated records.")
      .action((_, o) => o.copy(_4 = StandardOpenOption.TRUNCATE_EXISTING))

    opt[Unit]('w', "whole-sst")
      .text("Indicates the input SST was generated over the entire source dataset and not a sample.")
      .action((_, o) => o.copy(_5 = SstSource.Whole))

    help("help").text("Print this usage text.\n")

    arg[File]("<sstfile>")
      .text("Path to a file containing an SST in JSON format.")
      .action((f, o) => o.copy(_1 = Some(f)))

    arg[Positive]("<size>")
      .text("The number of records to generate.")
      .action((p, o) => o.copy(_2 = Some(p)))

    arg[File]("<outfile>")
      .text("Path to the file where generated records should be written.")
      .action((f, o) => o.copy(_3 = Some(f)))

    override def reportError(msg: String): Unit =
      Console.err.println(s"${RESET}${RED}[ERROR] ${msg}${RESET}")

    override def reportWarning(msg: String): Unit =
      Console.err.println("[WARN ] " + msg)
  }
}

sealed abstract class CliOptionsInstances {
  implicit val equal: Equal[CliOptions] =
    Equal.equalBy(o => (
      o.sstFile.getPath,
      o.outSize,
      o.outFile.getPath,
      o.writeMode,
      o.sstSource))

  implicit val show: Show[CliOptions] =
    Show.showFromToString

  private implicit def equalStandardOpenOption: Equal[StandardOpenOption] =
    Equal.equalA
}
