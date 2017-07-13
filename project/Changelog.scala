package changelog

import scala._
import scala.Predef._

import sbt._, Keys._

object ChangeLog extends Plugin {
  override lazy val settings = Seq(commands += changelogCommand)

  lazy val changelogCommand = Command.command("changelog") { state =>

    // TODO: make the source dir/files configurable
    val sources = new java.io.File("CHANGELOG").listFiles().toList

    val Version = raw"(\d+)\.(\d+)\.(\d+)\.md".r
    val byVersion = sources.map(f =>
      f.getName match {
        case Version(maj, min, rev) => (Some(maj.toInt), Some(min.toInt), Some(rev.toInt)) -> f
        case "revision.md"          => ((None, None, None)) -> f
        case "minor.md"             => ((None, None, Some(0))) -> f
        case "major.md"             => ((None, Some(0), Some(0))) -> f
      }).sortBy { case ((maj, min, rev), _) => (maj.getOrElse(1000), min.getOrElse(1000), rev.getOrElse(1000)) }.reverse

    val lines = "# Change Log" :: "" :: byVersion.flatMap { case (ver, f) =>
      val head = ver match {
        case (Some(maj), Some(0),   Some(0))   => s"## $maj.0.0"
        case (None,      Some(0),   Some(0))   =>  "## Next major version (X.0.0)"
        case (Some(maj), Some(min), Some(0))   => s"### $maj.$min.0"
        case (None,      None,      Some(0))   =>  "### Next minor version (X.X.0)"
        case (Some(maj), Some(min), Some(rev)) => s"#### $maj.$min.$rev"
        case _                                 =>  "#### Next revision"
      }

      (head ::
        "" ::
        scala.io.Source.fromFile(f).getLines.toList) :+ "" :+ ""
    }

    // TODO: make this configurable
    val dst = new java.io.File("target/changelog.md")

    val w = new java.io.FileWriter(dst)
    lines.foreach(l => w.write(l + "\n"))
    w.close()

    println(s"Wrote change log for ${sources.length} versions at ${dst.getPath}")

    state
  }
}
