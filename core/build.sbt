import quasar.project._

import scala.Some
import scala.collection.Seq

import scoverage._

mainClass in Compile := Some("quasar.repl.Repl")

libraryDependencies ++= Dependencies.core

fork in run := true

connectInput in run := true

outputStrategy := Some(StdoutOutput)

ScoverageKeys.coverageExcludedPackages := "quasar.repl;.*RenderTree"

ScoverageKeys.coverageMinimum := 79

ScoverageKeys.coverageFailOnMinimum := true

buildInfoKeys := Seq[BuildInfoKey](version, ScoverageKeys.coverageEnabled)

buildInfoPackage := "quasar.build"
