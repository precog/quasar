import quasar.project._

import scala.collection.Seq

import scoverage._

libraryDependencies ++= Dependencies.core

ScoverageKeys.coverageExcludedPackages := "quasar.repl;.*RenderTree"

ScoverageKeys.coverageMinimum := 79

ScoverageKeys.coverageFailOnMinimum := true

buildInfoKeys := Seq[BuildInfoKey](version, ScoverageKeys.coverageEnabled)

buildInfoPackage := "quasar.build"
