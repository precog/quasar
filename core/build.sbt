name := "Core"

mainClass in Compile := Some("quasar.repl.Repl")

fork in run := true

connectInput in run := true

outputStrategy := Some(StdoutOutput)

import ScoverageSbtPlugin._

scoverage.ScoverageKeys.coverageExcludedPackages := "quasar.repl;.*RenderTree"

scoverage.ScoverageKeys.coverageMinimum := 78

scoverage.ScoverageKeys.coverageFailOnMinimum := true

scoverage.ScoverageKeys.coverageHighlighting := true

sbtbuildinfo.BuildInfoPlugin.projectSettings

buildInfoKeys := Seq[BuildInfoKey](version, scoverage.ScoverageKeys.coverageEnabled)

buildInfoPackage := "quasar.build"

wartremoverErrors in (Compile, compile) ++= Warts.allBut(
  // NB: violation counts are from running `compile`
  Wart.Any,               // 113
  Wart.AsInstanceOf,      //  75
  Wart.IsInstanceOf,      //  79
  Wart.NoNeedForMonad,    //  62
  Wart.Nothing,           // 366
  Wart.Product,           // 180  _ these two are highly correlated
  Wart.Serializable,      // 182  /
  Wart.Throw)             // 412
