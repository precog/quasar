package quasar

import sbt._, Keys._
import sbttravisci.TravisCiPlugin, TravisCiPlugin.autoImport._

object TravisSpecs2Tuck extends AutoPlugin {

  override def requires = TravisCiPlugin && plugins.JvmPlugin
  override def trigger  = allRequirements

  override def projectSettings = Seq(
    testFrameworks := {
      if (isTravisBuild.value)
        Seq(TestFramework("quasar.specs2.TravisSpecs2Runner"))
      else
        (Global / testFrameworks).value
    },

    Test / parallelExecution := {
      if (isTravisBuild.value)
        false
      else
        (Test / parallelExecution).value
    })
}
