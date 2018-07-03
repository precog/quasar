import sbt._, Keys._

object ForkInTest extends AutoPlugin {

  override def requires = plugins.JvmPlugin
  override def trigger = allRequirements

  override def projectSettings = Seq(fork := true)
}
