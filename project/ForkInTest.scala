import sbt._, Keys._

object ForkInTest extends AutoPlugin {

  override def requires = plugins.JvmPlugin
  override def trigger = allRequirements

  object autoImport {
    val JvmOptsFromFile = IO.read(file(".jvmopts")).split("\n").toSeq.map(_.trim)
  }

  override def projectSettings = Seq(
    Test / fork := true,
    Test / javaOptions ++= autoImport.JvmOptsFromFile,

    Test / baseDirectory := file(file(".").getAbsolutePath))
}
