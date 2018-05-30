package precogbuild

import sbt._, Keys._
import scala._, Predef._

object Build {
  val BothScopes = "compile->compile;test->test"

  val warningOpts = Seq(
    //"-g:vars",
    //"-deprecation",
    //"-unchecked",
    "-feature",
    //"-Ywarn-value-discard",
    //"-Ywarn-numeric-widen",
    //"-Ywarn-unused",
    "-Ywarn-unused-import")

  val defaultArgSet = Seq(
    "-Ypartial-unification",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:implicitConversions",
    "-language:existentials")

  /** Watch out Jonesy! It's the ol' double-cross!
   *  Why, you...
   *
   *  Given a path like src/main/scala we want that to explode into something like the
   *  following, assuming we're currently building with java 1.7 and scala 2.10.
   *
   *    src/main/scala
   *    src/main/scala_2.10
   *    src/main_1.7/scala
   *    src/main_1.7/scala_2.10
   *
   *  Similarly for main/test, 2.10/2.11, 1.7/1.8.
   */
  def doubleCross(config: Configuration) = {
    unmanagedSourceDirectories in config ++= {
      val jappend = Seq("", "_" + javaSpecVersion)
      val sappend = Seq("", "_" + scalaBinaryVersion.value)
      val basis   = (sourceDirectory in config).value
      val parent  = basis.getParentFile
      val name    = basis.getName
      for (j <- jappend ; s <- sappend) yield parent / s"$name$j" / s"scala$s"
    }
  }

  def javaSpecVersion: String                       = sys.props("java.specification.version")
  def inBoth[A](f: Configuration => Seq[A]): Seq[A] = List(Test, Compile) flatMap f
  def kindProjector                                 = "org.spire-math" % "kind-projector" % "0.9.3" cross CrossVersion.binary

  implicit class ProjectOps(val p: sbt.Project) {
    def noArtifacts: Project = also(
                publish := (()),
           publishLocal := (()),
         Keys.`package` := file(""),
             packageBin := file(""),
      packagedArtifacts := Map()
    )
    def root: Project                                 = p in file(".")
    def also(ss: Seq[Setting[_]]): Project            = p settings (ss: _*)
    def also(s: Setting[_], ss: Setting[_]*): Project = also(s :: ss.toList)
    def deps(ms: ModuleID*): Project                  = also(libraryDependencies ++= ms.toList)
    def scalacArgs(args: String*): Project            = also(scalacOptions ++= args.toList)
    def strictVersions: Project                       = also(conflictManager := ConflictManager.strict)
    def serialTests: Project                          = also(parallelExecution in Test := false)
    def withWarnings: Project                         = scalacArgs(warningOpts: _*)
    def logImplicits: Project                         = scalacArgs("-Xlog-implicits")
    def crossJavaTargets: Project                     = also(inBoth(doubleCross))
    def scalacPlugins(ms: ModuleID*): Project         = also(ms.toList map (m => addCompilerPlugin(m)))

    def setup: Project = (
      serialTests scalacPlugins (kindProjector) scalacArgs (defaultArgSet: _*) also(
        organization := "org.quasar-analytics",
        scalaVersion := "2.12.4",
        logBuffered in Test := false))
  }
}
