import com.github.retronym.SbtOneJar
import sbt._
import Keys._

def standardSettings = Seq(exportJars := true) ++ Defaults.defaultSettings

def oneJarSettings = standardSettings ++ SbtOneJar.oneJarSettings

def coreSettings = oneJarSettings ++ Seq(mainClass := Some("slamdata.engine.repl.Repl"))

def webSettings = oneJarSettings ++ Seq(mainClass := Some("slamdata.engine.api.Server"))

lazy val root = Project("root", file(".")) aggregate(core, web, it) settings (standardSettings: _*)

lazy val core = (project in file("core")) settings (coreSettings: _*)

lazy val web = (project in file("web")) dependsOn (core) settings (webSettings: _*)

lazy val it = (project in file("it")) dependsOn (core, web)

licenses += ("GNU Affero GPL V3", url("http://www.gnu.org/licenses/agpl-3.0.html"))