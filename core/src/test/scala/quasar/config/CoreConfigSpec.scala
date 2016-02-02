package quasar.config

import quasar.Predef._
import quasar.fp.prism._
import quasar.fs.mount._
import quasar.physical.mongodb.fs.MongoDBFsType

import org.specs2.ScalaCheck
import pathy.Path._

class CoreConfigSpec extends ConfigSpec[CoreConfig] with ScalaCheck {
  import CoreConfigArbitrary._

  def configOps: ConfigOps[CoreConfig] = CoreConfig

  def sampleConfig(uri: ConnectionUri): CoreConfig = {
    CoreConfig(MountingsConfig2(Map(
      rootDir -> MountConfig2.fileSystemConfig(MongoDBFsType, uri)
    )))
  }

  val OldConfigStr =
    s"""{
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "database": "$dbName",
      |        "connectionUri": "${testUri.value}"
      |      }
      |    }
      |  }
      |}""".stripMargin

  "fromString" should {
    "parse previous config" in {
      configOps.fromString(OldConfigStr) must beRightDisjunction(TestConfig)
    }
  }

  "encoding" should {
    "round-trip any well-formed config" ! prop { (cfg: CoreConfig) =>
      val json = CoreConfig.codec.encode(cfg)
      val cfg2 = CoreConfig.codec.decode(json.hcursor)
      cfg2.result must beRightDisjunction(cfg)
    }.set(minTestsOk = 5)
  }
}
