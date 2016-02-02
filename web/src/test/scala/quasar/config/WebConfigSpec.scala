package quasar.config

import quasar.Predef._
import quasar.fp.prism._
import quasar.fs.mount._
import quasar.physical.mongodb.fs.MongoDBFsType

import pathy.Path._

class WebConfigSpec extends ConfigSpec[WebConfig] {

  def configOps: ConfigOps[WebConfig] = WebConfig

  def sampleConfig(uri: ConnectionUri): WebConfig = WebConfig(
    server = ServerConfig(92),
    mountings = MountingsConfig2(Map(
      rootDir -> MountConfig2.fileSystemConfig(MongoDBFsType, uri))))

  override val ConfigStr =
    s"""{
      |  "server": {
      |    "port": 92
      |  },
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "connectionUri": "${testUri.value}"
      |      }
      |    }
      |  }
      |}""".stripMargin
}
