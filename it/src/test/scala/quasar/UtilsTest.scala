package quasar.utils

import org.specs2.mutable.Specification

class UtilsTest extends Specification {
  "utils" should {
    "removeExtension" in {
      "simple filename" in {
        removeExtension("zips.data") ==== "zips"
      }
      "tricky filename" in {
        removeExtension("zips.latest.data") ==== "zips.latest"
      }
    }
  }
}
