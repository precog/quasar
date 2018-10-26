/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.common

import quasar.Qspec

import scalaz.std.option._
import scalaz.syntax.std.option._

object CPathSpec extends Qspec {

  "CPath parser" >> {

    "parse empty path" >> {
      CPath.parse(".") must_= CPath.Identity
    }

    // is this what we want?
    "parse multiple . as identity" >> {
      CPath.parse("...") must_= CPath.Identity
    }

    "parse single field" >> {
      CPath.parse(".aa") must_= CPath(CPathField("aa"))
    }

    "parse single field not starting with ." >> {
      CPath.parse("aa") must_= CPath(CPathField("aa"))
    }

    "parse multiple fields" >> {
      CPath.parse(".aa.bb.cc.dd") must_=
        CPath(CPathField("aa"), CPathField("bb"), CPathField("cc"), CPathField("dd"))
    }

    // is this what we want?
    "parse multiple fields with extra ." >> {
      CPath.parse(".aa...bb..cc.dd") must_=
        CPath(CPathField("aa"), CPathField("bb"), CPathField("cc"), CPathField("dd"))
    }

    "parse multiple fields not starting with ." >> {
      CPath.parse("aa.bb.cc.dd") must_=
        CPath(CPathField("aa"), CPathField("bb"), CPathField("cc"), CPathField("dd"))
    }

    "parse single index" >> {
      CPath.parse("[3]") must_= CPath(CPathIndex(3))
    }

    "parse multiple indices" >> {
      CPath.parse("[3][5][7][11]") must_=
        CPath(CPathIndex(3), CPathIndex(5), CPathIndex(7), CPathIndex(11))
    }

    "parse single array" >> {
      CPath.parse("[*]") must_= CPath(CPathArray)
    }

    "parse multiple arrays" >> {
      CPath.parse("[*][*][*][*]") must_=
        CPath(CPathArray, CPathArray, CPathArray, CPathArray)
    }

    "parse index structure with non-numerics as a field" >> {
      CPath.parse(".[42foobar]") must_= CPath(CPathField("[42foobar]"))
    }

    "parse index structure with non-numerics as a field not starting with ." >> {
      CPath.parse("[42foobar]") must_= CPath(CPathField("[42foobar]"))
    }

    "parse field structure with numerics as a field" >> {
      CPath.parse(".42") must_= CPath(CPathField("42"))
    }

    "parse field structure with numerics as a field not starting with ." >> {
      CPath.parse("42") must_= CPath(CPathField("42"))
    }

    "parse mixed path starting with field" >> {
      CPath.parse(".aa[3].bb[*][5]") must_=
        CPath(CPathField("aa"), CPathIndex(3), CPathField("bb"), CPathArray, CPathIndex(5))
    }

    "parse mixed path starting with index" >> {
      CPath.parse("[3].aa.bb[*][5]") must_=
        CPath(CPathIndex(3), CPathField("aa"), CPathField("bb"), CPathArray, CPathIndex(5))
    }

    "parse mixed path starting with array" >> {
      CPath.parse("[*].aa[3].bb[5]") must_=
        CPath(CPathArray, CPathField("aa"), CPathIndex(3), CPathField("bb"), CPathIndex(5))
    }
  }

  "CPath manipulation" >> {

    "hasPrefixComponent" >> {
      "identity path" >> {
        "no component exists" >> {
          CPath.Identity.hasPrefixComponent(CPathField("aa")) must beFalse
        }
      }

      "nonidentity path" >> {
        val path: CPath = CPath.parse(".aa[4][*].bb.cc")

        "component exists at prefix" >> {
          path.hasPrefixComponent(CPathField("aa")) must beTrue
        }

        "component does not exist at prefix" >> {
          path.hasPrefixComponent(CPathIndex(4)) must beFalse
        }
      }
    }

    "hasPrefix" >> {
      "identity path" >> {
        "prefix is identity" >> {
          CPath.Identity.hasPrefix(CPath.Identity) must beTrue
        }

        "prefix is not identity" >> {
          CPath.Identity.hasPrefix(CPath.parse("foo")) must beFalse
        }
      }

      "nonidentity path" >> {
        val path: CPath = CPath.parse(".aa[4][*].bb.cc")

        "prefix exists" >> {
          path.hasPrefix(CPath.parse(".aa[4][*]")) must beTrue
        }

        "prefix does not exist" >> {
          path.hasPrefix(CPath.parse("[4]")) must beFalse
        }

        "prefix is identity" >> {
          path.hasPrefix(CPath.Identity) must beTrue
        }
      }
    }

    "dropPrefix" >> {
      "identity path" >> {
        "prefix is identity" >> {
          CPath.Identity.dropPrefix(CPath.Identity) must_= CPath.Identity.some
        }

        "prefix is not identity" >> {
          CPath.Identity.dropPrefix(CPath.parse("foo")) must beNone
        }
      }

      "nonidenity path" >> {
        val path: CPath = CPath.parse(".aa[4][*].bb.cc")

        "drop identity" >> {
          path.dropPrefix(CPath.Identity) must_= path.some
        }

        "drop existing prefix" >> {
          path.dropPrefix(CPath.parse(".aa[4][*]")) must_=
            CPath.parse(".bb.cc").some
        }

        "drop nonexisting prefix" >> {
          path.dropPrefix(CPath.parse(".[4]")) must beNone
        }
      }
    }

    "head" >> {
      "identity path" >> {
        CPath.Identity.head must beNone
      }

      "nonidentity path" >> {
        val path: CPath = CPath.parse(".aa[4][*].bb.cc")

        path.head.map(CPath(_)) must_= CPath.parse(".aa").some
      }
    }

    "tail" >> {
      "identity path" >> {
        CPath.Identity.tail must_= CPath.Identity
      }

      "nonidentity path" >> {
        val path: CPath = CPath.parse(".aa[4][*].bb.cc")

        path.tail must_= CPath.parse("[4][*].bb.cc")
      }
    }
  }
}
