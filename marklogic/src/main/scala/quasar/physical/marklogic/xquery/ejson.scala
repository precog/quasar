/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.xquery

import quasar.Predef._
import quasar.NameGenerator

import eu.timepit.refined.auto._
import scalaz.syntax.monad._

object ejson {
  import syntax._, expr.{element, for_, func}, axes._
  import FunctionDecl.{FunctionDecl1, FunctionDecl2}

  val ejs = namespace("ejson", "http://quasar-analytics.org/ejson")

  val arrayN    = ejs name "array"
  val arrayEltN = ejs name "array-element"
  val mapN      = ejs name "map"
  val mapEntryN = ejs name "map-entry"
  val mapKeyN   = ejs name "map-key"
  val mapValueN = ejs name "map-value"

  // ejson:array-concat($arr1 as element(ejson:array), $arr2 as element(ejson:array)) as element(ejson:array)
  def arrayConcat[F[_]: NameGenerator: PrologW]: F[FunctionDecl2] =
    (ejs.name("array-concat").qn[F] |@| arrayN.qn |@| arrayEltN.qn) { (fname, aname, aelt) =>
      declare(fname)(
        $("arr1") as SequenceType(s"element($aname)"),
        $("arr2") as SequenceType(s"element($aname)")
      ).as(SequenceType(s"element($aname)")) { (arr1: XQuery, arr2: XQuery) =>
        mkArray[F] apply mkSeq_(arr1 `/` child(aelt), arr2 `/` child(aelt))
      }
    }.join

  // ejson:array-left-shift($arr as element(ejson:array)) as item()*
  def arrayLeftShift[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("array-left-shift").qn[F] |@| arrayN.qn |@| arrayEltN.qn) { (fname, aname, aelt) =>
      declare(fname)(
        $("arr") as SequenceType(s"element($aname)")
      ).as(SequenceType("item()*")) { arr =>
        arr `/` child(aelt) `/` child.node()
      }
    }

  // ejson:is-array($node as node()) as xs:boolean
  def isArray[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("is-array").qn[F] |@| arrayN.xqy) { (fname, aname) =>
      declare(fname)(
        $("node") as SequenceType("node()")
      ).as(SequenceType("xs:boolean")) { node =>
        fn.nodeName(node) === aname
      }
    }

  // ejson:is-map($node as node()) as xs:boolean
  def isMap[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("is-map").qn[F] |@| mapN.xqy) { (fname, mname) =>
      declare(fname)(
        $("node") as SequenceType("node()")
      ).as(SequenceType("xs:boolean")) { node =>
        fn.nodeName(node) === mname
      }
    }

  // ejson:map-left-shift($map as element(ejson:map)) as item()*
  def mapLeftShift[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("map-left-shift").qn[F] |@| mapN.qn |@| mapEntryN.qn |@| mapValueN.qn) {
      (fname, mname, mentry, mval) =>

      declare(fname)(
        $("map") as SequenceType(s"element($mname)")
      ).as(SequenceType("item()*")) { m =>
        m `/` child(mentry) `/` child(mval) `/` child.node()
      }
    }

  // ejson:map-lookup($map as element(ejson:map), $key as item()*) as item()*
  def mapLookup[F[_]: NameGenerator: PrologW]: F[FunctionDecl2] =
    (ejs.name("map-lookup").qn[F] |@| mapN.qn |@| mapEntryN.qn |@| mapKeyN.xqy |@| mapValueN.qn) {
      (fname, mname, mentry, mkey, mval) =>

      declare(fname)(
        $("map") as SequenceType(s"element($mname)"),
        $("key") as SequenceType.Top
      ).as(SequenceType.Top) { (m, k) =>
        m `/` child(mentry)(mkey === k) `/` child(mval) `/` child.node()
      }
    }

  // ejson:make-array($elements as element(ejson:array-element)*) as element(ejson:array)
  def mkArray[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("make-array").qn[F] |@| arrayN.qn |@| arrayN.xs |@| arrayEltN.qn) { (fname, aname, arrxs, aelt) =>
      declare(fname)(
        $("elements") as SequenceType(s"element($aelt)*")
      ).as(SequenceType(s"element($aname)")) { elts =>
        element { arrxs } { elts }
      }
    }

  // ejson:make-array-element($value as item()*) as element(ejson:array-element)
  def mkArrayElt[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("make-array-element").qn[F] |@| arrayEltN.qn |@| arrayEltN.xs) { (fname, aelt, aeltxs) =>
      declare(fname)(
        $("value") as SequenceType.Top
      ).as(SequenceType(s"element($aelt)")) { value =>
        element { aeltxs } { value }
      }
    }

  // ejson:make-map($entries as element(ejson:map-entry)*) as element(ejson:map)
  def mkMap[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("make-map").qn[F] |@| mapN.qn |@| mapN.xs |@| mapEntryN.qn) { (fname, mname, mapxs, ment) =>
      declare(fname)(
        $("entries") as SequenceType(s"element($ment)*")
      ).as(SequenceType(s"element($mname)")) { entries =>
        element { mapxs } { entries }
      }
    }

  // ejson:make-map-entry($key as item()*, $value as item()*) as element(ejson:map-entry)
  def mkMapEntry[F[_]: PrologW]: F[FunctionDecl2] =
    (ejs.name("make-map-entry").qn[F] |@| mapEntryN.qn |@| mapEntryN.xs |@| mapKeyN.xs |@| mapValueN.xs) {
      (fname, mentry, mentryxs, mkeyxs, mvalxs) =>

      declare(fname)(
        $("key") as SequenceType.Top,
        $("value") as SequenceType.Top
      ).as(SequenceType(s"element($mentry)")) { (key, value) =>
        element { mentryxs } {
          mkSeq_(
            element { mkeyxs } { key },
            element { mvalxs } { value }
          )
        }
      }
    }

  // ejson:seq-to-array($items as item()*) as element(ejson:array)
  def seqToArray[F[_]: NameGenerator: PrologW]: F[FunctionDecl1] =
    (ejs.name("seq-to-array").qn[F] |@| arrayN.qn) { (fname, aname) =>
      declare(fname)(
        $("items") as SequenceType("item()*")
      ).as(SequenceType(s"element($aname)")) { items: XQuery =>
        val x = "$x"

        for {
          arrElt <- mkArrayElt[F] apply x.xqy
          arr    <- mkArray[F] apply fn.map(func(x) { arrElt }, items)
        } yield arr
      }
    }.join

  // ejson:singleton-array($item as item()*) as element(ejson:array)
  def singletonArray[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("singleton-array").qn[F] |@| arrayN.qn) { (fname, aname) =>
      declare(fname)(
        $("item") as SequenceType.Top
      ).as(SequenceType(s"element($aname)")) { item: XQuery =>
        mkArrayElt[F].apply(item) flatMap (xqy => mkArray[F].apply(xqy))
      }
    }.join

  // ejson:singleton-map($key as item()*, $value as item()*) as element(ejson:map)
  def singletonMap[F[_]: PrologW]: F[FunctionDecl2] =
    (ejs.name("singleton-map").qn[F] |@| mapN.qn) { (fname, mname) =>
      declare(fname)(
        $("key") as SequenceType.Top,
        $("value") as SequenceType.Top
      ).as(SequenceType(s"element($mname)")) { (key: XQuery, value: XQuery) =>
        mkMapEntry[F].apply(key, value) flatMap (xqy => mkMap[F].apply(xqy))
      }
    }.join

  // ejson:zip-map-keys($map as element(ejson:map)) as element(ejson:map)
  def zipMapKeys[F[_]: NameGenerator: PrologW]: F[FunctionDecl1] =
    (ejs.name("zip-map-keys").qn[F] |@| mapN.qn |@| mapEntryN.qn |@| mapKeyN.qn |@| mapValueN.qn) {
      (fname, mname, mentry, mkey, mval) =>

      val mapType = SequenceType(s"element($mname)")
      val entry = "$entry"
      val key   = "$key"
      val value = "$value"

      declare(fname)(
        $("map") as mapType
      ).as(mapType) { map: XQuery =>
        for {
          kelt <- mkArrayElt[F] apply key.xqy
          velt <- mkArrayElt[F] apply value.xqy
          arr  <- mkArray[F] apply mkSeq_(kelt, velt)
          ment <- mkMapEntry[F] apply (key.xqy, arr)
          ents =  for_(
                    entry -> map `/` child(mentry))
                  .let_(
                    key   -> entry.xqy `/` child(mkey) `/` child.node(),
                    value -> entry.xqy `/` child(mval) `/` child.node())
                  .return_(ment)
          zmap <- mkMap[F] apply ents
        } yield zmap
      }
    }.join
}
