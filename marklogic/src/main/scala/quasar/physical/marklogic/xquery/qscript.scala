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
import quasar.fp.ski.ι
import quasar.physical.marklogic.xml.namespaces._

import java.lang.SuppressWarnings

import eu.timepit.refined.auto._
import scalaz.IList
import scalaz.syntax.monad._

/** Functions related to qscript planning. */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object qscript {
  import syntax._, expr._, axes.{attribute, child}
  import FunctionDecl.{FunctionDecl1, FunctionDecl2, FunctionDecl5}

  val qs     = NamespaceDecl(qscriptNs)
  val errorN = qs name qscriptError.local

  private val epoch = xs.dateTime("1970-01-01T00:00:00Z".xs)

  // qscript:as-date($item as item()) as xs:date?
  def asDate[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("as-date") map (_(
      $("item") as ST("item()")
    ).as(ST("xs:date?")) { item =>
      if_(isCastable(item, ST("xs:date")))
      .then_ { xs.date(item) }
      .else_ {
        if_(isCastable(item, ST("xs:dateTime")))
        .then_ { xs.date(xs.dateTime(item)) }
        .else_ { emptySeq }
      }
    })

  // qscript:as-map-key($item as item()) as xs:string
  def asMapKey[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("as-map-key").qn[F] map { fname =>
      declare(fname)(
        $("item") as ST("item()")
      ).as(ST("xs:string")) { item =>
        typeswitch(item)(
          ($("a") as ST("attribute()")) return_ (a =>
            fn.stringJoin(mkSeq_(fn.string(fn.nodeName(a)), fn.string(a)), "_".xs)),

          ($("e") as ST("element()"))   return_ (e =>
            fn.stringJoin(mkSeq_(
              fn.string(fn.nodeName(e)),
              fn.map(fname :# 1, mkSeq_(e `/` attribute.node(), e `/` child.node()))
            ), "_".xs))

        ) default ($("i"), fn.string)
      }
    }

  // qscript:combine-apply($fns as (function(item()) as item())*) as function(item()) as item()*
  def combineApply[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("combine-apply") map (_(
      $("fns") as ST("(function(item()) as item())*")
    ).as(ST("function(item()) as item()*")) { fns =>
      val (f, x) = ("$f", "$x")
      func(x) { fn.map(func(f) { f.xqy fnapply x.xqy }, fns) }
    })

  // qscript:combine-n($combiners as (function(item()*, item()) as item()*)*) as function(item()*, item()) as item()*
  def combineN[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("combine-n") map (_(
      $("combiners") as ST("(function(item()*, item()) as item()*)*")
    ).as(ST("function(item()*, item()) as item()*")) { combiners =>
      val (len, acc, i, x) = ("$len", "$acc", "$i", "$x")

      let_ (len -> fn.count(combiners)) return_ {
        func(acc, x) {
          for_ (i -> (1.xqy to len.xqy)) return_ {
            combiners(i.xqy) fnapply (acc.xqy(i.xqy), x.xqy)
          }
        }
      }
    })

  // qscript:delete-field($src as element(), $field as xs:QName) as element()
  def deleteField[F[_]: PrologW]: F[FunctionDecl2] =
    qs.declare("delete-field") map (_(
      $("src")   as ST("element()"),
      $("field") as ST("xs:QName")
    ).as(ST("element()")) { (src: XQuery, field: XQuery) =>
      val n = "$n"
      element { fn.nodeName(src) } {
        for_    (n -> (src `/` child.element()))
        .where_ (fn.nodeName(n.xqy) ne field)
        .return_(n.xqy)
      }
    })

  // qscript:element-dup-keys($elt as element()) as element()
  def elementDupKeys[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("element-dup-keys") map (_(
      $("elt") as ST("element()")
    ).as(ST("element()")) { elt: XQuery =>
      val (c, n) = ("$c", "$n")
      element { fn.nodeName(elt) } {
        for_    (c -> (elt `/` child.element()))
        .let_   (n -> fn.nodeName(c.xqy))
        .return_(element { n.xqy } { n.xqy })
      }
    })

  // qscript:element-left-shift($elt as element()) as item()*
  def elementLeftShift[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("element-left-shift") flatMap (_(
      $("elt") as ST("element()")
    ).as(ST("item()*")) { elt =>
      (ejson.arrayEltN.qn[F] |@| ejson.isArray[F].apply(elt))((aelt, eltIsArray) =>
        if_ (eltIsArray)
        .then_ { elt `/` child(aelt)  }
        .else_ { elt `/` child.node() })
    })

  // qscript:identity($x as item()*) as item()*
  def identity[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("identity") map (_(
      $("x") as ST.Top
    ).as(ST.Top)(ι))

  // qscript:inc-avg($st as map:map, $x as item()*) as map:map
  def incAvg[F[_]: PrologW]: F[FunctionDecl2] =
    qs.declare("inc-avg") flatMap (_(
      $("st") as ST("map:map"),
      $("x")  as ST.Top
    ).as(ST("map:map")) { (st: XQuery, x: XQuery) =>
      val (c, a, y) = ("$c", "$a", "$y")
      incAvgState[F].apply(c.xqy, y.xqy) map { nextSt =>
        let_(
          c -> (map.get(st, "cnt".xs) + 1.xqy),
          a -> map.get(st, "avg".xs),
          y -> (a.xqy + mkSeq_(mkSeq_(x - a.xqy) div c.xqy)))
        .return_(nextSt)
      }
    })

  // qscript:inc-avg-state($cnt as xs:integer, $avg as xs:double) as map:map
  def incAvgState[F[_]: PrologW]: F[FunctionDecl2] =
    qs.declare("inc-avg-state") map (_(
      $("cnt") as ST("xs:integer"),
      $("avg") as ST("xs:double")
    ).as(ST("map:map")) { (cnt, avg) =>
      map.new_(IList(
        map.entry("cnt".xs, cnt),
        map.entry("avg".xs, avg)))
    })

  def isDocumentNode(node: XQuery): XQuery =
    xdmp.nodeKind(node) === "document".xs

  def length[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("length").qn[F] map { fname =>
      declare(fname)(
        $("arrOrStr") as ST("item()")
      ).as(ST("xs:integer?")) { arrOrStr: XQuery =>
        typeswitch(arrOrStr)(
          $("arr") as ST("element()") return_ { arr =>
            let_("$ct" -> fn.count(arr `/` child.element())) return_ {
              if_("$ct".xqy gt 0.xqy)
              .then_ { "$ct".xqy }
              .else_ { fname(fn.string(arr)) }
            }
          },
          $("qn")  as ST("xs:QName")  return_ (qn => fname(fn.string(qn))),
          $("str") as ST("xs:string") return_ (fn.stringLength(_))
        ) default emptySeq
      }
    }

  // qscript:project-field($src as element(), $field as xs:QName) as item()*
  def projectField[F[_]: PrologW]: F[FunctionDecl2] =
    qs.declare("project-field") map (_(
      $("src")   as ST("element()"),
      $("field") as ST("xs:QName")
    ).as(ST.Top) { (src: XQuery, field: XQuery) =>
      fn.filter(func("$n")(fn.nodeName("$n".xqy) eq field), src `/` child.element())
    })

  def qError[F[_]: PrologW](desc: XQuery, errObj: Option[XQuery] = None): F[XQuery] =
    errorN.xqy[F] map (err => fn.error(err, Some(desc), errObj))

  // qscript:reduce-with(
  //   $initial  as function(item()*        ) as item()*,
  //   $combine  as function(item()*, item()) as item()*,
  //   $finalize as function(item()*        ) as item()*,
  //   $bucket   as function(item()*        ) as item(),
  //   $seq      as item()*
  // ) as item()*
  def reduceWith[F[_]: PrologW]: F[FunctionDecl5] =
    qs.declare("reduce-with") flatMap (_(
      $("initial")  as ST("function(item()*) as item()*"),
      $("combine")  as ST("function(item()*, item()) as item()*"),
      $("finalize") as ST("function(item()*) as item()*"),
      $("bucket")   as ST("function(item()*) as item()"),
      $("seq")      as ST("item()*")
    ).as(ST("item()*")) { (initial: XQuery, combine: XQuery, finalize: XQuery, bucket: XQuery, xs: XQuery) =>
      val (m, x, k, v, o) = ("$m", "$x", "$k", "$v", "$_")

      asMapKey[F].apply(bucket fnapply (x.xqy)) map { theKey =>
        let_(
          m -> map.map(),
          o -> for_ (x -> xs) .let_ (
                 k -> theKey,
                 v -> if_(map.contains(m.xqy, k.xqy))
                      .then_(combine fnapply (map.get(m.xqy, k.xqy), x.xqy))
                      .else_(initial fnapply (x.xqy)),
                 o -> map.put(m.xqy, k.xqy, v.xqy)
               ) .return_ (emptySeq)
        ) .return_ {
          for_ (k -> map.keys(m.xqy)) .return_ {
            finalize fnapply (map.get(m.xqy, k.xqy))
          }
        }
      }
    })

  // qscript:seconds-since-epoch($dt as xs:dateTime) as xs:double
  def secondsSinceEpoch[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("seconds-since-epoch") map (_(
      $("dt") as ST("xs:dateTime")
    ).as(ST("xs:double")) { dt =>
      mkSeq_(dt - epoch) div xs.dayTimeDuration("PT1S".xs)
    })

  // qscript:shifted-read($uri as xs:string, $include-id as xs:boolean) as element()*
  def shiftedRead[F[_]: NameGenerator: PrologW]: F[FunctionDecl2] =
    qs.declare("shifted-read") flatMap (_(
      $("uri")        as ST("xs:string"),
      $("include-id") as ST("xs:boolean")
    ).as(ST(s"element()*")) { (uri: XQuery, includeId: XQuery) =>
      for {
        d     <- freshVar[F]
        c     <- freshVar[F]
        b     <- freshVar[F]
        xform <- json.transformFromJson[F](c.xqy)
        incId <- ejson.seqToArray_[F](mkSeq_(
                   fn.concat("_".xs, xdmp.hmacSha1("quasar".xs, fn.documentUri(d.xqy))),
                   b.xqy))
      } yield {
        for_(d -> cts.search(fn.doc(), cts.directoryQuery(uri, "1".xs)))
          .let_(
            c -> d.xqy `/` child.node(),
            b -> (if_ (json.isObject(c.xqy)) then_ xform else_ c.xqy))
          .return_ {
            if_ (includeId) then_ { incId } else_ { b.xqy }
          }
      }
    })

  // qscript:timestamp-to-dateTime($millis as xs:integer) as xs:dateTime
  def timestampToDateTime[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("timestamp-to-dateTime") map (_(
      $("millis") as ST("xs:integer")
    ).as(ST("xs:dateTime")) { millis =>
      epoch + xs.dayTimeDuration(fn.concat("PT".xs, xs.string(millis div 1000.xqy), "S".xs))
    })

  // qscript:timezone-offset-seconds($dt as xs:dateTime) as xs:integer
  def timezoneOffsetSeconds[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("timezone-offset-seconds") map (_(
      $("dt") as ST("xs:dateTime")
    ).as(ST("xs:integer")) { dt =>
      fn.timezoneFromDateTime(dt) div xs.dayTimeDuration("PT1S".xs)
    })

  // qscript:zip-apply($fns as (function(item()*) as item()*)*) as function(item()*) as item()*
  def zipApply[F[_]: PrologW]: F[FunctionDecl1] =
    qs.declare("zip-apply") map (_(
      $("fns") as ST("(function(item()*) as item()*)*")
    ).as(ST("function(item()*) as item()*")) { fns =>
      val (len, i, x) = ("$len", "$i", "$x")

      let_ (len -> fn.count(fns)) return_ {
        func(x) {
          for_ (i -> (1.xqy to len.xqy)) return_ {
            fns(i.xqy) fnapply (x.xqy(i.xqy))
          }
        }
      }
    })

  // qscript:zip-map-element-keys($elt as element()) as element()
  def zipMapElementKeys[F[_]: NameGenerator: PrologW]: F[FunctionDecl1] =
    qs.declare("zip-map-element-keys") flatMap (_(
      $("elt") as ST("element()")
    ).as(ST(s"element()")) { elt =>
      val (c, n) = ("$child", "$name")

      for {
        kelt    <- ejson.mkArrayElt[F](n.xqy)
        velt    <- ejson.mkArrayElt[F](c.xqy)
        kvArr   <- ejson.mkArray_[F](mkSeq_(kelt, velt))
        kvEnt   <- ejson.renameOrWrap[F] apply (n.xqy, kvArr)
        entries =  for_ (c -> elt `/` child.element())
                   .let_(n -> fn.nodeName(c.xqy))
                   .return_(kvEnt)
        zMap    <- ejson.mkObject[F] apply entries
      } yield zMap
    })
}
