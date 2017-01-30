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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.fp.ski.{ι, κ}
import quasar.physical.marklogic.xml.namespaces._
import quasar.std.DateLib.TemporalPart
import quasar.physical.marklogic.xquery._

import java.lang.SuppressWarnings

import eu.timepit.refined.auto._
import eu.timepit.refined.api.Refined
import scalaz.{Bind, Functor, Monad}
import scalaz.syntax.monad._

/** Functions related to qscript planning. */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object lib {
  import syntax._, expr._, axes.{attribute, child}
  import FunctionDecl._

  val qs = NamespaceDecl(qscriptNs)

  val dateFmt     = "[Y0001]-[M01]-[D01]".xs
  val dateTimeFmt = "[Y0001]-[M01]-[D01]T[H01]:[m01]:[s01].[f001]Z".xs
  val timeFmt     = "[H01]:[m01]:[s01].[f001]".xs

  private val epoch    = xs.dateTime("1970-01-01T00:00:00Z".xs)
  private val timeZero = xs.time("00:00:00-00:00".xs)

  // qscript:as-date($item as item()?) as xs:date?
  def asDate[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("as-date") map (_(
      $("item") as ST("item()?")
    ).as(ST("xs:date?")) { item =>
      if_(isCastable(item, ST("xs:date")))
      .then_ { xs.date(item) }
      .else_ {
        if_(isCastable(item, ST("xs:dateTime")))
        .then_ { xs.date(xs.dateTime(item)) }
        .else_ { emptySeq }
      }
    })

  // qscript:as-dateTime($item as item()?) as xs:dateTime?
  def asDateTime[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("as-dateTime") map (_(
      $("item") as ST("item()?")
    ).as(ST("xs:dateTime?")) { item =>
      if_(isCastable(item, ST("xs:dateTime")))
      .then_ { xs.dateTime(item) }
      .else_ {
        if_(isCastable(item, ST("xs:date")))
        .then_ { xs.dateTime(xs.date(item)) }
        .else_ { emptySeq }
      }
    })

  // qscript:as-map-key($item as item()?) as xs:string
  def asMapKey[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.name("as-map-key").qn[F] map { fname =>
      declare(fname)(
        $("item") as ST("item()?")
      ).as(ST("xs:string")) { item =>
        typeswitch(item)(
          $("a").as(ST("attribute()"))      return_ (a =>
            fn.stringJoin(mkSeq_(fn.string(fn.nodeName(a)), fn.string(a)), "_".xs)),

          $("e").as(ST("element()"))        return_ (e =>
            fn.stringJoin(mkSeq_(
              fn.string(fn.nodeName(e)),
              fn.map(fname :# 1, mkSeq_(e `/` attribute.node(), e `/` child.node()))
            ), "_".xs)),

          $("jarr").as(ST("array-node()"))  return_ (jarr =>
            fn.stringJoin(mkSeq_(
              fn.string(fn.nodeName(jarr)),
              fn.map(fname :# 1, jarr `/` child.node())
            ), "_".xs)),

          $("jobj").as(ST("object-node()")) return_ (jobj =>
            fn.stringJoin(mkSeq_(
              fn.string(fn.nodeName(jobj)),
              fn.map(fname :# 1, jobj `/` child.node())
            ), "_".xs)),

          ST("null-node()") return_ "null".xs
        ) default ($("i"), fn.string)
      }
    }

  // qscript:combine-apply($fns as (function(item()) as item())*) as function(item()) as item()*
  def combineApply[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("combine-apply") map (_(
      $("fns") as ST("(function(item()) as item())*")
    ).as(ST("function(item()) as item()*")) { fns =>
      val (f, x) = ($("f"), $("x"))
      func(x.render) { fn.map(func(f.render) { (~f) fnapply ~x }, fns) }
    })

  // qscript:combine-n($combiners as (function(item()*, item()) as item()*)*) as function(item()*, item()) as item()*
  def combineN[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("combine-n") map (_(
      $("combiners") as ST("(function(item()*, item()) as item()*)*")
    ).as(ST("function(item()*, item()) as item()*")) { combiners =>
      val (f, i, acc, x) = ($("f"), $("i"), $("acc"), $("x"))

      func(acc.render, x.render) {
        for_ (f at i in combiners) return_ {
          (~f) fnapply ((~acc)(~i), ~x)
        }
      }
    })

  // qscript:comp-eq($x as item()*, $y as item()*) as xs:boolean?
  def compEq[F[_]: Bind: PrologW]: F[FunctionDecl2] =
    mkComparisonFunction[F]("eq", _ eq _, fn.False)

  // qscript:comp-ne($x as item()*, $y as item()*) as xs:boolean?
  def compNe[F[_]: Bind: PrologW]: F[FunctionDecl2] =
    mkComparisonFunction[F]("ne", _ ne _, fn.True)

  // qscript:comp-lt($x as item()*, $y as item()*) as xs:boolean?
  def compLt[F[_]: Bind: PrologW]: F[FunctionDecl2] =
    mkComparisonFunction[F]("lt", _ lt _, emptySeq)

  // qscript:comp-le($x as item()*, $y as item()*) as xs:boolean?
  def compLe[F[_]: Bind: PrologW]: F[FunctionDecl2] =
    mkComparisonFunction[F]("le", _ le _, emptySeq)

  // qscript:comp-gt($x as item()*, $y as item()*) as xs:boolean?
  def compGt[F[_]: Bind: PrologW]: F[FunctionDecl2] =
    mkComparisonFunction[F]("gt", _ gt _, emptySeq)

  // qscript:comp-ge($x as item()*, $y as item()*) as xs:boolean?
  def compGe[F[_]: Bind: PrologW]: F[FunctionDecl2] =
    mkComparisonFunction[F]("ge", _ ge _, emptySeq)

  // qscript:concat($x as item()?, $y as item()?) as item()?
  def concat[F[_]: Bind: PrologW, T](implicit SP: StructuralPlanner[F, T]): F[FunctionDecl2] =
    qs.declare("concat") flatMap (_(
      $("x") as ST("item()?"),
      $("y") as ST("item()?")
    ).as(ST("item()?")) { (x: XQuery, y: XQuery) =>
      val (xArr, yArr) = ($("xArr"), $("yArr"))
      for {
        xt    <- SP.isArray(x)
        yt    <- SP.isArray(y)
        xcs   <- toCharSeq[F].apply(x) >>= (SP.seqToArray)
        ycs   <- toCharSeq[F].apply(y) >>= (SP.seqToArray)
        arrXY <- SP.arrayConcat(x, y)
        xArrY <- SP.arrayConcat(xcs, y)
        yArrX <- SP.arrayConcat(x, ycs)
      } yield {
        let_(
          xArr := xt,
          yArr := yt)
        .return_(
          if_(~xArr and ~yArr)
          .then_(arrXY)
          .else_(if_(~yArr)
          .then_(xArrY)
          .else_(if_(~xArr)
          .then_(yArrX)
          .else_(fn.concat(x, y)))))
      }
    })

  // qscript:delete-field($src as element()?, $field as xs:string) as element()?
  def deleteField[F[_]: Functor: PrologW]: F[FunctionDecl2] =
    qs.declare("delete-field") map (_(
      $("src")   as ST("element()?"),
      $("field") as ST("xs:string")
    ).as(ST("element()?")) { (src: XQuery, field: XQuery) =>
      val (s, n) = ($("s"), $("n"))
      fn.map(func(s.render) {
        element { fn.nodeName(~s) } {
          mkSeq_(
            ~s `/` attribute.node(),
            for_    (n in (~s `/` child.element()))
            .where_ (fn.string(fn.nodeName(~n)) ne field)
            .return_(~n))
        }
      }, src)
    })

  // qscript:element-left-shift($elt as element()) as item()*
  def elementLeftShift[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("element-left-shift") map (_(
      $("elt") as ST("element()?")
    ).as(ST("item()*")) { elt =>
      elt `/` child.element()
    })

  // qscript:isoyear-from-dateTime($dt as xs:dateTime?) as xs:integer
  def isoyearFromDateTime[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("isoyear-from-dateTime") map (_(
      $("dt") as ST("xs:dateTime?")
    ).as(ST("xs:integer?")) { dt: XQuery =>
      if_((fn.monthFromDateTime(dt) eq 1.xqy) and (xdmp.weekFromDate(xs.date(dt)) ge 52.xqy))
      .then_ { fn.yearFromDateTime(dt) - 1.xqy }
      .else_ {
        if_((fn.monthFromDateTime(dt) eq 12.xqy) and (xdmp.weekFromDate(xs.date(dt)) lt 52.xqy))
        .then_ { fn.yearFromDateTime(dt) + 1.xqy }
        .else_ { fn.yearFromDateTime(dt) }
      }
    })

  // qscript:identity($x as item()*) as item()*
  def identity[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("identity") map (_(
      $("x") as ST.Top
    ).as(ST.Top)(ι))

  // qscript:inc-avg($st as map:map, $x as item()*) as map:map
  def incAvg[F[_]: Bind: PrologW]: F[FunctionDecl2] =
    qs.declare("inc-avg") flatMap (_(
      $("st") as ST("map:map"),
      $("x")  as ST.Top
    ).as(ST("map:map")) { (st: XQuery, x: XQuery) =>
      val (c, a, y) = ($("c"), $("a"), $("y"))
      incAvgState[F].apply(~c, ~y) map { nextSt =>
        let_(
          c := (map.get(st, "cnt".xs) + 1.xqy),
          a := map.get(st, "avg".xs),
          y := (~a + mkSeq_(mkSeq_(x - (~a)) div ~c)))
        .return_(nextSt)
      }
    })

  // qscript:inc-avg-state($cnt as xs:integer, $avg as xs:double) as map:map
  def incAvgState[F[_]: Functor: PrologW]: F[FunctionDecl2] =
    qs.declare("inc-avg-state") map (_(
      $("cnt") as ST("xs:integer"),
      $("avg") as ST("xs:double")
    ).as(ST("map:map")) { (cnt, avg) =>
      map.new_(mkSeq_(
        map.entry("cnt".xs, cnt),
        map.entry("avg".xs, avg)))
    })

  // qscript:length($arrOrStr as item()?) as xs:integer?
  def length[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.name("length").qn[F] map { fname =>
      declare(fname)(
        $("arrOrStr") as ST("item()?")
      ).as(ST("xs:integer?")) { arrOrStr: XQuery =>
        val ct = $("ct")
        typeswitch(arrOrStr)(
          $("arr") as ST("element()") return_ { arr =>
            let_(ct := fn.count(arr `/` child.element())) return_ {
              if_(~ct gt 0.xqy)
              .then_ { ~ct }
              .else_ { fname(fn.string(arr)) }
            }
          },
          $("jarr") as ST("array-node()") return_ { jarr =>
            fn.count(jarr `/` child.node())
          },
          $("txt") as ST("text()")           return_ (t  => fname(fn.string(t))),
          $("qn")  as ST("xs:QName")         return_ (qn => fname(fn.string(qn))),
          $("ut")  as ST("xs:untypedAtomic") return_ (ut => fname(fn.string(ut))),
          $("str") as ST("xs:string")        return_ (fn.stringLength(_))
        ) default emptySeq
      }
    }

  // qscript:meta($item as item()?) as node()?
  def meta[F[_]: Bind: PrologW, T](implicit SP: StructuralPlanner[F, T]): F[FunctionDecl1] =
    qs.declare("meta") flatMap (_(
      $("item") as ST("item()?")
    ).as(ST("node()?")) { item: XQuery =>
      val nodeClause = $("n") as ST("node()") return_ (SP.nodeMetadata(_))
      nodeClause map (ec => typeswitch(item)(ec) default emptySeq)
    })

  // qscript:project-field($src as element()?, $field as xs:QName?) as item()*
  def projectField[F[_]: Functor: PrologW]: F[FunctionDecl2] =
    qs.declare("project-field") map (_(
      $("src")   as ST("element()?"),
      $("field") as ST("xs:QName?")
    ).as(ST.Top) { (src: XQuery, field: XQuery) =>
      val n = $("n")
      fn.filter(func(n.render)(fn.nodeName(~n) eq field), src `/` child.element())
    })

  // qscript:reduce-with(
  //   $initial  as function(item()*        ) as item()*,
  //   $combine  as function(item()*, item()) as item()*,
  //   $finalize as function(item()*        ) as item()*,
  //   $bucket   as function(item()*        ) as item(),
  //   $seq      as item()*
  // ) as item()*
  def reduceWith[F[_]: Bind: PrologW]: F[FunctionDecl5] =
    qs.declare("reduce-with") flatMap (_(
      $("initial")  as ST("function(item()*) as item()*"),
      $("combine")  as ST("function(item()*, item()) as item()*"),
      $("finalize") as ST("function(item()*) as item()*"),
      $("bucket")   as ST("function(item()*) as item()"),
      $("seq")      as ST("item()*")
    ).as(ST("item()*")) { (initial: XQuery, combine: XQuery, finalize: XQuery, bucket: XQuery, xs: XQuery) =>
      val (m, x, k, v, o) = ($("m"), $("x"), $("k"), $("v"), $("_"))

      asMapKey[F].apply(bucket fnapply (~x)) map { theKey =>
        let_(
          m := map.map(),
          o := for_(
                 x in xs)
               .let_(
                 k := theKey,
                 v := if_(map.contains(~m, ~k))
                      .then_(combine fnapply (map.get(~m, ~k), ~x))
                      .else_(initial fnapply (~x)),
                 o := map.put(~m, ~k, ~v))
               .return_(emptySeq))
        .return_ {
          for_ (k in map.keys(~m)) .return_ {
            finalize fnapply (map.get(~m, ~k))
          }
        }
      }
    })

  // NB: Copied from StringLib.safeSubstring
  // qscript:safe-substring($str as xs:string?, $start as xs:integer?, $length as xs:integer?) as xs:string?
  def safeSubstring[F[_]: Functor: PrologW]: F[FunctionDecl3] =
    qs.declare("safe-substring") map (_(
      $("str")    as ST("xs:string?"),
      $("start")  as ST("xs:integer?"),
      $("length") as ST("xs:integer?")
    ).as(ST("xs:string?")) { (str: XQuery, start: XQuery, length: XQuery) =>
      val l = $("l")
      let_(l := fn.stringLength(str)) return_ {
        if_((start lt 1.xqy) or (start gt ~l))
        .then_ { "".xs }
        .else_(if_(length lt 0.xqy)
        .then_ { fn.substring(str, start, Some(~l)) }
        .else_ { fn.substring(str, start, Some(length)) })
      }
    })

  // qscript:seconds-since-epoch($dt as xs:dateTime?) as xs:double?
  def secondsSinceEpoch[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("seconds-since-epoch") map (_(
      $("dt") as ST("xs:dateTime?")
    ).as(ST("xs:double?")) { dt =>
      mkSeq_(dt - epoch) div xs.dayTimeDuration("PT1S".xs)
    })

  // qscript:timestamp-to-dateTime($millis as xs:integer?) as xs:dateTime?
  def timestampToDateTime[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("timestamp-to-dateTime") map (_(
      $("millis") as ST("xs:integer?")
    ).as(ST("xs:dateTime?")) { millis =>
      val prefix = $("prefix")
      let_(
        prefix := if_(millis lt 0.xqy) then_ "-PT".xs else_ "PT".xs)
      .return_(
        epoch + xs.dayTimeDuration(fn.concat(~prefix, xs.string(fn.abs(millis) div 1000.xqy), "S".xs)))
    })

  // qscript:start-of-day($item as item()?) as xs:$dateTime?
  def startOfDay[F[_]: Monad: PrologW]: F[FunctionDecl1] =
    qs.declare("start-of-day") flatMap (_(
      $("item") as ST("item()?")
    ).as(ST("item()?")) { item =>
      temporalTrunc[F](TemporalPart.Day).apply(item) map (tt =>
        typeswitch(item)(
          $("dateTime") as ST("xs:dateTime") return_ κ(tt),
          $("date") as ST("xs:date") return_ κ(xs.dateTime(item, timeZero))
        ) default emptySeq)
    })

  // qscript:temporal-trunc($item as item()?) as item()?
  def temporalTrunc[F[_]: Functor: PrologW](part: TemporalPart): F[FunctionDecl1] =
    qs.declare("temporal-trunc") map (_(
      $("item") as ST("item()?")
    ).as(ST("item()?")) { item =>
      import TemporalPart._

      def dt(
        h: XQuery, m: XQuery, d: XQuery, hr: XQuery, min: XQuery, sec: XQuery, ms: XQuery
      ): XQuery =
        fn.concat(
          xdmp.formatNumber(h, "0001".xs), "-".xs,
          xdmp.formatNumber(m,   "01".xs), "-".xs,
          xdmp.formatNumber(d,   "01".xs), "T".xs,
          xdmp.formatNumber(hr,  "01".xs), ":".xs,
          xdmp.formatNumber(min, "01".xs), ":".xs,
          xdmp.formatNumber(sec, "01".xs), ".".xs,
          xdmp.formatNumber(ms, "001".xs), "Z".xs)

      def fmtDateTime(x: XQuery): XQuery = fn.formatDateTime(x, dateTimeFmt)

      val (dateTime, wrap) = ($("dateTime"), $("wrap"))
      let_(
        dateTime := typeswitch(item)(
          $("dateTime") as ST("xs:dateTime") return_ (ι),
          $("date") as ST("xs:date") return_ (xs.dateTime(_, timeZero)),
          $("time") as ST("xs:time") return_ (xs.dateTime(xs.date("0001-01-01".xs), _))
        ) default emptySeq,
        wrap := typeswitch(item)(
          $("dateTime") as ST("xs:dateTime") return_ (κ(
            xdmp.function(xs.QName("xs:dateTime".xs)))),
          $("date")     as ST("xs:date") return_ (κ(
            func("$s")(xs.date(xdmp.parseDateTime(dateFmt, "$s".xqy))))),
          $("time")     as ST("xs:time") return_ (κ(
            func("$s")(xs.time(xdmp.parseDateTime(dateTimeFmt, "$s".xqy)))))
        ) default emptySeq
      ) return_ {

      xdmp.apply(~wrap, part match {
        case Century =>
          dt(
            (fn.yearFromDateTime(~dateTime) idiv 100.xqy) * 100.xqy, 1.xqy, 1.xqy,
            0.xqy, 0.xqy, 0.xqy, 0.xqy)
        case Day =>
          dt(
            fn.yearFromDateTime(~dateTime),
            fn.monthFromDateTime(~dateTime),
            fn.dayFromDateTime(~dateTime),
            0.xqy, 0.xqy, 0.xqy, 0.xqy)
        case Decade =>
          dt(
            (fn.yearFromDateTime(~dateTime) idiv 10.xqy) * 10.xqy, 1.xqy, 1.xqy,
            0.xqy, 0.xqy, 0.xqy, 0.xqy)
        case Hour =>
          dt(
            fn.yearFromDateTime(~dateTime), fn.monthFromDateTime(~dateTime), fn.dayFromDateTime(~dateTime),
            fn.hoursFromDateTime(~dateTime), 0.xqy, 0.xqy, 0.xqy)
        case Microsecond => fmtDateTime(~dateTime)
        case Millennium =>
          dt(
            (fn.yearFromDateTime(~dateTime) idiv 1000.xqy) * 1000.xqy, 1.xqy, 1.xqy,
            0.xqy, 0.xqy, 0.xqy, 0.xqy)
        case Millisecond => fmtDateTime(~dateTime)
        case Minute =>
          dt(
            fn.yearFromDateTime(~dateTime),
            fn.monthFromDateTime(~dateTime),
            fn.dayFromDateTime(~dateTime),
            fn.hoursFromDateTime(~dateTime), fn.minutesFromDateTime(~dateTime), 0.xqy, 0.xqy)
        case Month =>
          dt(
            fn.yearFromDateTime(~dateTime), fn.monthFromDateTime(~dateTime), 1.xqy,
            0.xqy, 0.xqy, 0.xqy, 0.xqy)
        case Quarter =>
          dt(
            fn.yearFromDateTime(~dateTime),
            mkSeq_(mkSeq_(xdmp.quarterFromDate(xs.date(~dateTime)) - 1.xqy) * 3.xqy) + 1.xqy,
            1.xqy,
            0.xqy, 0.xqy, 0.xqy, 0.xqy)
        case Second =>
          dt(
            fn.yearFromDateTime(~dateTime), fn.monthFromDateTime(~dateTime), fn.dayFromDateTime(~dateTime),
            fn.hoursFromDateTime(~dateTime), fn.minutesFromDateTime(~dateTime),
            math.floor(fn.secondsFromDateTime(~dateTime)), 0.xqy)
        case Week =>
          fmtDateTime(xs.dateTime(
            xs.date(~dateTime) - xs.dayTimeDuration(
              fn.concat("P".xs, xs.string(xdmp.weekdayFromDate(xs.date(~dateTime)) - 1.xqy), "D".xs))))
        case Year =>
          dt(
            fn.yearFromDateTime(~dateTime), 1.xqy, 1.xqy,
            0.xqy, 0.xqy, 0.xqy, 0.xqy)
      })
    }})

  // qscript:timezone-offset-seconds($dt as xs:dateTime?) as xs:integer?
  def timezoneOffsetSeconds[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("timezone-offset-seconds") map (_(
      $("dt") as ST("xs:dateTime?")
    ).as(ST("xs:integer?")) { dt =>
      fn.timezoneFromDateTime(dt) div xs.dayTimeDuration("PT1S".xs)
    })

  // qscript:to-char-seq($s as xs:string?) as xs:string*
  def toCharSeq[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("to-char-seq") map (_(
      $("s") as ST("xs:string?")
    ).as(ST("xs:string*")) { s: XQuery =>
      fn.map("fn:codepoints-to-string#1".xqy, fn.stringToCodepoints(s))
    })

  // qscript:zip-apply($fns as (function(item()*) as item()*)*) as function(item()*) as item()*
  def zipApply[F[_]: Functor: PrologW]: F[FunctionDecl1] =
    qs.declare("zip-apply") map (_(
      $("fns") as ST("(function(item()*) as item()*)*")
    ).as(ST("function(item()*) as item()*")) { fns =>
      val (f, i, x) = ($("f"), $("i"), $("x"))

      func(x.render) {
        for_ (f at i in fns) return_ {
          (~f) fnapply ((~x)(~i))
        }
      }
    })

  ////

  private def mkComparisonFunction[F[_]: Bind: PrologW](opName: String, op: (XQuery, XQuery) => XQuery, recover: XQuery): F[FunctionDecl2] =
    qs.declare(Refined.unsafeApply(s"comp-$opName")) flatMap (_(
      $("x") as ST.Top,
      $("y") as ST.Top
    ).as(ST("xs:boolean?")) { (x: XQuery, y: XQuery) =>
      (asDateTime[F].apply(x) |@| asDateTime[F].apply(y))((xdt, ydt) =>
        try_ {
          if_(
            isCastable(x, ST("xs:date"))     or
            isCastable(y, ST("xs:date"))     or
            isCastable(x, ST("xs:dateTime")) or
            isCastable(y, ST("xs:dateTime")))
          .then_ { op(xdt, ydt) }
          .else_ { op(x, y) }
        } .catch_($("_")) { _ =>
          recover
        })
    })
}
