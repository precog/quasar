/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.niflheim

import java.io._
import quasar.blueeyes.json._
import quasar.precog.common._

import quasar.precog.BitSet

import org.specs2.mutable._
import org.specs2.ScalaCheck

abstract class cleanup(f: File) extends After {
  def after = {
    try {
      f.delete()
    } catch {
      case _: Exception => ()
    }
  }
}

object RawHandlerSpecs extends Specification with ScalaCheck {
  def blockid = 999
  def tempfile(): File = {
    val f = File.createTempFile("niflheim", ".rawlog")
    f.delete()
    f
  }

  implicit val ordering = scala.math.Ordering.by[(String, CType), String](_.toString)

  def slurp(f: File): String =
    new java.util.Scanner(f, "UTF-8").useDelimiter("\0").next()

  def json(s: String): Seq[JValue] =
    JParser.parseManyFromString(s).valueOr(throw _)

  def bitset(ns: Int*) = {
    val bs = new BitSet
    ns.foreach(bs.set)
    bs
  }

  def decs(ns: Double*): Array[BigDecimal] = ns.map(BigDecimal(_)).toArray

  def makeps(f: File) = new PrintStream(new FileOutputStream(f, true), false, "UTF-8")

  "raw handler" should {

    /**
     * Just tests the most basic functionality: generating snapshots
     * from repeated writes. This is the core of what RawHandler will
     * normally be doing.
     */
    val tmp1 = tempfile()
    "generate snapshots from writes" in new cleanup(tmp1) {
      val h = RawHandler.empty(blockid, tmp1)
      h.length must_== 0

      h.snapshot(None).segments.length must_== 0
      h.snapshotRef(None).segments.length must_== 0

      h.write(16, json("""
        {"a": 123, "b": true, "c": false, "d": null, "e": "cat", "f": {"aa": 11.0, "bb": 22.0}}
        {"a": 9999.0, "b": "xyz", "arr": [1,2,3]}
        {"a": 0, "b": false, "c": 0.0, "y": [], "z": {}}
        """))

      h.length must_== 3

      val segs1 = h.snapshot(None).segments
      segs1 must contain(ArraySegment(blockid, CPath(".a"), CNum, bitset(0, 1, 2), decs(123, 9999.0, 0)))
      segs1 must contain(BooleanSegment(blockid, CPath(".b"), bitset(0, 2), bitset(0), 3))

      val segs1R = h.snapshotRef(None).segments
      segs1R mustEqual segs1

      h.write(17, json("""
        999
        123.0
        "cat"
        [1,2,3.0, "four"]
        {"b": true}
        """))

      h.length must_== 8

      val segs2 = h.snapshot(None).segments
      segs2 must contain(BooleanSegment(blockid, CPath(".b"), bitset(0, 2, 7), bitset(0, 7), 8))

      val segs2R = h.snapshotRef(None).segments
      segs2R mustEqual segs2
    }

    /**
     * Test log file writing/reading.
     *
     * The RawHandler should support being stopped and then recreated
     * on its previous log. This test doesn't test the format itself,
     * but just whether the data written can be read back in correctly.
     */
    val tmp2 = tempfile()
    "correctly read log files" in new cleanup(tmp2) {
      val h1 = RawHandler.empty(blockid, tmp2)

      val js = """
{"a": 123, "b": true, "c": false, "d": null, "e": "cat", "f": {"aa": 11.0, "bb": 22.0}}
{"a": 9999.0, "b": "xyz", "arr": [1,2,3]}
{"a": 0, "b": false, "c": 0.0, "y": [], "z": {}}
""".trim

      h1.write(18, json(js))
      val s1 = h1.snapshot(None).segments
      val s1R = h1.snapshotRef(None).segments
      h1.close()

      val (h2, events, true) = RawHandler.load(blockid, tmp2)
      val s2 = h2.snapshot(None).segments
      val s2R = h2.snapshotRef(None).segments

      implicit val ord: Ordering[Segment] = Ordering.by[Segment, String](_.toString)

      events.toSet must_== Set(18)
      s2.sorted must_== s1.sorted
      s2R.sorted must_== s1R.sorted
    }


    /**
     * Test recovery from corrupted rawlog file.
     *
     * In this case the log is missing an "##end 101\n" stanza. The
     * first load() should clean up the file and report the error.
     * Future loads should load the same data without complaint
     * (indicating the file has been cleaned).
     */
    val tmp3 = tempfile()
    "recover from errors" in new cleanup(tmp3) {

      // write a valid event, then a mal-formed event
      val ps = makeps(tmp3)
      RawLoader.writeHeader(ps, blockid)
      RawLoader.writeEvents(ps, 100, json("""{"a": 1000, "b": 2.0}"""))
      ps.println("##start 101")
      ps.println("""{"a": 2000, "b": 3.0}""")
      ps.close()

      // try to load
      val (h1, events1, ok1) = RawHandler.load(blockid, tmp3)
      h1.length must_== 1
      events1.toSet must_== Set(100)
      ok1 must_== false

      // close this handler
      h1.close()

      // open a new handler, we should have sanitized the rawlog
      val (h2, events2, ok2) = RawHandler.load(blockid, tmp3)
      h2.length must_== 1
      events2.toSet must_== Set(100)
      ok2 must_== true
    }


    /**
     * Test missing files.
     *
     * In case the log is not there, load() must throw an Exception.
     */
    val tmp4 = tempfile()
    "throw an exception when loading empty logs" in new cleanup(tmp4) {
      RawHandler.load(blockid, tmp4) must throwA[Exception]
    }


    /**
     * Test file collisions.
     *
     * In case the log is already there, empty() must throw an Exception.
     */
    val tmp5 = tempfile()
    "throw an exception when creating already-present logs " in new cleanup(tmp5) {
      RawHandler.empty(blockid, tmp5).close()
      RawHandler.empty(blockid, tmp5) must throwA[Exception]
    }


    /**
     * Empty rawlog.
     *
     * It is fine to have a rawlog without any actual events in it.
     */
    val tmp6 = tempfile()
    "load empty files " in new cleanup(tmp6) {
      RawHandler.empty(blockid, tmp6).close()
      val (h, es, ok) = RawHandler.load(blockid, tmp6)
      ok must_== true
      es.isEmpty must_== true
      h.length must_== 0
    }


    /**
     * Test recovery from totally corrupted rawlog file #2.
     *
     * In this case the log is full of garbage; load() should throw an error.
     */
    val tmp7 = tempfile()
    "throw errors when totally corrupted" in new cleanup(tmp7) {
      val ps = makeps(tmp7)
      ps.println("jwgeigjewaijgweigjweijew")
      ps.close()
      RawHandler.load(blockid, tmp7) must throwA[Exception]
    }


    /**
     * Test recovery from partially-corrupted rawlog file #3.
     *
     * In this case the log is OK until an event ends up full of garbage.
     */
    val tmp8 = tempfile()
    "recover when partially corrupted" in new cleanup(tmp8) {
      val range = (0 until 20)

      val ps = makeps(tmp8)
      RawLoader.writeHeader(ps, blockid)
      def makejson(i: Int) = json("""{"a": %s, "b": %s}""" format (i * 2, i * 3))
      range.foreach(i => RawLoader.writeEvents(ps, i, makejson(i)))
      ps.println("biewjgwijgjigiwej")
      ps.close()

      val (h, events, ok) = RawHandler.load(blockid, tmp8)
      h.length must_== range.length
      events.toSet must_== range.toSet
      ok must_== false

      // double-check the data
      val cpa = CPath(".a")
      val cpb = CPath(".b")

      val cpaR = ColumnRef(cpa, CNum)
      val cpbR = ColumnRef(cpb, CNum)

      val bs = new BitSet()
      range.foreach(i => bs.set(i))

      val sa = ArraySegment(blockid, cpa, CNum, bs.copy, range.map(i => BigDecimal(i * 2)).toArray)
      val sb = ArraySegment(blockid, cpb, CNum, bs.copy, range.map(i => BigDecimal(i * 3)).toArray)

      h.snapshot(Some(Set(cpa))).segments must contain(sa)
      h.snapshot(Some(Set(cpb))).segments must contain(sb)

      h.snapshotRef(Some(Set(cpaR))).segments must contain(sa)
      h.snapshotRef(Some(Set(cpbR))).segments must contain(sb)

      h.snapshotRef(Some(Set(ColumnRef(cpa, CString)))).segments must beEmpty
      h.snapshotRef(Some(Set(ColumnRef(cpb, CString)))).segments must beEmpty
    }

    val tmp9 = tempfile()
    "make sure we defensively copy" in new cleanup(tmp9) {
      val h = RawHandler.empty(blockid, tmp9)
      val cpa = CPath(".a")
      val cpb = CPath(".b")

      val struct1 = h.structure
      struct1.toSet must_== Set()

      val snap1 = h.snapshot(None).segments
      snap1.toSet must_== Set()

      val snap1R = h.snapshotRef(None).segments
      snap1R.toSet must_== Set()

      val a1 = h.snapshot(Some(Set(cpa))).segments
      a1.toSet must_== Set()

      val a1R = h.snapshotRef(Some(Set(ColumnRef(cpa, CString)))).segments
      a1R.toSet must_== Set()

      h.write(16, json("""{"a": "foo"} {"a": "bar"}"""))

      val struct2 = h.structure
      struct1.toSet must_== Set()
      struct2.toSet must_== Set(ColumnRef(cpa, CString))

      val snap2 = h.snapshot(None).segments
      snap1.toSet must_== Set()
      snap2.toSet must_== Set(ArraySegment(blockid, cpa, CString, bitset(0, 1), Array("foo", "bar")))

      val snap2R = h.snapshotRef(None).segments
      snap1R.toSet must_== Set()
      snap2R.toSet must_== Set(ArraySegment(blockid, cpa, CString, bitset(0, 1), Array("foo", "bar")))

      val a2 = h.snapshot(Some(Set(cpa))).segments
      a1.toSet must_== Set()
      a2.toSet must_== Set(ArraySegment(blockid, cpa, CString, bitset(0, 1), Array("foo", "bar")))

      val a2R = h.snapshotRef(Some(Set(ColumnRef(cpa, CString)))).segments
      a1R.toSet must_== Set()
      a2R.toSet must_== Set(ArraySegment(blockid, cpa, CString, bitset(0, 1), Array("foo", "bar")))

      val a2REmpty = h.snapshotRef(Some(Set(ColumnRef(cpa, CNum)))).segments
      a2REmpty.toSet must_== Set()

      h.write(17, json("""{"a": "qux", "b": "xyz"} {"a": "baz", "b": "bla"}"""))

      val struct3 = h.structure
      struct1.toSet must_== Set()
      struct2.toSet must_== Set(ColumnRef(cpa, CString))
      struct3.toSet must_== Set(ColumnRef(cpa, CString), ColumnRef(cpb, CString))

      val snap3 = h.snapshot(None).segments
      val snap3R = h.snapshotRef(None).segments

      snap1.toSet must_== Set()
      snap2.toSet must_== Set(ArraySegment(blockid, cpa, CString, bitset(0, 1), Array("foo", "bar")))
      snap3.toSet must_== Set(
        ArraySegment(blockid, cpa, CString, bitset(0, 1, 2, 3), Array("foo", "bar", "qux", "baz")),
        ArraySegment(blockid, cpb, CString, bitset(2, 3), Array(null, null, "xyz", "bla")))
      snap3 mustEqual snap3R

      val a3 = h.snapshot(Some(Set(cpa))).segments
      val a3R = h.snapshotRef(Some(Set(ColumnRef(cpa, CString)))).segments
      val a3REmpty = h.snapshotRef(Some(Set(ColumnRef(cpa, CNum)))).segments

      a1.toSet must_== Set()
      a2.toSet must_== Set(ArraySegment(blockid, cpa, CString, bitset(0, 1), Array("foo", "bar")))
      a3.toSet must_== Set(ArraySegment(blockid, cpa, CString, bitset(0, 1, 2, 3), Array("foo", "bar", "qux", "baz")))
      a3 mustEqual a3R

      a3REmpty.toSet must_== Set()
    }
  }
}
