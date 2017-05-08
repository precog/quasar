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

package quasar.blueeyes
package json

import quasar.precog._, JsonTestSupport._

import scalaz._

import scala.util.control.Exception._
import scala.math.min
import scala.util.Random.nextInt

import java.net.URLDecoder
import java.nio.ByteBuffer

class JsonParserSpec extends Specification with ScalaCheck {
  import JParser._
  import AsyncParser._

  "Any valid json can be parsed" in {
    val parsing = (json: JValue) => { parseUnsafe(json.renderPretty); true }
    prop(parsing)
  }

  "Parsing is thread safe" in {
    import java.util.concurrent._

    val json     = Examples.person
    val executor = Executors.newFixedThreadPool(100)
    val results  = (0 to 100).map(_ => executor.submit(new Callable[JValue] { def call = parseUnsafe(json) })).toList.map(_.get)
    results.zip(results.tail).forall(pair => pair._1 == pair._2) mustEqual true
  }

  "All valid string escape characters can be parsed" in {
    parseUnsafe("[\"abc\\\"\\\\\\/\\b\\f\\n\\r\\t\\u00a0\\uffff\"]") must_== JArray(JString("abc\"\\/\b\f\n\r\t\u00a0\uffff") :: Nil)
  }
}

object ParserBugsSpec extends Specification {
  "Unicode ffff is a valid char in string literal" in {
    JParser.parseFromString(""" {"x":"\uffff"} """) must not(throwAn[java.lang.Exception])
  }

  "Does not hang when parsing 2.2250738585072012e-308" in {
    allCatch.opt(JParser.parseFromString(""" [ 2.2250738585072012e-308 ] """)) mustNotEqual None
  }

  "Does not hang when parsing 22.250738585072012e-309" in {
    allCatch.opt(JParser.parseFromString(""" [ 22.250738585072012e-309 ] """)) mustNotEqual None
  }

  "Can parse funky characters" in {
    JParser.parseUnsafe(URLDecoder.decode("\"%E2%84%A2\"", "UTF-8")) must_== JString("™")
  }
}

object ParsingByteBufferSpec extends Specification {
  "Respects current ByteBuffer's position" in {
    val bb = ByteBufferWrap(Array(54, 55, 56, 57))
    bb.remaining must_== 4
    bb.get must_== 54
    bb.remaining must_== 3
    JParser.parseFromByteBuffer(bb) must_== Success(JNum(789))
    bb.remaining must_== 0
  }

  "Respects current ByteBuffer's limit" in {
    val bb = ByteBufferWrap(Array(54, 55, 56, 57))
    bb.limit(3)
    JParser.parseFromByteBuffer(bb) must_== Success(JNum(678))
    bb.remaining must_== 0
    bb.limit(4)
    bb.remaining must_== 1
    JParser.parseFromByteBuffer(bb) must_== Success(JNum(9))
    bb.remaining must_== 0
  }
}

object AsyncParserSpec extends Specification {
  import AsyncParser._

  private def loadBytes(path: String): Array[Byte] = jPath(path).slurpBytes

  private def chunk(data: Array[Byte], i: Int, j: Int) = {
    val len = min(j, data.length) - i
    if (len > 0) More(ByteBufferWrap(data, i, len)) else Done
  }

  private def chunkAll(async: AsyncParser, data: Array[Byte], f: () => Int) = {
    var vs = ArrayBuffer.empty[JValue]
    val n = data.length
    var i                   = 0
    var parser: AsyncParser = async
    while (i < n) {
      val step                              = f()
      val (AsyncParse(errors, results), p0) = parser(chunk(data, i, i + step))
      if (!errors.isEmpty) sys.error("failed %s" format errors)
      vs ++= results
      parser = p0
      i += step
    }
    vs
  }

  private def runTest(path: String, step: Int) = {
    val data = loadBytes(path)
    chunkAll(AsyncParser.stream(), data, () => step)
  }

  private def runTestRandomStep(path: String, f: () => Int) = {
    val data = loadBytes(path)
    chunkAll(AsyncParser.stream(), data, f)
  }

  "Async parser works on one 1M chunk" in {
    val vs = runTest("blueeyes/src/test/resources/z1k_nl.json", 1024 * 1024)
    vs.length must_== 1000
    0 until 1000 forall { _.toOption must beSome }
  }

  "Async parser works on chunks of 100K" in {
    val vs = runTest("blueeyes/src/test/resources/z1k_nl.json", 100 * 1024)
    vs.length must_== 1000
    (0 until 1000).forall { _.toOption must beSome }
  }

  "Async parser works on chunks of 10K" in {
    val vs = runTest("blueeyes/src/test/resources/z1k_nl.json", 10 * 1024)
    vs.length must_== 1000
    (0 until 1000).forall { _.toOption must beSome }
  }

  "Async parser works on chunks of 1K" in {
    val vs = runTest("blueeyes/src/test/resources/z1k_nl.json", 1024)
    vs.length must_== 1000
    (0 until 1000).forall { _.toOption must beSome }
  }

  "Async parser works on chunks of 100B" in {
    val vs = runTest("blueeyes/src/test/resources/z1k_nl.json", 100)
    vs.length must_== 1000
    (0 until 1000).forall { _.toOption must beSome }
  }

  "Async parser works on chunks of 10B" in {
    val vs = runTest("blueeyes/src/test/resources/z1k_nl.json", 10)
    vs.length must_== 1000
    (0 until 1000).forall { _.toOption must beSome }
  }

  "Async parser works on chunks of 1B" in {
    val vs = runTest("blueeyes/src/test/resources/z1k_nl.json", 1)
    vs.length must_== 1000
    (0 until 1000).forall { _.toOption must beSome }
  }

  "Async parser works on chunks of sizes 10B-1K" in {
    val f  = () => nextInt(1014) + 10
    val vs = runTestRandomStep("blueeyes/src/test/resources/z1k_nl.json", f)
    vs.length must_== 1000
    (0 until 1000).forall { _.toOption must beSome }
  }

  "Async parser works on chunks of sizes 1k-10K" in {
    val f  = () => nextInt(9 * 1024) + 1024
    val vs = runTestRandomStep("blueeyes/src/test/resources/z1k_nl.json", f)
    vs.length must_== 1000
    (0 until 1000).forall { _.toOption must beSome }
  }

  def run1(chunks: Seq[Input], expected: Int) = {
    var parser: AsyncParser = AsyncParser.stream()
    var t0                  = System.nanoTime
    var count               = 0
    chunks.foreach { input =>
      val (AsyncParse(errors, results), p0) = parser(input)
      if (!errors.isEmpty) sys.error("errors: %s" format errors)
      count += results.length
      parser = p0
    }
    val t = System.nanoTime - t0
    if (count != expected) sys.error("wrong number of records")
    t
  }

  def run2(bb: ByteBuffer, expected: Int) = {
    val tt0 = System.nanoTime
    val v   = JParser.parseManyFromByteBuffer(bb)
    val tt  = System.nanoTime - tt0
    val seq = v.toOption.getOrElse(sys.error("failed to parse"))
    if (seq.length != expected) sys.error("wrong number of records")
    tt
  }

  "Async parser performs adequately" in {
    val n    = 1 * 1000
    val data = loadBytes("blueeyes/src/test/resources/z1k_nl.json")

    val step = 100000
    println("parsing %d bytes with %d-byte chunks" format (data.length, step))

    def chunks = (0 until data.length by step).map(i => chunk(data, i, i + step))
    def bb     = ByteBufferWrap(data)

    // warmup
    run1(chunks, n); run2(bb, n)
    run1(chunks, n); run2(bb, n)
    System.gc()

    val t1 = (0 until 10).foldLeft(0.0) { (t, _) =>
      val tt = run1(chunks, n); System.gc(); t + tt
    }
    println("async: %.2f ms" format (t1 / 10000000.0))

    val t2 = (0 until 10).foldLeft(0.0) { (t, _) =>
      val tt = run2(bb, n); System.gc(); t + tt
    }
    println("byteb: %.2f ms" format (t2 / 10000000.0))
    ok
  }

  "Async parser can fail fast" in {
    val json = """{"foo": 123, "bar": 999}
{"foo": 123, "bar": 999x}
{"foo": 123, "bar": 999}
{"foo": 123, "bar": 999}
{"foo": 123, "bar": 999
{"foo": 123, "bar": 999}
{"foo": 123, "bar": 999}
{"foo": 123, "bar": {"foo": 123, "bar": 999}}
{"foo": 123, "bar: {"foo": 123, "bar": 999}}
{"foo": 123, "bar": 999}
{"foo": 123, "bar": 999}
xyz
{"foo": 123, "bar": 999}
{"foo": 123, "bar": 999}"""

    val bs = json.getBytes(Utf8Charset)
    val c  = chunk(bs, 0, bs.length)

    var p = AsyncParser.stream()
    val (AsyncParse(es, js), p2) = p(c)

    // we should only have parsed 1 valid record, and seen 1 error
    json.split('\n').length must_== 14
    es.length must_== 1
    js.length must_== 1

    def confirm(e: ParseException, y: Int, x: Int) =
      (e.line must_== y) && (e.col must_== x)

    confirm(es(0), 2, 24)
  }

  "Handles whitespace correctly" in {
    def ja(ns: Int*) = JArray(ns.map(n => JNum(n)): _*)

    JParser.parseFromString("[1, 2,\t3,\n4,\r5]\r").toOption must_== Some(ja(1, 2, 3, 4, 5))
    JParser.parseManyFromString("[1,\r\n2]\r\n[3,\r\n4]\r\n").toOption must_== Some(Seq(ja(1, 2), ja(3, 4)))
    JParser.parseFromString("[1, 2,\t3,\n4,\0 5]").toOption must_== None
    JParser.parseManyFromString("[1,\r\n2]\0[3,\r\n4]\r\n").toOption must_== None
  }

  "Handles whitespace correctly" in {
    def ja(ns: Int*) = JArray(ns.map(n => JNum(n)): _*)

    JParser.parseFromString("[1, 2,\t3,\n4,\r5]\r").toOption must_== Some(ja(1, 2, 3, 4, 5))
    JParser.parseManyFromString("[1,\r\n2]\r\n[3,\r\n4]\r\n").toOption must_== Some(Seq(ja(1, 2), ja(3, 4)))
    JParser.parseFromString("[1, 2,\t3,\n4,\0 5]").toOption must_== None
    JParser.parseManyFromString("[1,\r\n2]\0[3,\r\n4]\r\n").toOption must_== None
  }
}

object ArrayUnwrappingSpec extends Specification {
  import JParser._
  import AsyncParser._

  def bb(s: String)        = More(ByteBufferWrap(s.getBytes("UTF-8")))
  def j(s: String, n: Int) = JObject(Map(s -> JNum(n)))

  "Unwrapping array parser catches errors" in {
    val p1                       = AsyncParser.unwrap()
    val (AsyncParse(e1, r1), p2) = p1.apply(bb("""[{"a": 1}, {"b": 2}"""))
    e1.length must_== 0
    r1 must_== Seq(j("a", 1), j("b", 2))

    val (AsyncParse(e2, r2), p3) = p2.apply(bb(""", {"c": 3}"""))
    e2.length must_== 0
    r2 must_== Seq(j("c", 3))

    val (AsyncParse(e3a, r3a), _) = p3.apply(bb("""["""))
    r3a must_== Seq()
    e3a.length must_== 1

    val (AsyncParse(e3b, r3b), _) = p3.apply(bb(""))
    r3b must_== Seq()
    e3b.length must_== 0

    val (AsyncParse(e3c, r3c), _) = p3.apply(bb("]"))
    r3c must_== Seq()
    e3c.length must_== 0
  }

  "Unwrapping array parser treats non-arrays correctly" in {
    val p1                      = AsyncParser.unwrap()
    val (AsyncParse(e1, _), p2) = p1.apply(bb("""{"a": 1, "b": 2"""))
    e1.length must_== 0

    // ending the object is valid
    val (AsyncParse(e2a, _), _) = p2.apply(bb("""}"""))
    e2a.length must_== 0

    // acting like you're in an array is not valid
    val (AsyncParse(e2b, _), _) = p2.apply(bb("""}, 999"""))
    e2b.length must_== 1

    // in unwrap mode only a single object is allowed
    val (AsyncParse(e2c, _), _) = p2.apply(bb("""} 999"""))
    e2c.length must_== 1
  }

  "Unwrapping array parser performs adequately" in {
    import scala.math.min
    import java.nio._

    val num = 100 * 1000
    //val num = 1 * 1000 * 1000
    //val num = 2 * 1000 * 1000
    val elem = """{"a": 999, "b": [1,2,3], "c": "fooooo", "d": {"aa": 123}}"""

    def sync(elem: String, num: Int): (Int, Int, Long) = {
      val sb = new StringBuilder
      sb.append("[" + elem)
      for (_ <- 1 until num) {
        sb.append(",")
        sb.append(elem)
      }
      sb.append("]")
      val data = sb.toString.getBytes("UTF-8")
      val bb   = ByteBufferWrap(data)
      val t0   = System.currentTimeMillis
      JParser.parseFromByteBuffer(bb)
      val ms = System.currentTimeMillis() - t0
      (1, data.length, ms)
    }

    def syncStream(elem: String, num: Int): (Int, Int, Long) = {
      val sb = new StringBuilder
      for (_ <- 0 until num) {
        sb.append(elem)
        sb.append("\n")
      }
      val data        = sb.toString.getBytes("UTF-8")
      val bb          = ByteBufferWrap(data)
      val t0          = System.currentTimeMillis
      val Success(js) = JParser.parseManyFromByteBuffer(bb)
      val ms          = System.currentTimeMillis() - t0
      (js.length, data.length, ms)
    }

    def async(parser: AsyncParser, isArray: Boolean, elem: String, num: Int): (Int, Int, Long) = {
      val elemsPerChunk    = 4520
      val completeChunks   = num / elemsPerChunk
      val partialChunkSize = num % elemsPerChunk
      assert(partialChunkSize != 0)

      val es       = (0 until elemsPerChunk).map(_ => elem)
      val leftover = (0 until partialChunkSize).map(_ => elem)
      val (firstChunk, chunk, lastChunk) = if (isArray) {
        (es.mkString("[", ",", ",").getBytes("UTF-8"), es.mkString("", ",", ",").getBytes("UTF-8"), leftover.mkString("", ",", "]").getBytes("UTF-8"))
      } else {
        (es.mkString("", "\n", "\n").getBytes("UTF-8"), es.mkString("", "\n", "\n").getBytes("UTF-8"), leftover.mkString("", "\n", "\n").getBytes("UTF-8"))
      }

      var i      = 0
      var offset = 0
      var p      = parser
      var done   = false
      var seen   = 0
      var bytes  = 0
      val t0 = System.currentTimeMillis

      while (i <= completeChunks && !done) {
        val (AsyncParse(errors, results), parser) = if (i <= completeChunks) {
          val data =
            if (i == 0)
              firstChunk
            else if (i < completeChunks)
              chunk
            else
              lastChunk

          bytes += data.length
          p.apply(More(ByteBufferWrap(data)))
        } else {
          done = true
          p.apply(Done)
        }
        if (!errors.isEmpty) throw errors.head
        seen += results.length
        p = parser
        i += 1
      }

      val ms = System.currentTimeMillis() - t0
      (seen, bytes, ms)
    }

    def verifyAndTime(tpl: (Int, Int, Long), expected: Int): (Int, Long) = {
      tpl._1 must_== expected
      (tpl._2, tpl._3)
    }

    val (b1, t1) = verifyAndTime(sync(elem, num), 1)
    val (b2, t2) = verifyAndTime(syncStream(elem, num), num)
    val (b3, t3) = verifyAndTime(async(AsyncParser.stream(), true, elem, num), 1)
    val (b4, t4) = verifyAndTime(async(AsyncParser.stream(), false, elem, num), num)
    val (b5, t5) = verifyAndTime(async(AsyncParser.unwrap(), true, elem, num), num)

    b2 must_== b4
    b1 must_== b3
    b1 must_== b5
    println("parsed array (%d bytes):  sync=%dms  async=%dms  unpacked=%dms" format (b1, t1, t3, t5))
    println("parsed stream (%d bytes): sync=%dms  async=%dms" format (b2, t2, t4))
    ok
  }
}
