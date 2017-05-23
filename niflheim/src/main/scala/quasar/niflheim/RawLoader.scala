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

import quasar.blueeyes.json._
import quasar.precog.common._
import quasar.precog.util._

import scala.{specialized => spec}
import scala.collection.mutable

import java.io._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private[niflheim] object RawLoader {
  private val fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")

  private val utf8 = java.nio.charset.Charset.forName("UTF-8")

  /**
   * Write the rawlog header to 'os'. Currently this is:
   *
   *   "##rawlog <id> 1\n"
   */
  def writeHeader(os: OutputStream, id: Long): Unit = {
    val s = "##rawlog " + id.toString + " 1\n"
    os.write(s.getBytes(utf8))
    os.flush()
  }

  /**
   * Write the given event to 'os'. Each event consists of an
   * 'eventid' and a sequence of Jvalue instances.
   */
  def writeEvents(os: OutputStream, eventid: Long, values: Seq[JValue]) {
    val e = eventid.toString
    os.write(("##start " + e + "\n").getBytes(utf8))
    values.foreach { j =>
      os.write(j.renderCompact.getBytes(utf8))
      os.write('\n')
    }
    os.write(("##end " + e + "\n").getBytes(utf8))
    os.flush()
  }

  /**
   * Load the rawlog (using the version 1 format).
   *
   * This method assumes the header line has already been parsed, and
   * expects to see zero-or-more of the following groups:
   */
  def load1(id: Long, f: File, reader: BufferedReader): (Seq[JValue], Seq[Long], Boolean) = {
    val rows = mutable.ArrayBuffer.empty[JValue]
    val events = mutable.ArrayBuffer.empty[(Long, Int)]
    var line = reader.readLine()
    var ok = true
    while (ok && line != null) {
      if (line.startsWith("##start ")) {
        try {
          val eventid = line.substring(8).toLong
          val count = loadEvents1(reader, eventid, rows)
          if (count < 0) {
            ok = false
          } else {
            events.append((eventid, count))
            line = reader.readLine()
          }
        } catch {
          case _: Exception =>
            ok = false
        }
      } else {
        ok = false
      }
    }
    if (!ok) recover1(id, f, rows, events)
    (rows, events.map(_._1), ok)
  }

  /**
   * Generate a "corrupted" rawlog file name.
   *
   * From "/foo/bar" we'l return "/foo/bar-corrupted-20130213155306768"
   */
  def getCorruptFile(f: File): File =
    new File(f.getPath + "-corrupted-" + fmt.format(LocalDateTime.now()))

  /**
   * Recovery
   */
  def recover1(id: Long, f: File, rows: mutable.ArrayBuffer[JValue], events: mutable.ArrayBuffer[(Long, Int)]) {

    // open a tempfile to write a "corrected" rawlog to, and write the header
    val tmp = File.createTempFile("nilfheim", "recovery")
    val os = new BufferedOutputStream(new FileOutputStream(tmp, true))
    writeHeader(os, id)

    // for each event, write its rows to the rawlog
    var row = 0
    val values = mutable.ArrayBuffer.empty[JValue]
    events.foreach { case (eventid, count) =>
      var i = 0
      while (i < count) {
        values.append(rows(row))
        row += 1
        i += 1
      }
      writeEvents(os, eventid, values)
      values.clear()
    }

    // rename the rawlog file to indicate corruption
    f.renameTo(getCorruptFile(f))

    // rename the tempfile to the rawlog file
    tmp.renameTo(f)
  }

  def isValidEnd1(line: String, eventid: Long): Boolean = try {
    line.substring(6).toLong == eventid
  } catch {
    case _: Exception => false
  }

  def loadEvents1(reader: BufferedReader, eventid: Long, rows: mutable.ArrayBuffer[JValue]): Int = {
    val sofar = mutable.ArrayBuffer.empty[JValue]

    var line = reader.readLine()
    var going = true
    var ok = false
    var count = 0

    while (going && line != null) {
      if (line.startsWith("##end ")) {
        going = false
        ok = isValidEnd1(line, eventid)
      } else {
        try {
          sofar.append(JParser.parseUnsafe(line))
          count += 1
          line = reader.readLine()
        } catch {
          case _: Exception =>
            going = false
        }
      }
    }
    if (ok) {
      rows ++= sofar
      count
    } else {
      -1
    }
  }

  def load(id: Long, f: File): (Seq[JValue], Seq[Long], Boolean) = {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), utf8))
    try {
      val header = reader.readLine()
      if (header == null)
        sys.error("missing header")
      else if (header == ("##rawlog " + id.toString + " 1"))
        load1(id, f, reader)
      else
        sys.error("unsupported header: %s" format header)
    } finally {
      reader.close()
    }
  }
}
