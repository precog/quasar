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

package quasar.precog.util

import java.time._
import java.time.format._

import java.util.regex.Pattern

object DateTimeUtil {

  // mimics ISO_INSTANT
  private val dateTimeRegex =
    Pattern.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")

  def looksLikeIso8601(s: String): Boolean = dateTimeRegex.matcher(s).matches

  // FIXME ok this really sucks.  Instant ≠ ZonedDateTime
  def parseDateTime(value: String): ZonedDateTime = {
    val utc = ZoneId.of("UTC")

    try {
      Instant.parse(value).atZone(utc)
    } catch {
      case _: Throwable => try {
        ZonedDateTime.of(
          LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE),
          LocalTime.of(0, 0, 0, 0),
          utc)
      } catch {
        case _: Throwable =>
          ZonedDateTime.of(
            LocalDate.now(utc), // FIXME what is the expected default?
            LocalTime.parse(value, DateTimeFormatter.ISO_TIME),
            utc)
      }
    }
  }

  def isValidISO(str: String): Boolean = try {
    parseDateTime(str); true
  } catch {
    case e:IllegalArgumentException => { false }
  }

  def isValidTimeZone(str: String): Boolean = try {
    ZoneId.of(str); true
  } catch {
    case e:IllegalArgumentException => { false }
  }

  def isValidFormat(time: String, fmt: String): Boolean = try {
    DateTimeFormatter.ofPattern(fmt)./*withOffsetParsed().*/parse(time); true
  } catch {
    case e: IllegalArgumentException => { false }
  }

  def isValidPeriod(period: String): Boolean = try {
    Period.parse(period); true
  } catch {
    case e: IllegalArgumentException => { false }
  }
}
