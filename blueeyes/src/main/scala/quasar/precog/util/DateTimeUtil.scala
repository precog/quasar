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
import java.time.temporal.{TemporalAccessor, TemporalQuery}

import java.util.regex.Pattern

object DateTimeUtil {
  // from http://stackoverflow.com/a/30072486/9815
  private val fullParser = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss[xxx]")

  // from joda-time javadoc
  private val basicParser = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSZ")

  //2013-02-04T18:07:39.608835
  private val dateTimeRegex = Pattern.compile("^[0-9]{4}-?[0-9]{2}-?[0-9]{2}.*$")

  def looksLikeIso8601(s: String): Boolean = dateTimeRegex.matcher(s).matches

  def parseDateTime(value0: String): ZonedDateTime = {
    val value = value0.trim.replace(" ", "T")

    val parser = if (value.contains("-") || value.contains(":")) {
      fullParser
    } else {
      basicParser
    }

    parser.parse(value, new TemporalQuery[ZonedDateTime] {
      def queryFrom(temporal: TemporalAccessor) =
        ZonedDateTime.from(temporal)
    })
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
