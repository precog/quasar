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

package quasar.precog.util

import quasar.time.OffsetDate

import java.time._
import java.time.format._
import java.util.regex.Pattern

object DateTimeUtil {

  // mimics ISO_INSTANT
  private val dateTimeRegex =
    Pattern.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")

  def looksLikeIso8601(s: String): Boolean = dateTimeRegex.matcher(s).matches

  def parseInstant(value: String): Instant = Instant.parse(value)

  def parseOffsetDateTime(value: String): OffsetDateTime = OffsetDateTime.parse(value)

  def parseOffsetTime(value: String): OffsetTime = OffsetTime.parse(value)

  def parseOffsetDate(value: String): OffsetDate = OffsetDate.parse(value)

  def parseLocalDateTime(value: String): LocalDateTime = LocalDateTime.parse(value)

  def parseLocalTime(value: String): LocalTime = LocalTime.parse(value)

  def parseLocalDate(value: String): LocalDate = LocalDate.parse(value)

  def isValidTimestamp(str: String): Boolean = try {
    parseInstant(str); true
  } catch {
    case _:IllegalArgumentException => false
  }

  def isValidOffsetDateTime(str: String): Boolean = try {
    parseOffsetDateTime(str); true
  } catch {
    case _:IllegalArgumentException => false
  }

  def isValidOffsetTime(str: String): Boolean = try {
    parseOffsetTime(str); true
  } catch {
    case _:IllegalArgumentException => false
  }

  def isValidOffsetDate(str: String): Boolean = try {
    parseOffsetDate(str); true
  } catch {
    case _:IllegalArgumentException => false
  }

  def isValidLocalDateTime(str: String): Boolean = try {
    parseLocalDateTime(str); true
  } catch {
    case _:IllegalArgumentException => false
  }

  def isValidLocalTime(str: String): Boolean = try {
    parseLocalTime(str); true
  } catch {
    case _:IllegalArgumentException => false
  }

  def isValidLocalDate(str: String): Boolean = try {
    parseLocalDate(str); true
  } catch {
    case _:IllegalArgumentException => false
  }

  def isValidFormat(time: String, fmt: String): Boolean = try {
    DateTimeFormatter.ofPattern(fmt)./*withOffsetParsed().*/parse(time); true
  } catch {
    case _: IllegalArgumentException => false
  }

  def isValidDuration(period: String): Boolean = try {
    Duration.parse(period); true
  } catch {
    case _: IllegalArgumentException => false
  }
}
