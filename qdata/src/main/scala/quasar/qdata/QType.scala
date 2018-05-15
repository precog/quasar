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

package quasar.qdata

sealed trait QType

final case object QLong extends QType
final case object QDouble extends QType
final case object QReal extends QType

final case object QByte extends QType
final case object QString extends QType
final case object QNull extends QType
final case object QBoolean extends QType

final case object QLocalDateTime extends QType
final case object QLocalDate extends QType
final case object QLocalTime extends QType
final case object QOffsetDateTime extends QType
final case object QOffsetDate extends QType
final case object QOffsetTime extends QType
final case object QInterval extends QType

final case object QArray extends QType
final case object QObject extends QType
