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

package quasar.api

import quasar.Data
import quasar.Predef._

import scalaz.concurrent.Task
import scalaz.stream.Process

class MessageFormatSpec extends org.specs2.mutable.Specification {
  import org.http4s._, QValue._
  import org.http4s.headers.{Accept}

  import MessageFormat._
  import JsonPrecision._
  import JsonFormat._

  "fromAccept" should {
    "use default if no accept provided" in {
      fromAccept(None) must_== MessageFormat.Default
    }

    "choose precise" in {
      val accept = Accept(
        new MediaType("application", "ldjson").withExtensions(Map("mode" -> "precise")))
      fromAccept(Some(accept)) must_== JsonContentType(Precise, LineDelimited)
    }

    "choose streaming via boundary extension" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("boundary" -> "NL")))
      fromAccept(Some(accept)) must_== JsonContentType(Readable, LineDelimited)
    }

    "choose precise list" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("mode" -> "precise")))
      fromAccept(Some(accept)) must_== JsonContentType(Precise, SingleArray)
    }

    "choose streaming and precise via extensions" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("mode" -> "precise", "boundary" -> "NL")))
      fromAccept(Some(accept)) must_== JsonContentType(Precise, LineDelimited)
    }

    "choose CSV" in {
      val accept = Accept(
        new MediaType("text", "csv"))
      fromAccept(Some(accept)) must_== Csv.Default
    }

    "choose CSV with custom format" in {
      val accept = Accept(
        new MediaType("text", "csv").withExtensions(Map(
          "columnDelimiter" -> "\t",
          "rowDelimiter" -> ";",
          "quoteChar" -> "'",
          "escapeChar" -> "\\")))
      fromAccept(Some(accept)) must_== Csv('\t', ";", '\'', '\\', None)
    }

    "choose CSV over JSON" in {
      val accept = Accept(
        new MediaType("text", "csv").withQValue(q(1.0)),
        new MediaType("application", "ldjson").withQValue(q(0.9)))
      fromAccept(Some(accept)) must_== Csv.Default
    }

    "choose JSON over CSV" in {
      val accept = Accept(
        new MediaType("text", "csv").withQValue(q(0.9)),
        new MediaType("application", "ldjson"))
      fromAccept(Some(accept)) must_== JsonContentType(Readable, LineDelimited)
    }
  }

  "Csv.escapeNewlines" should {
    """escape \r\n""" in {
      Csv.escapeNewlines("\r\n") must_== """\r\n"""
    }

    """not affect \"""" in {
      Csv.escapeNewlines("\\\"") must_== "\\\""
    }
  }

  "Csv.unescapeNewlines" should {
    """unescape \r\n""" in {
      Csv.unescapeNewlines("""\r\n""") must_== "\r\n"
    }

    """not affect \"""" in {
      Csv.escapeNewlines("""\"""") must_== """\""""
    }
  }

  "encoding" should {
    "csv" >> {
      val simpleData = List(
        Data.Obj(ListMap("a" -> Data.Int(1))),
        Data.Obj(ListMap("b" -> Data.Int(2))),
        Data.Obj(ListMap("c" -> Data.Set(List(Data.Int(3))))))
      val simpleExpected = List("a,b,c[0]", "1,,", ",2,", ",,3").mkString("", "\r\n", "\r\n")
      def test(data: List[Data], expectedEncoding: String, format: Csv) =
        format.encode(Process.emitAll(data): Process[Task,Data]).runLog.run.mkString("") must_== expectedEncoding
      "simple" >> test(
        data = simpleData,
        expectedEncoding = simpleExpected,
        format = Csv.Default
      )
      "with quoting" >> test(
        data = List(
          Data.Obj(ListMap(
            "a" -> Data.Str("\"Hey\""),
            "b" -> Data.Str("a, b, c")))),
        expectedEncoding = List("a,b", "\"\"\"Hey\"\"\",\"a, b, c\"").mkString("", "\r\n", "\r\n"),
        format = Csv.Default
      )
      "alternative delimiters" >> {
        val alternative = Csv(columnDelimiter = '\t', rowDelimiter = ";", quoteChar = '\'', escapeChar = '\\', None)
        test(
          data = simpleData,
          expectedEncoding = "a\tb\tc[0];1\t\t;\t2\t;\t\t3;",
          format = alternative
        )
      }
    }
  }
}
