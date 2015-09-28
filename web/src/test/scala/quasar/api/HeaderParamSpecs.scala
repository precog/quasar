package quasar.api
import quasar.Predef._
import scalaz._
import org.specs2.mutable.Specification

class HeaderParamSpecs extends Specification {
  import org.http4s.util._

  import HeaderParam._

  "parse" should {
    "parse one" in {
      parse("""{ "Accept": "text/csv" }""") must_==
        \/-(Map(CaseInsensitiveString("Accept") -> List("text/csv")))
    }

    "parse mulitple values" in {
      parse("""{ "Foo": [ "bar", "baz" ] }""") must_==
        \/-(Map(CaseInsensitiveString("Foo") -> List("bar", "baz")))
    }

    "fail with invalid json" in {
      parse("""{""") must_==
        -\/("parse error (JSON terminates unexpectedly.)")
    }

    "fail with non-object" in {
      parse("""0""") must_==
        -\/("expected a JSON object; found: 0")
    }

    "fail with non-string/array value" in {
      parse("""{ "Foo": 0 }""") must_==
        -\/("expected a string or array of strings; found: 0")
    }

    "fail with non-string value in array" in {
      parse("""{ "Foo": [ 0 ] }""") must_==
        -\/("expected string in array; found: 0")
    }
  }

  "rewrite" should {
    import org.http4s._
    import org.http4s.headers._

    "overwrite conflicting header" in {
      val headers = rewrite(
        Headers(`Accept`(MediaType.`text/csv`)),
        Map(CaseInsensitiveString("accept") -> List("application/json")))

      headers.get(`Accept`) must beSome(`Accept`(MediaType.`application/json`))
    }

    "add non-conflicting header" in {
      val headers = rewrite(
        Headers(`Accept`(MediaType.`text/csv`)),
        Map(CaseInsensitiveString("user-agent") -> List("some_phone_browser/0.0.1")))

      headers.get(`Accept`) must beSome(`Accept`(MediaType.`text/csv`))
      headers.get(`User-Agent`) must beSome(`User-Agent`(AgentProduct("some_phone_browser", Some("0.0.1"))))
    }
  }
}