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

package quasar

import quasar.Predef.{ -> => _, _ }
import quasar.api.ToQResponse.ops._
import quasar.contrib.pathy._
import quasar.effect.Failure

import argonaut.{DecodeResult => _, _}, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.http4s.dsl.{Path => HPath, _}
import org.http4s.headers.`Content-Disposition`
import org.http4s.server._
import org.http4s.server.staticcontent._
import org.http4s.util._
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

package object api {
  type ResponseT[F[_], A]   = EitherT[F, Response, A]
  type ResponseIOT[F[_], A] = EitherT[F, Task[Response], A]
  type ResponseOr[A]        = ResponseT[Task, A]

  type ApiErrT[F[_], A] = EitherT[F, ApiError, A]

  // Fill in the missing HTTP4s instance
  implicit val caseInsensitiveStringEqual: Equal[CaseInsensitiveString] =
    Equal.equalA

  /** Interpret a `Failure` effect into `ResponseOr` given evidence the
    * failure type can be converted to a `QResponse`.
    */
  def failureResponseOr[E](implicit E: ToQResponse[E, ResponseOr])
    : Failure[E, ?] ~> ResponseOr =
    joinResponseOr compose failureResponseIOT[Task, E]

  def failureResponseIOT[F[_]: Monad, E](implicit E: ToQResponse[E, ResponseOr])
    : Failure[E, ?] ~> ResponseIOT[F, ?] = {

    def errToResp(e: E): Task[Response] =
      e.toResponse[ResponseOr].toHttpResponse(NaturalTransformation.refl)

    convertError[F](errToResp) compose Failure.toError[EitherT[F, E, ?], E]
  }

  /** Sequences the `Response` on the left with the outer `Task`. */
  val joinResponseOr: ResponseIOT[Task, ?] ~> ResponseOr =
    new (ResponseIOT[Task, ?] ~> ResponseOr) {
      def apply[A](et: ResponseIOT[Task, A]) =
        EitherT(et.run.flatMap(_.fold(
          _.map(_.left[A]),
          _.right[Response].point[Task])))
    }


  object Destination extends HeaderKey.Singleton {
    type HeaderT = Header
    val name = CaseInsensitiveString("Destination")
    override def matchHeader(header: Header): Option[HeaderT] = {
      if (header.name ≟ name) Some(header)
      else None
    }
    override def parse(s: String): ParseResult[Header] =
      ParseResult.success(Header.Raw(name, s))
  }

  object XFileName extends HeaderKey.Singleton {
    type HeaderT = Header
    val name = CaseInsensitiveString("X-File-Name")
    override def matchHeader(header: Header): Option[HeaderT] = {
      if (header.name ≟ name) Some(header)
      else None
    }
    override def parse(s: String): ParseResult[Header] =
      ParseResult.success(Header.Raw(name, s))
  }

  object HeaderParam extends HttpMiddleware {
    type HeaderValues = Map[CaseInsensitiveString, List[String]]

    def parse(param: String): String \/ HeaderValues = {
      def strings(json: Json): String \/ List[String] =
        json.string.map(str => \/-(str :: Nil)).getOrElse(
          json.array.map { vs =>
            vs.traverse(v => v.string \/> (s"expected string in array; found: $v"))
          }.getOrElse(-\/(s"expected a string or array of strings; found: $json")))

      for {
        json <- Parse.parse(param).leftMap("parse error (" + _ + ")").disjunction
        obj <- json.obj \/> (s"expected a JSON object; found: $json")
        values <- obj.toList.traverse { case (k, v) =>
          strings(v).map(CaseInsensitiveString(k) -> _)
        }
      } yield Map(values: _*)
    }

    def rewrite(headers: Headers, param: HeaderValues): Headers =
      Headers(
        param.toList.flatMap {
          case (k, vs) => vs.map(v => Header.Raw(CaseInsensitiveString(k), v))
        } ++
        headers.toList.filterNot(h => param contains h.name))

    def apply(service: HttpService): HttpService =
      Service.lift { req =>
        (req.params.get("request-headers").fold[String \/ Request](\/-(req)) { v =>
          parse(v).map(hv => req.copy(headers = rewrite(req.headers, hv)))
        }).fold(
          err => BadRequest(Json("error" := "invalid request-headers: " + err)),
          service.run)
      }
  }

  // [#1861] Workaround to support a small slice of RFC 5987 for this isolated case
  object RFC5987ContentDispositionRender extends HttpMiddleware {
    // NonUnitStatements due to http4s's Writer
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    final case class ContentDisposition(dispositionType: String, parameters: Map[String, String])
      extends Header.Parsed {
      import org.http4s.util.Writer
      override def key = `Content-Disposition`
      override lazy val value = super.value
      override def renderValue(writer: Writer): writer.type = {
        writer.append(dispositionType)
        parameters.foreach(p =>
          p._1.endsWith("*").fold(
            writer << "; " << p._1 << "=" << p._2,
            writer << "; " << p._1 << "=\"" << p._2 << '"'))
        writer
      }
    }

    def apply(service: HttpService): HttpService =
      service ∘ (resp => resp.headers.get(`Content-Disposition`).cata(
        i => resp.copy(headers = resp.headers
          .filter(_.name ≠ `Content-Disposition`.name)
          .put(ContentDisposition(i.dispositionType, i.parameters))),
        resp))
  }

  object Prefix {
    def apply(prefix: String)(service: HttpService): HttpService = {
      import monocle.macros.GenLens
      import scalaz.std.option._

      val _uri_path = GenLens[Request](_.uri) composeLens GenLens[Uri](_.path)

      val stripChars = prefix match {
        case "/"                    => 0
        case x if x.startsWith("/") => x.length
        case x                      => x.length + 1
      }

      def rewrite(path: String): Option[String] =
        if (path.startsWith(prefix)) Some(path.substring(stripChars))
        else None

      Service.lift { req: Request =>
        _uri_path.modifyF(rewrite)(req) match {
          case Some(req1) => service(req1)
          case None       => HttpService.notFound // note: This needs to change to `Response.fallthrough` when http4s is upgraded
        }
      }
    }
  }

  /** This encoder translates spaces into pluses, but we want the
   *  more rigorous encoding %20.
   */
  def uriEncodeUtf8(s: String): String = java.net.URLEncoder.encode(s, "UTF-8").replace("+", "%20")
  def uriDecodeUtf8(s: String): String = java.net.URLDecoder.decode(s, "UTF-8")

  private val dotdot = "%2E%2E"
  private val dot    = "%2E"

  private val escapeRel: String => String = {
    case ".." => dotdot
    case "."  => dot
    case s    => uriEncodeUtf8(s)
  }

  private val unescapeRel: String => String = uriDecodeUtf8

  val UriPathCodec = PathCodec('/', escapeRel, unescapeRel)

  // NB: HPath's own toString doesn't encode properly
  private def pathString(p: HPath) =
    "/" + p.toList.map(escapeRel).mkString("/")

  // TODO: See if possible to avoid re-encoding and decoding
  object AsDirPath {
    def unapply(p: HPath): Option[ADir] = {
      UriPathCodec.parseAbsDir(pathString(p)) map sandboxAbs
    }
  }

  object AsFilePath {
    def unapply(p: HPath): Option[AFile] = {
      UriPathCodec.parseAbsFile(pathString(p)) map sandboxAbs
    }
  }

  object AsPath {
    def unapply(p: HPath): Option[APath] = {
      AsDirPath.unapply(p) orElse AsFilePath.unapply(p)
    }
  }

  def decodedDir(encodedPath: String): ApiError \/ ADir =
    decodedPath(encodedPath) flatMap { path =>
      refineType(path).swap.leftAs(ApiError.fromMsg(
        BadRequest withReason "Directory path expected.",
        s"Expected '${posixCodec.printPath(path)}' to be a directory.",
        "path" := path))
    }

  def decodedPath(encodedPath: String): ApiError \/ APath =
    AsPath.unapply(HPath(encodedPath)) \/> ApiError.fromMsg(
      BadRequest withReason "Malformed path.",
      s"Failed to parse '${uriDecodeUtf8(encodedPath)}' as an absolute path.",
      "encodedPath" := encodedPath)

  def transcode(from: PathCodec, to: PathCodec): String => String =
    from.parsePath(to.unsafePrintPath, to.unsafePrintPath, to.unsafePrintPath, to.unsafePrintPath)

  def staticFileService(basePath: String): HttpService = {
    def pathCollector(file: jFile, config: FileService.Config, req: Request): Task[Option[Response]] = Task.delay {
      if (file.isDirectory) StaticFile.fromFile(new jFile(file, "index.html"), Some(req))
      else if (!file.isFile) None
      else StaticFile.fromFile(file, Some(req))
    }

    fileService(FileService.Config(
      systemPath = basePath,
      pathCollector = pathCollector))
  }

  def redirectService(basePath: String) = HttpService {
    // NB: this means we redirected to a path that wasn't handled, and need
    // to avoid getting into a loop.
    case GET -> path if path.startsWith(HPath(basePath)) => NotFound()

    case GET -> AsPath(path) =>
      // TODO: probably need a URL-specific codec here
      TemporaryRedirect(Uri(path = basePath + posixCodec.printPath(path)))
  }
}
