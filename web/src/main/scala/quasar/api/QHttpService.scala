package quasar.api

import org.http4s.{Request, Response}
import org.http4s.server.HttpService
import scalaz._
import scalaz.concurrent.Task

final case class QHttpService[S[_]](f: Request => Free[S, QuasarResponse[S]]) {
  def apply(req: Request): Free[S, QuasarResponse[S]] = f(req)

  def toHttpService(i: S ~> ResponseOr)(implicit S: Functor[S]): HttpService = {
    def mkResponse(prg: Free[S, QuasarResponse[S]]) =
      prg.foldMap(i).flatMap(r => EitherT.right(r.toHttpResponse(i))).merge

    HttpService.lift(f andThen mkResponse)
  }
}
