package quasar.api.services

import quasar._
import quasar.Predef._
import quasar.api.{MessageFormat, Destination, AsDirPath}
import quasar.{Variables, fs}
import quasar.fs._
import quasar.recursionschemes.Fix, Fix._
import quasar.sql.{ParsingPathError, ParsingError, SQLParser, Query}

import argonaut._, Argonaut._
import org.http4s.headers.Accept
import org.http4s._
import org.http4s.dsl._
import org.http4s.server._
import org.http4s.argonaut._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.~>

object query {

  implicit val QueryDecoder = new QueryParamDecoder[Query] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Query] =
      Query(value.value).successNel[ParseFailure]
  }

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object QueryParam extends QueryParamDecoderMatcher[Query]("q")

  def formatParsingError(error: ParsingError): Task[Response] = error match {
    case ParsingPathError(e) => ???
    case _ => BadRequest(error.message)
  }

  private val QueryParameterMustContainQuery = BadRequest("The request must contain a query")
  private val POSTContentMustContainQuery    = BadRequest("The body of the POST must contain a query")
  private val DestinationHeaderMustExist     = BadRequest("The '" + Destination.name + "' header must be specified")

  def translateSemanticErrors(error: SemanticErrors): Task[Response] = BadRequest(error.shows)

  private val VarPrefix = "var."
  private def vars(req: Request) = Variables(req.params.collect {
    case (k, v) if k.startsWith(VarPrefix) => (VarName(k.substring(VarPrefix.length)), VarValue(v)) })

  def addOffsetLimit(query: sql.Expr, offset: Option[Natural], limit: Option[Positive]): sql.Expr = {
    val skipped = offset.fold(query)(o => sql.Binop(query, sql.IntLiteral(o.value), sql.Offset))
    limit.fold(skipped)(l => sql.Binop(skipped, sql.IntLiteral(l.value), sql.Limit))
  }

  def service[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                                    W: WriteFile.Ops[S],
                                                    M: ManageFile.Ops[S],
                                                    Q: QueryFile.Ops[S]): HttpService = {

    val removePhaseResults = new (FileSystemErrT[PhaseResultT[Free[S,?], ?], ?] ~> FileSystemErrT[Free[S,?], ?]) {
      def apply[A](t: FileSystemErrT[PhaseResultT[Free[S,?], ?], A]): FileSystemErrT[Free[S,?], A] =
        EitherT[Free[S,?],FileSystemError,A](t.run.value)
    }

    HttpService {
      case req @ GET -> AsDirPath(path) :? QueryParam(query) +& Offset(offset) +& Limit(limit) => {
        handleOffsetLimitParams(offset, limit) { (offset, limit) =>
          SQLParser.parseInContext(query, fs.convert(path)).fold(
            formatParsingError,
            expr => queryPlan(addOffsetLimit(expr, offset, limit), vars(req)).run.value.fold(
              errs => translateSemanticErrors(errs),
              logicalPlan => {
                val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
                formatQuasarDataStreamAsHttpResponse(f)(
                  data = Q.evaluate(logicalPlan).translate[FileSystemErrT[Free[S, ?], ?]](removePhaseResults),
                  format = requestedFormat)
              }
            )
          )
        }
      }
      case GET -> _ => QueryParameterMustContainQuery
      case req @ POST -> AsDirPath(path) =>
        EntityDecoder.decodeString(req).flatMap { query =>
          if (query == "") POSTContentMustContainQuery
          else {
            req.headers.get(Destination).fold(DestinationHeaderMustExist) { destination =>
              val parseRes = SQLParser.parseInContext(Query(query),fs.convert(path)).leftMap(formatParsingError)
              val destinationFile = posixCodec.parsePath(
                relFile => \/-(\/-(relFile)),
                absFile => \/-(-\/(absFile)),
                relDir => -\/(BadRequest("Destination must be a file")),
                absDir => -\/(BadRequest("Destination must be a file")))(destination.value)
              // Add path of query if destination is a relative file or else just jump through Sandbox hoop
              val absDestination = destinationFile.flatMap(_.bimap(
                absFile => sandbox(rootDir, absFile).map(file0 => rootDir </> file0) \/> InternalServerError("Could not sandbox file"),
                relFile => sandbox(currentDir, relFile).map(file0 => currentDir </> file0).map(file1 => path </> file1) \/> InternalServerError("Could not sandbox file")
              ).merge)
              val resultOrError = (parseRes |@| absDestination)((expr, out) => {
                // Unwrap the 3 level Monad Transformer, convert from Free to Task
                Q.executeQuery(expr, vars(req), out).run.run.run.foldMap(f).flatMap { case (phases, result) =>
                  result.fold(
                    translateSemanticErrors,
                    _.fold(
                      fileSystemErrorResponse,
                      result => {
                        Ok(Json.obj(
                          "out" := posixCodec.printPath(result),
                          "phases" := phases
                        ))
                      }
                    )
                  )
                }
              })
              resultOrError.merge
            }
          }
        }
    }
  }

  def compileService[S[_]: Functor](f: S ~> Task)(implicit Q: QueryFile.Ops[S], M: ManageFile.Ops[S]): HttpService = {
    def phaseResultsResponse(prs: PhaseResults): Option[Task[Response]] =
      prs.lastOption map {
        case PhaseResult.Tree(name, value)   => Ok(Json(name := value))
        case PhaseResult.Detail(name, value) => Ok(name + "\n" + value)
      }

    def explainQuery(expr: sql.Expr, offset: Option[Natural], limit: Option[Positive], vars: Variables): Task[Response] =
      queryPlan(addOffsetLimit(expr, offset, limit), vars).run.value fold (
        translateSemanticErrors,
        lp => Q.explain(lp).run.run.foldMap(f).flatMap {
          case (phases, \/-(_)) =>
            phaseResultsResponse(phases)
              .getOrElse(InternalServerError(
                s"No explain output for plan: \n\n" + RenderTree[Fix[LogicalPlan]].render(lp).shows
              ))

          case (_, -\/(fsErr)) =>
            fileSystemErrorResponse(fsErr)
        })

    HttpService {
      case req @ GET -> AsDirPath(path) :? QueryParam(query) +& Offset(offset) +& Limit(limit) =>
        handleOffsetLimitParams(offset, limit) { (offset, limit) =>
          SQLParser.parseInContext(query, fs.convert(path))
            .fold(formatParsingError, expr => explainQuery(expr, offset, limit, vars(req)))
        }

      case GET -> _ => QueryParameterMustContainQuery
    }
  }
}
