package quasar.api

import quasar._
import quasar.fp._
import quasar.Predef._
import quasar.fs._, Path._
import quasar.recursionschemes.Fix
import quasar._, Backend._, Evaluator._
import quasar.config._

import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import argonaut._, Argonaut._

import org.http4s.{Request, Method, Uri, headers, ParseException}
import org.http4s.client.{Client, blaze}
import org.http4s.argonaut.jsonOf
import org.http4s.util.UrlCodingUtils.urlEncode

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

import org.parboiled2._

object Utils {

  def mounter(backend: Backend)(cfg: Config) = Errors.liftE[quasar.Evaluator.EnvironmentError](Task.now(backend))

  def tester(cfg: BackendConfig) = Errors.liftE[quasar.Evaluator.EnvironmentError](Task.now(()))
  
  /**
   * Start a server, with the given backend, execute something, and then tear
   * down the server.
   */
  def withServer[A](backend: Backend, config: Config)(f: (Client, Uri) => A): A =
    withServer[A]((_: Config) => backend.point[EnvTask], config)(f)

  def withServer[A](createBackend: Config => EnvTask[Backend], config: Config)(f: (Client, Uri) => A): A =
    withServerRecordConfigChange(createBackend, config)((client, uri, _) => f(client, uri))

   /**
   * Start a server with the given backend function and initial config, execute something,
   * and then tear down the server.
   *
   * The body receives an accessor function that, when called, returns the list of
   * configs asked to be reloaded since the server started.
   */
  def withServerRecordConfigChange[A](createBackend: Config => EnvTask[Backend], config: Config)(f: (Client, Uri, ListBuffer[Config]) => A): A = { 
    import shapeless._
    // TODO: Extend specs2 to understand Task and avoid all the runs in this implementation. See SD-945
    import org.http4s.Http4s._
    import org.http4s.Uri._
    val port = Server.anyAvailablePort.run
    val client = blaze.defaultClient
    val baseUri = Uri(Some("http".ci), Some(Authority(host = RegName("localhost"), port = Some(port))))
    val reloads = ListBuffer[Config]()
    def recordConfigChange(cfg: Config) = Task.delay { ignore(reloads += cfg) }

    val updatedConfig = (lens[Config] >> 'server >> 'port0).set(config)(Some(port))
    def unexpectedRestart(config: Config) = Task.fail(new java.lang.AssertionError("Did not expect the server to be restarted with this config: " + config))
    val api = FileSystemApi(updatedConfig, createBackend, tester,
                            restartServer = unexpectedRestart,
                            configChanged = recordConfigChange)

    val srv = Server.createServer(port, 1.seconds, api.AllServices).run.run
    try { f(client, baseUri, reloads) } finally { 
      client.shutdown
      srv.traverse_(_.shutdown.void).run 
    }
    
  }

  def withServerExpectingRestart[A, B](backend: Backend, config: Config, timeoutMillis: Long = 10000, port: Int = 8888)
                                      (causeRestart: (Client, Uri) => A)(afterRestart: (Client, Uri) => B): B = {
    import org.http4s.Http4s._
    import org.http4s.Uri._
    val port = Server.anyAvailablePort.run
    val client = blaze.defaultClient
    val baseUri = Uri(Some("http".ci), Some(Authority(host = RegName("localhost"), port = Some(port))))
    import scala.concurrent.duration._

    type S = (Int, org.http4s.server.Server)
    
    val (servers, forCfg) = Server.servers(Nil, None, 1.seconds, tester, mounter(backend), _ => Task.now(()))

    val channel = Process[S => Task[A \/ B]](
      κ(Task.delay(\/.left(causeRestart(client, baseUri)))),
      κ(Task.delay(\/.right(afterRestart(client, baseUri))).onFinish(_ => forCfg(None))))

    val exec = servers.through(channel).runLast.flatMap(
      _.flatMap(_.toOption).cata[Task[B]](
        Task.now(_),
        Task.fail(new RuntimeException("impossible!"))))

    (forCfg(Some((port, config))) *> exec).runFor(timeoutMillis)
  }

  case class ErrorMessage(error: String)
  implicit def ErrorMessageCodecJson = casecodec1(ErrorMessage.apply, ErrorMessage.unapply)("error")
  implicit val errorMessage = jsonOf[ErrorMessage]

  case class ErrorDetail(detail: String)
  case class ErrorMessage2(error: String, details: List[ErrorDetail])

  implicit def ErrorDetailCodecJson = casecodec1(ErrorDetail.apply, ErrorDetail.unapply)("detail")
  implicit val errorDetail = jsonOf[ErrorDetail]
  implicit def ErrorMessage2CodecJson = casecodec2(ErrorMessage2.apply, ErrorMessage2.unapply)("error", "details")
  implicit val errorMessage2 = jsonOf[ErrorMessage2]

  case class MountResponse(children: List[Mount])
  case class Mount(name: String, `type`: String)
  implicit def MountCodecJson = casecodec2(Mount.apply, Mount.unapply)("name", "type")
  implicit def ReponseCodecJson = casecodec1(MountResponse.apply, MountResponse.unapply)("children")
  implicit val MountDecoder = jsonOf[Mount]
  implicit val mountResponseDecoder = jsonOf[MountResponse]

  val jsonContentType = "application/json"
  
  val preciseContentType = "application/ldjson; mode=\"precise\"; charset=UTF-8"
  val readableContentType = "application/ldjson; mode=\"readable\"; charset=UTF-8"
  val arrayContentType = "application/json; mode=\"readable\"; charset=UTF-8"
  val csvContentType = "text/csv"
  val charsetParam = "; charset=UTF-8"
  val csvResponseContentType = raw"""Content-Type: text/csv; columnDelimiter=","; rowDelimiter="\\r\\n"; quoteChar="\""; escapeChar="\""""" + charsetParam
  val nl = java.lang.System.lineSeparator
  
  implicit class RichUri(uri: Uri) {
    def /(segment: String): Uri = uri.withPath(s"${uri.path}/${urlEncode(segment)}")
  }
  implicit class RichSet[T](a: Set[T]) {
    def ⊆(b: Set[T]): Boolean = (a -- b).size == 0
  }
}

object Mock {

  sealed trait Action
  
  object Action {
    final case class Save(path: Path, rows: List[Data]) extends Action
    final case class Append(path: Path, rows: List[Data]) extends Action
  }

  implicit val PlanRenderTree = new RenderTree[Plan] {
    def render(v: Plan) = Terminal(List("Stub.Plan"), None)
  }

  case class Plan(description: String)

  object JournaledBackend {
    def apply(files: Map[Path, Process[Task, Data]]): Backend =
      new JournaledBackend(files)
  }

  /**
   * A mock backend that records the actions taken on it and exposes this through the mutable `actions` buffer
   */
  class JournaledBackend(files: Map[Path, Process[Task, Data]]) extends PlannerBackend[Plan] {

    private val pastActions = scala.collection.mutable.ListBuffer[Action]()

    val planner = new Planner[Plan] {
      def plan(logical: Fix[LogicalPlan]) = Planner.emit(Vector.empty, \/-(Plan("logical: " + logical.toString)))
    }
    val evaluator = new Evaluator[Plan] {
      val name = "Stub"
      def execute(physical: Plan) =
        EitherT.right(Task.now(ResultPath.Temp(Path("tmp/out"))))
      def compile(physical: Plan) = "Stub" -> Cord(physical.toString)
    }
    val RP = PlanRenderTree

    def scan0(path: Path, offset: Long, limit: Option[Long]) =
      files.get(path).fold(
        Process.eval[Backend.ResTask, Data](EitherT.left(Task.now(Backend.ResultPathError(NonexistentPathError(path, Some("no backend"))))))) { p =>
        val limited = p.drop(offset.toInt).take(limit.fold(Int.MaxValue)(_.toInt))
        limited.translate(liftP).translate[Backend.ResTask](Errors.convertError(Backend.ResultPathError(_)))
      }
    def count0(path: Path) =
      EitherT[Task, PathError, Long](files.get(path).fold[Task[PathError \/ Long]](Task.now(-\/(NonexistentPathError(path, Some("no backend"))))) { p =>
          p.map(κ(1)).sum.runLast.map(n => \/-(n.get))
        })

    def save0(path: Path, values: Process[Task, Data]) =
      if (path.pathname.contains("pathError"))
        EitherT.left(Task.now(PPathError(InvalidPathError("simulated (client) error"))))
      else if (path.pathname.contains("valueError"))
        EitherT.left(Task.now(PWriteError(WriteError(Data.Str(""), Some("simulated (value) error")))))
      else Errors.liftE[ProcessingError](values.runLog.map { rows =>
          pastActions += Action.Save(path, rows.toList)
          ()
        })

    def append0(path: Path, values: Process[Task, Data]) =
      if (path.pathname.contains("pathError"))
        Process.eval[Backend.PathTask, WriteError](EitherT.left(Task.now(InvalidPathError("simulated (client) error"))))
      else if (path.pathname.contains("valueError"))
        Process.eval(WriteError(Data.Str(""), Some("simulated (value) error")).point[Backend.PathTask])
      else Process.eval_(Backend.liftP(values.runLog.map { rows =>
        pastActions += Action.Append(path, rows.toList)
        ()
      }))

    def delete0(path: Path) = ().point[Backend.PathTask]

    def move0(src: Path, dst: Path, semantics: Backend.MoveSemantics) = ().point[Backend.PathTask]

    def ls0(dir: Path): Backend.PathTask[Set[Backend.FilesystemNode]] = {
      val children = files.keys.toList.map(_.rebase(dir).toOption.map(p => Backend.FilesystemNode(p.head, Backend.Plain))).flatten
      children.toSet.point[Backend.PathTask]
    }

    def defaultPath = Path.Current

    def actions: List[Action] = pastActions.toList
  }

  def simpleFiles(files: Map[Path, List[Data]]): Map[Path, Process[Task, Data]] =
    files ∘ { ds => Process.emitAll(ds) }

  val emptyBackend = JournaledBackend(ListMap())
}

object AccessControlAllowMethodsParser {
  def apply(input: ParserInput) = new AccessControlAllowMethodsParser(input).methods.run()
}

class AccessControlAllowMethodsParser(val input: ParserInput) extends MethodParser {
  import scala.Any
  def methods: Rule1[Set[Method]] = rule {
    headers.`Access-Control-Allow-Methods`.name.toString ~ ": " ~ 
      oneOrMore(method).separatedBy(", ") ~> { (methods: Seq[Method]) => methods.toSet}
  }
}

trait MethodParser extends Parser {
  import CharPredicate.Alpha
  def method: Rule1[Method] = rule {
    capture(oneOrMore(Alpha | '-')) ~> ((m:String) => Method.fromString(m).valueOr(e => throw ParseException(e)))
  }
}