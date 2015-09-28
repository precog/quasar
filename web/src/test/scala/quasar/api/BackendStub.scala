package quasar.api
import quasar.Predef._

// import quasar.fp._
import quasar._, Evaluator._//, Backend._, 
import quasar.config._
import quasar.fs._//, Path._

import scalaz._ , Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import org.http4s.Uri
import org.http4s.client.Client

import scala.collection.mutable.ListBuffer

trait BackendStub {
  import Utils._
  def mounter(backend: Backend)(cfg: Config) = Errors.liftE[quasar.Evaluator.EnvironmentError](Task.now(backend))

  /**
   * Mounts a backend without any data at each of the mount points described in
   * the [[Config]].
   */
  val backendForConfig: Config => EnvTask[Backend] = {
    val emptyFiles = Map.empty.withDefault((_: Path) => Process.halt)
    val bdefn = BackendDefinition(_ => Mock.JournaledBackend(emptyFiles).point[EnvTask])
    Mounter.mount(_, bdefn)
  }

  val files1 = ListMap(
    Path("bar") -> List(
      Data.Obj(ListMap("a" -> Data.Int(1))),
      Data.Obj(ListMap("b" -> Data.Int(2))),
      Data.Obj(ListMap("c" -> Data.Set(List(Data.Int(3)))))),
    Path("dir/baz") -> List(),
    Path("tmp/out") -> List(Data.Obj(ListMap("0" -> Data.Str("ok")))),
    Path("tmp/dup") -> List(Data.Obj(ListMap("4" -> Data.Str("ok")))),
    Path("a file") -> List(Data.Obj(ListMap("1" -> Data.Str("ok")))),
    Path("quoting") -> List(
      Data.Obj(ListMap(
        "a" -> Data.Str("\"Hey\""),
        "b" -> Data.Str("a, b, c")))),
    Path("empty") -> List())
  val bigFiles = ListMap(
    // Something simple:
    Path("range") -> Process.range(0, 100*1000).map(n => Data.Obj(ListMap("n" -> Data.Int(n)))),
    // A closer analog to what we do in the MongoDB backend:
    Path("resource") -> {
      class Count { var n = 0 }
      val acquire = Task.delay { new Count }
      def release(r: Count) = Task.now(())
      def step(r: Count) = Task.delay {
        if (r.n < 100*1000) {
          r.n += 1
          Data.Obj(ListMap("n" -> Data.Int(r.n)))
        }
        else throw Cause.End.asThrowable
      }
      scalaz.stream.io.resource(acquire)(release)(step)
    })
  val noBackends = NestedBackend(Map())
  val backends = createBackendsFromMock(Mock.JournaledBackend(bigFiles), Mock.JournaledBackend(Mock.simpleFiles(files1)))

  def createBackendsFromMock(large: Backend, normal: Backend) =
    NestedBackend(ListMap(
      DirNode("empty") -> Mock.emptyBackend,
      DirNode("foo") -> normal,
      DirNode("non") -> NestedBackend(ListMap(
        DirNode("root") -> NestedBackend(ListMap(
          DirNode("mounting") -> normal)))),
      DirNode("large") -> large,
      DirNode("badPath1") -> Mock.emptyBackend,
      DirNode("badPath2") -> Mock.emptyBackend))

  // We don't put the port here as the `withServer` function will supply the port based on it's optional input.
  val config1 = Config(SDServerConfig(None), ListMap(
    Path("/foo/") -> MongoDbConfig("mongodb://localhost/foo"),
    Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting")))

  def withBackends[A](f: (Client, Uri) => A): A = withServer[A](backends, config1)(f)
  def withoutBackend[A](f: (Client, Uri) => A): A = withServer[A](noBackends, config1)(f)
  def withBackendsRecordConfigChange[A](f: (Client, Uri, ListBuffer[Config]) => A): A = withServerRecordConfigChange[A](backendForConfig, config1)(f)

  def withoutBackendExpectingRestart[A, B](causeRestart: (Client, Uri) => A)(afterRestart: (Client, Uri) => B): B = 
        withServerExpectingRestart[A, B](noBackends, config1)(causeRestart)(afterRestart)

  def mockTest[A](f: (Client, Uri, Mock.JournaledBackend) => A) = {
    val mock = new Mock.JournaledBackend(Mock.simpleFiles(files1))
    val backends = createBackendsFromMock(Mock.JournaledBackend(bigFiles), mock)
    withServer(backends, config1) { (client, uri) => f(client, uri, mock) }
  }
}