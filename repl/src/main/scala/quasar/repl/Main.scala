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

package quasar.repl

import slamdata.Predef._
import quasar.config._
import quasar.console._
import quasar.contrib.scopt._
import quasar.effect._
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._
import quasar.main._

import org.jboss.aesh.console.{AeshConsoleCallback, Console, ConsoleOperation, Prompt}
import org.jboss.aesh.console.helper.InterruptHook
import org.jboss.aesh.console.settings.SettingsBuilder
import org.jboss.aesh.edit.actions.Action
import pathy.Path, Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

object Main {
  private def consoleIO(console: Console): ConsoleIO ~> Task =
    new (ConsoleIO ~> Task) {
      import ConsoleIO._

      def apply[A](c: ConsoleIO[A]): Task[A] = c match {
        case PrintLn(message) => Task.delay { console.getShell.out.println(message) }
      }
    }

  type DriverEff0[A] = Coproduct[ConsoleIO, Task, A]
  type DriverEff[A]  = Coproduct[ReplFail, DriverEff0, A]
  type DriverEffM[A] = Free[DriverEff, A]

  private def driver(f: Command => Free[DriverEff, Unit]): Task[Unit] = {
    def shutdownConsole(c: Console): Task[Unit] =
      Task.delay(c.getShell.out.println("Exiting...")) >>
      Task.delay(c.stop)

    Task delay {
      val console =
        new Console(new SettingsBuilder()
          .parseOperators(false)
          .enableExport(false)
          .interruptHook(new InterruptHook {
            def handleInterrupt(console: Console, action: Action) =
              shutdownConsole(console).unsafePerformSync
          })
          .create())

      console.setPrompt(new Prompt("💪 $ "))

      val i: DriverEff ~> MainTask =
        Failure.toError[MainTask, String]                  :+:
        liftMT[Task, MainErrT].compose(consoleIO(console)) :+:
        liftMT[Task, MainErrT]

      console.setConsoleCallback(new AeshConsoleCallback() {
        override def execute(input: ConsoleOperation): Int = {
          Command.parse(input.getBuffer.trim) match {
            case Command.Exit =>
              shutdownConsole(console).unsafePerformSync

            case command      =>
              f(command).foldMap(i).run.unsafePerformSync.valueOr(
                err => console.getShell.out.println("Quasar error: " + err))
          }
          0
        }
      })

      console.start()

      ()
    }
  }

  type ReplEff[S[_], A] = (
        Repl.RunStateT
    :\: ConsoleIO
    :\: ReplFail
    :\: Timing
    :\: Task
    :/: S
  )#M[A]

  def repl[S[_]](
    fs: S ~> DriverEffM
  )(implicit
    S0: Mounting :<: S,
    S1: QueryFile :<: S,
    S2: ReadFile :<: S,
    S3: WriteFile :<: S,
    S4: ManageFile :<: S,
    S5: FileSystemFailure :<: S
  ): Task[Command => Free[DriverEff, Unit]] = {
    TaskRef(Repl.RunState(rootDir, DebugLevel.Normal, 10, OutputFormat.Table, Map())).map { ref =>
      val i: ReplEff[S, ?] ~> DriverEffM =
        injectFT[Task, DriverEff].compose(AtomicRef.fromTaskRef(ref)) :+:
        injectFT[ConsoleIO, DriverEff]                                :+:
        injectFT[ReplFail, DriverEff]                                 :+:
        injectFT[Task, DriverEff].compose(Timing.toTask)              :+:
        injectFT[Task, DriverEff]                                     :+:
        fs

      (cmd => Repl.command[ReplEff[S, ?]](cmd).foldMap(i))
    }
  }

  private val DF = Failure.Ops[String, DriverEff]

  private val mt: MainTask ~> DriverEffM =
    new (MainTask ~> DriverEffM) {
      def apply[A](mt: MainTask[A]): DriverEffM[A] =
        free.lift(mt.run).into[DriverEff].flatMap(_.fold(
          err => DF.fail(err),
          _.point[DriverEffM]))
    }

  def startRepl(quasarInter: CoreEff ~> QErrs_TaskM): Task[Unit] =
    for {
      runCmd  <- repl[CoreEff](mt compose QErrs_Task.toMainTask compose quasarInter)
      _       <- driver(runCmd)
    } yield ()

  def safeMain(args: Vector[String]): Task[Unit] =
    logErrors(for {
      opts    <- CliOptions.parser.safeParse(args, CliOptions.default)
      cfgPath <- opts.config.fold(none[FsFile].point[MainTask])(cfg =>
        FsPath.parseSystemFile(cfg)
          .toRight(s"Invalid path to config file: $cfg.")
          .map(some))
      _ <- initMetaStoreOrStart[CoreConfig](
        CmdLineConfig(cfgPath, opts.cmd),
        (_, quasarInter) => startRepl(quasarInter).liftM[MainErrT],
        // The REPL does not allow you to change metastore
        // so no need to supply a function to persist the metastore
        _ => ().point[MainTask])
    } yield ())

  def main(args: Array[String]): Unit =
    safeMain(args.toVector).unsafePerformSync
}
