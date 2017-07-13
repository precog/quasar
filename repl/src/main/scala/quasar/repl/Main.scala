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
import quasar.cli.Cmd, Cmd._
import quasar.config._
import quasar.console._
import quasar.db.StatefulTransactor
import quasar.effect._
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._
import quasar.main._, config._, metastore._
import quasar.metastore.{MetaStoreAccess, Schema}

import doobie.syntax.connectionio._
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

  private def driver(f: Command => Free[DriverEff, Unit], e: Task[Unit]): Task[Unit] = {
    def shutdownConsole(c: Console): Task[Unit] =
      Task.delay(c.getShell.out.println("Exiting...")) >>
      e.attempt                                        >>
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

  def main(args: Array[String]): Unit = {
    val cfgOpsCore = ConfigOps[CoreConfig]

    def start(tx: StatefulTransactor) =
      for {
        _       <- verifySchema(Schema.schema, tx.transactor)
        ctx     <- metastoreCtx(tx)
        // NB: for now, there's no way to add mounts through the REPL, so no point
        // in starting if you can't do anything and can't correct the situation.
        _       <- MetaStoreAccess.fsMounts
                     .map(_.isEmpty)
                     .transact(ctx.metastore.transactor)
                     .liftM[MainErrT]
                     .ifM(
                       MainTask.raiseError("No mounts configured."),
                       ().point[MainTask])

        coreApi =  QErrsTCnxIO.toMainTask(ctx.metastore.transactor) compose ctx.interp
        runCmd  <- repl[CoreEff](mt compose coreApi).liftM[MainErrT]
        _       <- driver(runCmd, ctx.closeMnts).liftM[MainErrT]
      } yield ()

    val main0: MainTask[Unit] = for {
      opts    <- CliOptions.parser.parse(args.toSeq, CliOptions.default)
                      .cata(_.point[MainTask], MainTask.raiseError("Couldn't parse options."))
      cfgPath <- opts.config.fold(none[FsFile].point[MainTask])(cfg =>
                   FsPath.parseSystemFile(cfg)
                     .toRight(s"Invalid path to config file: $cfg.")
                     .map(some))
      cfg     <- loadConfigFile[CoreConfig](cfgPath).liftM[MainErrT]
      msCfg   <- cfg.metastore.cata(
                   Task.now, MetaStoreConfig.configOps.default
                 ).liftM[MainErrT]
      tx      <- metastoreTransactor(msCfg)
      _       <- opts.cmd match {
                   case Start               => start(tx)
                   case InitUpdateMetaStore => initUpdateMigrate(Schema.schema, tx.transactor, cfgPath)
                 }
    } yield ()

    logErrors(main0).unsafePerformSync
  }

}
