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

package quasar.physical.mongodb.planner

import slamdata.Predef.{Map => _, _}
import quasar.contrib.pathy.AFile
import quasar.fs.{FileSystemError, MonadFsErr}
import quasar.physical.mongodb.{sigil, Collection}
import quasar.qscript._
import quasar.qscript.RecFreeS._
import quasar.fp.{:<<:, ACopK}

import matryoshka._
import matryoshka.implicits._
import org.bson.{BsonDocument, BsonValue}
import scalaz._, Scalaz._

/** A QScript transformation that, given a function to retrieve a document
  * from a collection, inserts projections around any `ShiftedRead`s of
  * collections having documents wrapped in the Quasar sigil, eliding them
  * from the query.
  */
object elideQuasarSigil {
  import FileSystemError.pathErr
  import MapFuncsCore._

  def apply[T[_[_]]: CorecursiveT, F[a] <: ACopK[a]: Functor, M[_]: Monad: MonadFsErr]
      (anyDoc: Collection => OptionT[M, BsonDocument])
      (implicit
        SR: Const[ShiftedRead[AFile], ?] :<<: F,
        QC: QScriptCore[T, ?] :<<: F)
      : F[T[F]] => M[F[T[F]]] = {

    case sr @ SR(Const(ShiftedRead(f, IdOnly))) =>
      sr.point[M]

    case sr @ SR(Const(ShiftedRead(f, idStatus))) =>
      (collection[M](f).liftM[OptionT] >>= anyDoc).run
        .map(_.flatMap(getValue[T]).fold(sr) { fm =>
          QC(Map(sr.embed, elideSigil[T](idStatus, fm).asRec))
        })

    case other => other.point[M]
  }

  ////

  private def collection[M[_]: Monad: MonadFsErr](f: AFile): M[Collection] =
    Collection.fromFile(f).fold(
      e => MonadFsErr[M].raiseError(pathErr(e)),
      _.point[M])

  private def elideSigil[T[_[_]]: CorecursiveT](idStatus: IdStatus, prj: FreeMap[T]): FreeMap[T] =
    idStatus match {
      case IdOnly    => HoleF
      case ExcludeId => prj
      case IncludeId =>
        MapFuncCore.StaticArray(List(
          Free.roll(MFC(ProjectIndex(HoleF, IntLit(0)))),
          prj >> Free.roll(MFC(ProjectIndex(HoleF, IntLit(1))))))
    }

  private def getValue[T[_[_]]: CorecursiveT](doc: BsonDocument): Option[FreeMap[T]] =
    if (sigil.Sigil[BsonValue].mapReduceQuasarSigilExists(doc))
      Some(sigil.projectMapReduceQuasarValue[T])
    else if (sigil.Sigil[BsonValue].quasarSigilExists(doc))
      Some(sigil.projectQuasarValue[T])
    else
      None
}
