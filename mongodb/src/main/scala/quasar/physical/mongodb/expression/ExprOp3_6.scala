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

package quasar.physical.mongodb.expression

import slamdata.Predef._
import quasar.fp._
import quasar.fp.ski._
import quasar.physical.mongodb.{Bson}
import quasar.time.TemporalPart, TemporalPart._

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

/** "Pipeline" operators added in MongoDB version 3.6 */
trait ExprOp3_6F[A]

object ExprOp3_6F {

  object DateParts {
    val isoWeekYear = "isoWeekYear"
    val isoWeek = "isoWeek"
    val isoDayOfWeek = "isoDayOfWeek"
    val year = "year"
    val month = "month"
    val day = "day"
    val hour = "hour"
    val minute = "minute"
    val second = "second"
    val millisecond = "millisecond"
    val timezone = "timezone"
  }

  final case class $mergeObjectsF[A](docs: List[A]) extends ExprOp3_6F[A]
  final case class $dateFromStringF[A](string: A, timezone: Option[A]) extends ExprOp3_6F[A]
  final case class $dateFromPartsF[A](
    year: A,
    month: Option[A],
    day: Option[A],
    hour: Option[A],
    minute: Option[A],
    second: Option[A],
    millisecond: Option[A],
    timezone: Option[A]
  ) extends ExprOp3_6F[A]
  final case class $dateFromPartsIsoF[A](
    isoWeekYear: A,
    isoWeek: Option[A],
    isoDayOfWeek: Option[A],
    hour: Option[A],
    minute: Option[A],
    second: Option[A],
    millisecond: Option[A],
    timezone: Option[A]
  ) extends ExprOp3_6F[A]
  final case class $dateToPartsF[A](date: A, timezone: Option[A], iso8601: Option[Boolean]) extends ExprOp3_6F[A]

  // the order of $dateFromPartsF's TemporalPart arguments
  private val dateFromPartsArgs =
    List[TemporalPart](
      Year,
      Month,
      Day,
      Hour,
      Minute,
      Second,
      Millisecond
    )

  def dateFromPartsArgIndex(part: TemporalPart): Option[Int] = {
    val i = dateFromPartsArgs.indexWhere(_ === part)
    if (i === -1) none else i.some
  }

  implicit val equal: Delay[Equal, ExprOp3_6F] =
    new Delay[Equal, ExprOp3_6F] {
      def apply[A](eq: Equal[A]) = {
        implicit val EQ: Equal[A] = eq
        Equal.equal {
          case ($mergeObjectsF(d1), $mergeObjectsF(d2)) => d1 ≟ d2
          case ($dateFromStringF(s1, t1), $dateFromStringF(s2, t2)) => s1 ≟ s2 && t1 ≟ t2
          case ($dateFromPartsF(y1, m1, d1, h1, mi1, s1, ms1, t1), $dateFromPartsF(y2, m2, d2, h2, mi2, s2, ms2, t2)) =>
            y1 ≟ y2 && m1 ≟ m2 && d1 ≟ d2 && h1 ≟ h2 && mi1 ≟ mi2 && s1 ≟ s2 && ms1 ≟ ms2 && t1 ≟ t2
          case ($dateFromPartsIsoF(y1, w1, d1, h1, mi1, s1, ms1, t1), $dateFromPartsIsoF(y2, w2, d2, h2, mi2, s2, ms2, t2)) =>
            y1 ≟ y2 && w1 ≟ w2 && d1 ≟ d2 && h1 ≟ h2 && mi1 ≟ mi2 && s1 ≟ s2 && ms1 ≟ ms2 && t1 ≟ t2
          case ($dateToPartsF(d1, t1, i1), $dateToPartsF(d2, t2, i2)) =>
            d1 ≟ d2 && t1 ≟ t2 && i1 ≟ i2
          case _ => false
        }
      }
    }

  implicit val traverse: Traverse[ExprOp3_6F] = new Traverse[ExprOp3_6F] {
    def traverseImpl[G[_], A, B](fa: ExprOp3_6F[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[ExprOp3_6F[B]] =
      fa match {
        case $mergeObjectsF(d) => G.map(d.traverse(f))($mergeObjectsF(_))
        case $dateFromStringF(s, t) => (f(s) |@| Traverse[Option].traverse(t)(f))($dateFromStringF(_, _))
        case $dateFromPartsF(y, m, d, h, mi, s, ms, t) =>
          (f(y) |@| Traverse[Option].traverse(m)(f) |@| Traverse[Option].traverse(d)(f) |@|
           Traverse[Option].traverse(h)(f) |@| Traverse[Option].traverse(mi)(f) |@|
           Traverse[Option].traverse(s)(f) |@| Traverse[Option].traverse(ms)(f) |@|
           Traverse[Option].traverse(t)(f))($dateFromPartsF(_, _, _, _, _, _, _, _))
        case $dateFromPartsIsoF(y, w, d, h, mi, s, ms, t) =>
          (f(y) |@| Traverse[Option].traverse(w)(f) |@| Traverse[Option].traverse(d)(f) |@|
           Traverse[Option].traverse(h)(f) |@| Traverse[Option].traverse(mi)(f) |@|
           Traverse[Option].traverse(s)(f) |@| Traverse[Option].traverse(ms)(f) |@|
           Traverse[Option].traverse(t)(f))($dateFromPartsIsoF(_, _, _, _, _, _, _, _))
        case $dateToPartsF(d, t, i) => (f(d) |@| Traverse[Option].traverse(t)(f))($dateToPartsF(_, _, i))
      }
  }

  implicit def ops[F[_]: Functor](implicit I: ExprOp3_6F :<: F)
      : ExprOpOps.Aux[ExprOp3_6F, F] =
    new ExprOpOps[ExprOp3_6F] {
      type OUT[A] = F[A]

      val simplify: AlgebraM[Option, ExprOp3_6F, Fix[F]] = κ(None)

      def bson: Algebra[ExprOp3_6F, Bson] = {
        case $mergeObjectsF(d) => Bson.Doc("$mergeObjects" -> Bson.Arr(d: _*))
        case $dateFromStringF(s, t) =>
          Bson.Doc("$dateFromString" -> Bson.Doc(
            ListMap("dateString" -> s) ++
              (t.map(tz => ListMap("timezone" -> tz)).getOrElse(ListMap()))))
        case $dateFromPartsF(y, m, d, h, mi, s, ms, t) =>
          Bson.Doc("$dateFromParts" -> Bson.Doc.opt(ListMap(
            DateParts.year -> y.some,
            DateParts.month -> m,
            DateParts.day -> d,
            DateParts.hour -> h,
            DateParts.minute -> mi,
            DateParts.second -> s,
            DateParts.millisecond -> ms,
            DateParts.timezone -> t)))
        case $dateFromPartsIsoF(y, w, d, h, mi, s, ms, t) =>
          Bson.Doc("$dateFromParts" -> Bson.Doc.opt(ListMap(
            DateParts.isoWeekYear -> y.some,
            DateParts.isoWeek -> w,
            DateParts.isoDayOfWeek -> d,
            DateParts.hour -> h,
            DateParts.minute -> mi,
            DateParts.second -> s,
            DateParts.millisecond -> ms,
            DateParts.timezone -> t)))
        case $dateToPartsF(d, t, i) =>
          Bson.Doc("$dateToParts" -> Bson.Doc.opt(ListMap(
            "date" -> d.some,
            "timezone" -> t,
            "iso8601" -> i.map(b => Bson.Bool(b)))))
      }

      def rebase[T](base: T)(implicit T: Recursive.Aux[T, OUT]) = I(_).some

      def rewriteRefsM[M[_]: Monad](applyVar: PartialFunction[DocVar, M[DocVar]]) = κ(None)
    }

  final class fixpoint[T, EX[_]: Functor]
    (embed: EX[T] => T)
    (implicit I: ExprOp3_6F :<: EX) {
    @inline private def convert(expr: ExprOp3_6F[T]): T = embed(I.inj(expr))

    def $mergeObjects(docs: List[T]): T = convert($mergeObjectsF(docs))
    def $dateFromString(s: T, tz: Option[T]): T = convert($dateFromStringF(s, tz))
    def $dateFromParts(y: T, m: Option[T], d: Option[T], h: Option[T], mi: Option[T],
          s: Option[T], ms: Option[T], tz: Option[T])
        : T =
      convert($dateFromPartsF(y, m, d, h, mi, s, ms, tz))
    def $dateFromPartsIso(y: T, w: Option[T], d: Option[T], h: Option[T], mi: Option[T],
          s: Option[T], ms: Option[T], tz: Option[T]): T =
      convert($dateFromPartsIsoF(y, w, d, h, mi, s, ms, tz))
    def $dateToParts(date: T, tz: Option[T], iso8601: Option[Boolean]): T =
      convert($dateToPartsF(date, tz, iso8601))
  }
}
