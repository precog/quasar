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

package quasar.physical.mongodb.expression

import quasar.Predef._
import quasar._, Planner._
import quasar.fp._
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.physical.mongodb.{Bson, BsonField}

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

/** "Pipeline" operators available in all supported version of MongoDB
  * (since 2.6).
  */
trait ExprOpCoreF[A]
object ExprOpCoreF {
  final case class $includeF[A]() extends ExprOpCoreF[A]
  final case class $varF[A](docVar: DocVar) extends ExprOpCoreF[A]

  final case class $andF[A](first: A, second: A, others: A*) extends ExprOpCoreF[A]
  final case class $orF[A](first: A, second: A, others: A*)  extends ExprOpCoreF[A]
  final case class $notF[A](value: A)                        extends ExprOpCoreF[A]

  final case class $setEqualsF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $setIntersectionF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $setDifferenceF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $setUnionF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $setIsSubsetF[A](left: A, right: A) extends ExprOpCoreF[A]

  final case class $anyElementTrueF[A](value: A) extends ExprOpCoreF[A]
  final case class $allElementsTrueF[A](value: A) extends ExprOpCoreF[A]

  final case class $cmpF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $eqF[A](left: A, right: A)  extends ExprOpCoreF[A]
  final case class $gtF[A](left: A, right: A)  extends ExprOpCoreF[A]
  final case class $gteF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $ltF[A](left: A, right: A)  extends ExprOpCoreF[A]
  final case class $lteF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $neqF[A](left: A, right: A) extends ExprOpCoreF[A]

  final case class $addF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $divideF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $modF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $multiplyF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $subtractF[A](left: A, right: A) extends ExprOpCoreF[A]

  final case class $concatF[A](first: A, second: A, others: A*)
      extends ExprOpCoreF[A]
  final case class $strcasecmpF[A](left: A, right: A) extends ExprOpCoreF[A]
  final case class $substrF[A](value: A, start: A, count: A) extends ExprOpCoreF[A]
  final case class $toLowerF[A](value: A) extends ExprOpCoreF[A]
  final case class $toUpperF[A](value: A) extends ExprOpCoreF[A]

  final case class $metaF[A]() extends ExprOpCoreF[A]

  final case class $sizeF[A](array: A) extends ExprOpCoreF[A]

  final case class $arrayMapF[A](input: A, as: DocVar.Name, in: A)
      extends ExprOpCoreF[A]
  final case class $letF[A](vars: ListMap[DocVar.Name, A], in: A)
      extends ExprOpCoreF[A]
  final case class $literalF[A](value: Bson) extends ExprOpCoreF[A]

  final case class $dayOfYearF[A](date: A)   extends ExprOpCoreF[A]
  final case class $dayOfMonthF[A](date: A)  extends ExprOpCoreF[A]
  final case class $dayOfWeekF[A](date: A)   extends ExprOpCoreF[A]
  final case class $yearF[A](date: A)        extends ExprOpCoreF[A]
  final case class $monthF[A](date: A)       extends ExprOpCoreF[A]
  final case class $weekF[A](date: A)        extends ExprOpCoreF[A]
  final case class $hourF[A](date: A)        extends ExprOpCoreF[A]
  final case class $minuteF[A](date: A)      extends ExprOpCoreF[A]
  final case class $secondF[A](date: A)      extends ExprOpCoreF[A]
  final case class $millisecondF[A](date: A) extends ExprOpCoreF[A]

  final case class $condF[A](predicate: A, ifTrue: A, ifFalse: A)
      extends ExprOpCoreF[A]
  final case class $ifNullF[A](expr: A, replacement: A) extends ExprOpCoreF[A]

  // TODO: if this is needed, comment explaining why
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[ExprOpCoreF[A]] =
    I.prj(expr)

  implicit def equal:
      Delay[Equal, ExprOpCoreF] =
    new Delay[Equal, ExprOpCoreF] {
      def apply[A](eq: Equal[A]) = {
        implicit val EQ = eq
        Equal.equal {
          case ($includeF(), $includeF())           => true
          case ($varF(dv1), $varF(dv2))             => dv1 ≟ dv2
          case ($addF(l1, r1), $addF(l2, r2))       => (l1 ≟ l2) && (r1 ≟ r2)
          case ($andF(a1, b1, cs1 @ _*), $andF(a2, b2, cs2 @ _*))
                                                    => (a1 ≟ a2) && (b1 ≟ b2) && (cs1.toList ≟ cs2.toList)
          case ($setEqualsF(l1, r1), $setEqualsF(l2, r2)) => (l1 ≟ l2) && (r1 ≟ r2)
          case ($setIntersectionF(l1, r1), $setIntersectionF(l2, r2)) => (l1 ≟ l2) && (r1 ≟ r2)
          case ($setDifferenceF(l1, r1), $setDifferenceF(l2, r2)) => (l1 ≟ l2) && (r1 ≟ r2)
          case ($setUnionF(l1, r1), $setUnionF(l2, r2)) => (l1 ≟ l2) && (r1 ≟ r2)
          case ($setIsSubsetF(l1, r1), $setIsSubsetF(l2, r2)) => (l1 ≟ l2) && (r1 ≟ r2)
          case ($anyElementTrueF(v1), $anyElementTrueF(v2)) => v1 ≟ v2
          case ($allElementsTrueF(v1), $allElementsTrueF(v2)) => v1 ≟ v2
          case ($arrayMapF(a1, b1, c1), $arrayMapF(a2, b2, c2)) => (a1 ≟ a2) && (b1 ≟ b2) && (c1 ≟ c2)
          case ($cmpF(l1, r1), $cmpF(l2, r2))       => (l1 ≟ l2) && (r1 ≟ r2)
          case ($concatF(a1, b1, cs1 @ _*), $concatF(a2, b2, cs2 @ _*)) => (a1 ≟ a2) && (b1 ≟ b2) && (cs1.toList ≟ cs2.toList)
          case ($condF(a1, b1, c1), $condF(a2, b2, c2)) => (a1 ≟ a2) && (b1 ≟ b2) && (c1 ≟ c2)
          case ($dayOfMonthF(v1), $dayOfMonthF(v2)) => v1 ≟ v2
          case ($dayOfWeekF(v1), $dayOfWeekF(v2))   => v1 ≟ v2
          case ($dayOfYearF(v1), $dayOfYearF(v2))   => v1 ≟ v2
          case ($divideF(l1, r1), $divideF(l2, r2)) => (l1 ≟ l2) && (r1 ≟ r2)
          case ($eqF(l1, r1), $eqF(l2, r2))         => (l1 ≟ l2) && (r1 ≟ r2)
          case ($gtF(l1, r1), $gtF(l2, r2))         => (l1 ≟ l2) && (r1 ≟ r2)
          case ($gteF(l1, r1), $gteF(l2, r2))       => (l1 ≟ l2) && (r1 ≟ r2)
          case ($hourF(v1), $hourF(v2))             => v1 ≟ v2
          case ($metaF(), $metaF())                 => true
          case ($sizeF(v1), $sizeF(v2))             => v1 ≟ v2
          case ($ifNullF(a1, b1), $ifNullF(a2, b2)) => (a1 ≟ a2) && (b1 ≟ b2)
          case ($letF(vs1, r1), $letF(vs2, r2))     => (vs1 ≟ vs2) && (r1 ≟ r2)
          case ($literalF(lit1), $literalF(lit2))   => lit1 ≟ lit2
          case ($ltF(l1, r1), $ltF(l2, r2))         => (l1 ≟ l2) && (r1 ≟ r2)
          case ($lteF(l1, r1), $lteF(l2, r2))       => (l1 ≟ l2) && (r1 ≟ r2)
          case ($millisecondF(v1), $millisecondF(v2)) => v1 ≟ v2
          case ($minuteF(v1), $minuteF(v2))         => v1 ≟ v2
          case ($modF(l1, r1), $modF(l2, r2))       => (l1 ≟ l2) && (r1 ≟ r2)
          case ($monthF(v1), $monthF(v2))           => v1 ≟ v2
          case ($multiplyF(l1, r1), $multiplyF(l2, r2)) => (l1 ≟ l2) && (r1 ≟ r2)
          case ($neqF(l1, r1), $neqF(l2, r2))       => (l1 ≟ l2) && (r1 ≟ r2)
          case ($notF(v1), $notF(v2))               => v1 ≟ v2
          case ($orF(a1, b1, cs1 @ _*), $orF(a2, b2, cs2 @ _*)) => (a1 ≟ a2) && (b1 ≟ b2) && (cs1.toList ≟ cs2.toList)
          case ($secondF(v1), $secondF(v2))         => v1 ≟ v2
          case ($strcasecmpF(a1, b1), $strcasecmpF(a2, b2)) => (a1 ≟ a2) && (b1 ≟ b2)
          case ($substrF(a1, b1, c1), $substrF(a2, b2, c2)) => (a1 ≟ a2) && (b1 ≟ b2) && (c1 ≟ c2)
          case ($subtractF(l1, r1), $subtractF(l2, r2)) => (l1 ≟ l2) && (r1 ≟ r2)
          case ($toLowerF(v1), $toLowerF(v2))       => v1 ≟ v2
          case ($toUpperF(v1), $toUpperF(v2))       => v1 ≟ v2
          case ($weekF(v1), $weekF(v2))             => v1 ≟ v2
          case ($yearF(v1), $yearF(v2))             => v1 ≟ v2
          case _                                    => true
        }
      }
    }

  implicit val traverse: Traverse[ExprOpCoreF] = new Traverse[ExprOpCoreF] {
    def traverseImpl[G[_], A, B](fa: ExprOpCoreF[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[ExprOpCoreF[B]] =
      fa match {
        case $includeF()          => G.point($includeF())
        case $varF(dv)            => G.point($varF(dv))
        case $addF(l, r)          => (f(l) |@| f(r))($addF(_, _))
        case $andF(a, b, cs @ _*) => (f(a) |@| f(b) |@| cs.toList.traverse(f))($andF(_, _, _: _*))
        case $setEqualsF(l, r)       => (f(l) |@| f(r))($setEqualsF(_, _))
        case $setIntersectionF(l, r) => (f(l) |@| f(r))($setIntersectionF(_, _))
        case $setDifferenceF(l, r)   => (f(l) |@| f(r))($setDifferenceF(_, _))
        case $setUnionF(l, r)        => (f(l) |@| f(r))($setUnionF(_, _))
        case $setIsSubsetF(l, r)     => (f(l) |@| f(r))($setIsSubsetF(_, _))
        case $anyElementTrueF(v)     => G.map(f(v))($anyElementTrueF(_))
        case $allElementsTrueF(v)    => G.map(f(v))($allElementsTrueF(_))
        case $arrayMapF(a, b, c)  => (f(a) |@| f(c))($arrayMapF(_, b, _))
        case $cmpF(l, r)          => (f(l) |@| f(r))($cmpF(_, _))
        case $concatF(a, b, cs @ _*) => (f(a) |@| f(b) |@| cs.toList.traverse(f))($concatF(_, _, _: _*))
        case $condF(a, b, c)      => (f(a) |@| f(b) |@| f(c))($condF(_, _, _))
        case $dayOfMonthF(a)      => G.map(f(a))($dayOfMonthF(_))
        case $dayOfWeekF(a)       => G.map(f(a))($dayOfWeekF(_))
        case $dayOfYearF(a)       => G.map(f(a))($dayOfYearF(_))
        case $divideF(a, b)       => (f(a) |@| f(b))($divideF(_, _))
        case $eqF(a, b)           => (f(a) |@| f(b))($eqF(_, _))
        case $gtF(a, b)           => (f(a) |@| f(b))($gtF(_, _))
        case $gteF(a, b)          => (f(a) |@| f(b))($gteF(_, _))
        case $hourF(a)            => G.map(f(a))($hourF(_))
        case $metaF()             => G.point($metaF())
        case $sizeF(a)            => G.map(f(a))($sizeF(_))
        case $ifNullF(a, b)       => (f(a) |@| f(b))($ifNullF(_, _))
        case $letF(a, b)          =>
          (Traverse[ListMap[DocVar.Name, ?]].sequence[G, B](a.map(t => t._1 -> f(t._2))) |@| f(b))($letF(_, _))
        case $literalF(lit)       => G.point($literalF(lit))
        case $ltF(a, b)           => (f(a) |@| f(b))($ltF(_, _))
        case $lteF(a, b)          => (f(a) |@| f(b))($lteF(_, _))
        case $millisecondF(a)     => G.map(f(a))($millisecondF(_))
        case $minuteF(a)          => G.map(f(a))($minuteF(_))
        case $modF(a, b)          => (f(a) |@| f(b))($modF(_, _))
        case $monthF(a)           => G.map(f(a))($monthF(_))
        case $multiplyF(a, b)     => (f(a) |@| f(b))($multiplyF(_, _))
        case $neqF(a, b)          => (f(a) |@| f(b))($neqF(_, _))
        case $notF(a)             => G.map(f(a))($notF(_))
        case $orF(a, b, cs @ _*)  => (f(a) |@| f(b) |@| cs.toList.traverse(f))($orF(_, _, _: _*))
        case $secondF(a)          => G.map(f(a))($secondF(_))
        case $strcasecmpF(a, b)   => (f(a) |@| f(b))($strcasecmpF(_, _))
        case $substrF(a, b, c)    => (f(a) |@| f(b) |@| f(c))($substrF(_, _, _))
        case $subtractF(a, b)     => (f(a) |@| f(b))($subtractF(_, _))
        case $toLowerF(a)         => G.map(f(a))($toLowerF(_))
        case $toUpperF(a)         => G.map(f(a))($toUpperF(_))
        case $weekF(a)            => G.map(f(a))($weekF(_))
        case $yearF(a)            => G.map(f(a))($yearF(_))
      }
  }

  implicit def ops[F[_]: Functor](implicit I: ExprOpCoreF :<: F): ExprOpOps.Aux[ExprOpCoreF, F] = new ExprOpOps[ExprOpCoreF] {
    type OUT[A] = F[A]

    val fp = fixpoint[Fix[F], F](Fix(_))

    def simplify: AlgebraM[Option, ExprOpCoreF, Fix[F]] = {
      case $condF(Fix($literalF(Bson.Bool(true))),  c, _) => c.some
      case $condF(Fix($literalF(Bson.Bool(false))), _, a) => a.some
      case $condF(Fix($literalF(_)),                _, _) => fp.$literal(Bson.Null).some
      case $ifNullF(Fix($literalF(Bson.Null)), r)         => r.some
      case $ifNullF(Fix($literalF(e)),         _)         => fp.$literal(e).some
      case $notF(Fix($literalF(Bson.Bool(b))))            => fp.$literal(Bson.Bool(!b)).some
      case $notF(Fix($literalF(_)))                       => fp.$literal(Bson.Null).some
      case _ => None
    }

    val bson: Algebra[ExprOpCoreF, Bson] = {
      case $includeF()                   => Bson.Bool(true)
      case $varF(dv)                     => dv.bson
      case $andF(first, second, others @ _*) =>
        Bson.Doc("$and" -> Bson.Arr(first +: second +: others: _*))
      case $orF(first, second, others @ _*) =>
        Bson.Doc("$or" -> Bson.Arr(first +: second +: others: _*))
      case $notF(value)                  => Bson.Doc("$not" -> value)
      case $setEqualsF(left, right)      => Bson.Doc("$setEquals" -> Bson.Arr(left, right))
      case $setIntersectionF(left, right) =>
        Bson.Doc("$setIntersection" -> Bson.Arr(left, right))
      case $setDifferenceF(left, right)  => Bson.Doc("$setDifference" -> Bson.Arr(left, right))
      case $setUnionF(left, right)       => Bson.Doc("$setUnion" -> Bson.Arr(left, right))
      case $setIsSubsetF(left, right)    => Bson.Doc("$setIsSubset" -> Bson.Arr(left, right))
      case $anyElementTrueF(value)       => Bson.Doc("$anyElementTrue" -> value)
      case $allElementsTrueF(value)      => Bson.Doc("$allElementsTrue" -> value)
      case $cmpF(left, right)            => Bson.Doc("$cmp" -> Bson.Arr(left, right))
      case $eqF(left, right)             => Bson.Doc("$eq" -> Bson.Arr(left, right))
      case $gtF(left, right)             => Bson.Doc("$gt" -> Bson.Arr(left, right))
      case $gteF(left, right)            => Bson.Doc("$gte" -> Bson.Arr(left, right))
      case $ltF(left, right)             => Bson.Doc("$lt" -> Bson.Arr(left, right))
      case $lteF(left, right)            => Bson.Doc("$lte" -> Bson.Arr(left, right))
      case $neqF(left, right)            => Bson.Doc("$ne" -> Bson.Arr(left, right))
      case $addF(left, right)            => Bson.Doc("$add" -> Bson.Arr(left, right))
      case $divideF(left, right)         => Bson.Doc("$divide" -> Bson.Arr(left, right))
      case $modF(left, right)            => Bson.Doc("$mod" -> Bson.Arr(left, right))
      case $multiplyF(left, right)       => Bson.Doc("$multiply" -> Bson.Arr(left, right))
      case $subtractF(left, right)       => Bson.Doc("$subtract" -> Bson.Arr(left, right))
      case $concatF(first, second, others @ _*) =>
        Bson.Doc("$concat" -> Bson.Arr(first +: second +: others: _*))
      case $strcasecmpF(left, right)     => Bson.Doc("$strcasecmp" -> Bson.Arr(left, right))
      case $substrF(value, start, count) =>
        Bson.Doc("$substr" -> Bson.Arr(value, start, count))
      case $toLowerF(value)              => Bson.Doc("$toLower" -> value)
      case $toUpperF(value)              => Bson.Doc("$toUpper" -> value)
      case $metaF()                      => Bson.Doc("$meta" -> Bson.Text("textScore"))
      case $sizeF(array)                 => Bson.Doc("$size" -> array)
      case $arrayMapF(input, as, in) =>
        Bson.Doc(
          "$map"-> Bson.Doc(
            "input" -> input,
            "as"    -> Bson.Text(as.name),
            "in"    -> in))
      case $letF(vars, in) =>
        Bson.Doc(
          "$let" -> Bson.Doc(
            "vars" -> Bson.Doc(vars.map(t => (t._1.name, t._2))),
            "in"   -> in))
      case $literalF(value)              => Bson.Doc("$literal" -> value)
      case $dayOfYearF(date)             => Bson.Doc("$dayOfYear" -> date)
      case $dayOfMonthF(date)            => Bson.Doc("$dayOfMonth" -> date)
      case $dayOfWeekF(date)             => Bson.Doc("$dayOfWeek" -> date)
      case $yearF(date)                  => Bson.Doc("$year" -> date)
      case $monthF(date)                 => Bson.Doc("$month" -> date)
      case $weekF(date)                  => Bson.Doc("$week" -> date)
      case $hourF(date)                  => Bson.Doc("$hour" -> date)
      case $minuteF(date)                => Bson.Doc("$minute" -> date)
      case $secondF(date)                => Bson.Doc("$second" -> date)
      case $millisecondF(date)           => Bson.Doc("$millisecond" -> date)
      case $condF(predicate, ifTrue, ifFalse) =>
        Bson.Doc("$cond" -> Bson.Arr(predicate, ifTrue, ifFalse))
      case $ifNullF(expr, replacement)   => Bson.Doc("$ifNull" -> Bson.Arr(expr, replacement))
    }

    // FIXME: Define a proper `Show[ExprOpCoreF]` instance.
    @SuppressWarnings(Array("org.wartremover.warts.ToString"))
    def toJsSimple: AlgebraM[PlannerError \/ ?, ExprOpCoreF, JsFn] = {
      val mjs = quasar.physical.mongodb.javascript[JsCore](Fix(_))
      import mjs._

      def expr1(x1: JsFn)(f: JsCore => JsCore): PlannerError \/ JsFn =
        \/-(JsFn(JsFn.defaultName, f(x1(jscore.Ident(JsFn.defaultName)))))
      def expr2(x1: JsFn, x2: JsFn)(f: (JsCore, JsCore) => JsCore): PlannerError \/ JsFn =
        \/-(JsFn(JsFn.defaultName, f(x1(jscore.Ident(JsFn.defaultName)), x2(jscore.Ident(JsFn.defaultName)))))

      def unop(op: jscore.UnaryOperator, x: JsFn) =
        expr1(x)(x => jscore.UnOp(op, x))
      def binop(op: jscore.BinaryOperator, l: JsFn, r: JsFn) =
        expr2(l, r)((l, r) => jscore.BinOp(op, l, r))
      def invoke(x: JsFn, name: String) =
        expr1(x)(x => jscore.Call(jscore.Select(x, name), Nil))

      def const(bson: Bson): PlannerError \/ JsCore = {
        def js(l: Js.Lit) = \/-(jscore.Literal(l))
        bson match {
          case Bson.Int64(n)        => js(Js.num(n))
          case Bson.Int32(n)        => js(Js.num(n.toLong))
          case Bson.Dec(x)          => js(Js.num(x))
          case Bson.Bool(v)         => js(Js.Bool(v))
          case Bson.Text(v)         => js(Js.Str(v))
          case Bson.Null            => js(Js.Null)
          case Bson.Doc(values)     => values.map { case (k, v) => jscore.Name(k) -> const(v) }.sequenceU.map(jscore.Obj(_))
          case Bson.Arr(values)     => values.toList.traverse(const(_)).map(jscore.Arr(_))
          case o @ Bson.ObjectId(_) => \/-(toJsObjectId(o))
          case d @ Bson.Date(_)     => \/-(toJsDate(d))
          // TODO: implement the rest of these (see SD-451)
          case Bson.Regex(_, _)     => -\/(UnsupportedJS(bson.shows))
          case Bson.Symbol(_)       => -\/(UnsupportedJS(bson.shows))
          case Bson.Undefined       => \/-(jscore.ident("undefined"))

          case _ => -\/(NonRepresentableInJS(bson.shows))
        }
      }

      {
        case expr @ $includeF()      => -\/(NonRepresentableInJS(expr.toString))
        case $varF(dv)               => \/-(dv.toJs)
        case $addF(l, r)             => binop(jscore.Add, l, r)
        case $andF(f, s, o @ _*)     =>
          \/-(NonEmptyList(f, s +: o: _*).foldLeft1((l, r) =>
            JsFn(JsFn.defaultName, jscore.BinOp(jscore.And, l(jscore.Ident(JsFn.defaultName)), r(jscore.Ident(JsFn.defaultName))))))
        case $condF(t, c, a)         =>
          \/-(JsFn(JsFn.defaultName,
              jscore.If(t(jscore.Ident(JsFn.defaultName)), c(jscore.Ident(JsFn.defaultName)), a(jscore.Ident(JsFn.defaultName)))))
        case $divideF(l, r)          => binop(jscore.Div, l, r)
        case $eqF(l, r)              => binop(jscore.Eq, l, r)
        case $gtF(l, r)              => binop(jscore.Gt, l, r)
        case $gteF(l, r)             => binop(jscore.Gte, l, r)
        case $literalF(bson)         => const(bson).map(l => JsFn.const(l))
        case $ltF(l, r)              => binop(jscore.Lt, l, r)
        case $lteF(l, r)             => binop(jscore.Lte, l, r)
        case expr @ $metaF()          => -\/(NonRepresentableInJS(expr.toString))
        case $multiplyF(l, r)        => binop(jscore.Mult, l, r)
        case $modF(l, r)             => binop(jscore.Mod, l, r)
        case $neqF(l, r)             => binop(jscore.Neq, l, r)
        case $notF(a)                => unop(jscore.Not, a)
        case $orF(f, s, o @ _*)      =>
          \/-(NonEmptyList(f, s +: o: _*).foldLeft1((l, r) =>
            JsFn(JsFn.defaultName, jscore.BinOp(jscore.Or, l(jscore.Ident(JsFn.defaultName)), r(jscore.Ident(JsFn.defaultName))))))

        case $concatF(f, s, o @ _*)  =>
          \/-(NonEmptyList(f, s +: o: _*).foldLeft1((l, r) =>
            JsFn(JsFn.defaultName, jscore.BinOp(jscore.Add, l(jscore.Ident(JsFn.defaultName)), r(jscore.Ident(JsFn.defaultName))))))
        case $substrF(f, start, len) =>
          \/-(JsFn(JsFn.defaultName,
            jscore.Call(
              jscore.Select(f(jscore.Ident(JsFn.defaultName)), "substr"),
              List(start(jscore.Ident(JsFn.defaultName)), len(jscore.Ident(JsFn.defaultName))))))
        case $subtractF(l, r)        => binop(jscore.Sub, l, r)
        case $toLowerF(a)            => invoke(a, "toLowerCase")
        case $toUpperF(a)            => invoke(a, "toUpperCase")

        case $yearF(a)               => invoke(a, "getUTCFullYear")
        // case $dayOfYear(a)           => // TODO: no JS equivalent
        case $monthF(a)              => expr1(a)(x =>
          jscore.BinOp(jscore.Add,
            jscore.Call(jscore.Select(x, "getUTCMonth"), Nil),
            jscore.Literal(Js.Num(1, false))))
        case $dayOfMonthF(a)         => invoke(a, "getUTCDate")
        // case $week(a)                => // TODO: no JS equivalent
        case $dayOfWeekF(a)          => expr1(a)(x =>
          jscore.BinOp(jscore.Add,
            jscore.Call(jscore.Select(x, "getUTCDay"), Nil),
            jscore.Literal(Js.Num(1, false))))
        case $hourF(a)               => invoke(a, "getUTCHours")
        case $minuteF(a)             => invoke(a, "getUTCMinutes")
        case $secondF(a)             => invoke(a, "getUTCSeconds")
        case $millisecondF(a)        => invoke(a, "getUTCMilliseconds")

        // TODO: implement the rest of these and remove the catch-all (see SD-451)
        case expr                    => -\/(UnsupportedJS(expr.toString))
      }
    }

    def rewriteRefs0(applyVar: PartialFunction[DocVar, DocVar]) = {
      case $varF(f) => applyVar.lift(f).map(fp.$var)
      case _        => None
    }
  }

  /** "Fixed" constructors, with the corecursive type and the coproduct type
    * captured when an instance is created.
    */
  final case class fixpoint[T, EX[_]: Functor]
    (embed: EX[T] => T)
    (implicit I: ExprOpCoreF :<: EX) {
    @inline private def convert(expr: ExprOpCoreF[T]): T = embed(I.inj(expr))

    def $include(): T                    = convert($includeF[T]())
    def $var(docVar: DocVar): T          = convert($varF[T](docVar))

    def $and(first: T, second: T, others: T*): T
                                         = convert($andF(first, second, others: _*))
    def $or(first: T, second: T, others: T*): T
                                         = convert($orF(first, second, others: _*))
    def $not(value: T): T                = convert($notF(value))

    def $setEquals(left: T, right: T): T = convert($setEqualsF(left, right))
    def $setIntersection(left: T, right: T): T
                                         = convert($setIntersectionF(left, right))
    def $setDifference(left: T, right: T): T
                                         = convert($setDifferenceF(left, right))
    def $setUnion(left: T, right: T): T  = convert($setUnionF(left, right))
    def $setIsSubset(left: T, right: T): T
                                         = convert($setIsSubsetF(left, right))

    def $anyElementTrue(value: T): T     = convert($anyElementTrueF(value))
    def $allElementsTrue(value: T): T    = convert($allElementsTrueF(value))

    def $cmp(left: T, right: T): T       = convert($cmpF(left, right))
    def $eq(left: T, right: T): T        = convert($eqF(left, right))
    def $gt(left: T, right: T): T        = convert($gtF(left, right))
    def $gte(left: T, right: T): T       = convert($gteF(left, right))
    def $lt(left: T, right: T): T        = convert($ltF(left, right))
    def $lte(left: T, right: T): T       = convert($lteF(left, right))
    def $neq(left: T, right: T): T       = convert($neqF(left, right))

    def $add(left: T, right: T): T       = convert($addF(left, right))
    def $divide(left: T, right: T): T    = convert($divideF(left, right))
    def $mod(left: T, right: T): T       = convert($modF(left, right))
    def $multiply(left: T, right: T): T  = convert($multiplyF(left, right))
    def $subtract(left: T, right: T): T  = convert($subtractF(left, right))

    def $concat(first: T, second: T, others: T*): T
                                         = convert($concatF(first, second, others: _*))
    def $strcasecmp(left: T, right: T): T
                                         = convert($strcasecmpF(left, right))
    def $substr(value: T, start: T, count: T): T
                                         = convert($substrF(value, start, count))
    def $toLower(value: T): T            = convert($toLowerF(value))
    def $toUpper(value: T): T            = convert($toUpperF(value))

    def $meta(): T                       = convert($metaF[T]())

    def $size(array: T): T               = convert($sizeF(array))

    def $arrayMap(input: T, as: DocVar.Name, in: T): T
                                         = convert($arrayMapF(input, as, in))
    def $let(vars: ListMap[DocVar.Name, T], in: T): T
                                         = convert($letF(vars, in))
    def $literal(value: Bson): T         = convert($literalF[T](value))

    def $dayOfYear(date: T): T           = convert($dayOfYearF(date))
    def $dayOfMonth(date: T): T          = convert($dayOfMonthF(date))
    def $dayOfWeek(date: T): T           = convert($dayOfWeekF(date))
    def $year(date: T): T                = convert($yearF(date))
    def $month(date: T): T               = convert($monthF(date))
    def $week(date: T): T                = convert($weekF(date))
    def $hour(date: T): T                = convert($hourF(date))
    def $minute(date: T): T              = convert($minuteF(date))
    def $second(date: T): T              = convert($secondF(date))
    def $millisecond(date: T): T         = convert($millisecondF(date))

    def $cond(predicate: T, ifTrue: T, ifFalse: T): T
                                         = convert($condF(predicate, ifTrue, ifFalse))
    def $ifNull(expr: T, replacement: T): T
                                         = convert($ifNullF(expr, replacement))

    val $$ROOT: T    = $var(DocVar.ROOT())
    val $$CURRENT: T = $var(DocVar.CURRENT())

    // FIXME: used only by tests and should live in src/test somewhere
    def $field(field: String, others: String*): T =
      $var(DocField(others.map(BsonField.Name(_)).foldLeft[BsonField](BsonField.Name(field))(_ \ _)))
  }
}

// "Unfixed" constructors/extractors, which inject/project ops into an arbitrary
// expression type, but handle any type for the recursive arguments.

object $includeF {
  def apply[EX[_], A]()(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$includeF[A]())
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Boolean =
    I.prj(expr) match {
      case Some(ExprOpCoreF.$includeF()) => true
      case _                             => false
    }
}
object $varF {
  def apply[EX[_], A](docVar: DocVar)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$varF[A](docVar))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[DocVar] =
    I.prj(expr) collect {
      case ExprOpCoreF.$varF(docVar) => docVar
    }
}

object $andF {
  def apply[EX[_], A](first: A, second: A, others: A*)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$andF[A](first, second, others: _*))
  def unapplySeq[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A, Seq[A])] =
    I.prj(expr) collect {
      case ExprOpCoreF.$andF(first, second, others @ _*) => (first, second, others.toList)
    }
}
object $orF {
  def apply[EX[_], A](first: A, second: A, others: A*)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$orF[A](first, second, others: _*))
  def unapplySeq[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A, Seq[A])] =
    I.prj(expr) collect {
      case ExprOpCoreF.$orF(first, second, others @ _*) => (first, second, others.toList)
    }
}

object $notF {
  def apply[EX[_], A](value: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$notF[A](value))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$notF(value) => value
    }
}

object $setEqualsF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$setEqualsF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$setEqualsF(left, right) => (left, right)
    }
}
object $setIntersectionF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$setIntersectionF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$setIntersectionF(left, right) => (left, right)
    }
}
object $setDifferenceF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$setDifferenceF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$setDifferenceF(left, right) => (left, right)
    }
}
object $setUnionF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$setUnionF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$setUnionF(left, right) => (left, right)
    }
}
object $setIsSubsetF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$setIsSubsetF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$setIsSubsetF(left, right) => (left, right)
    }
}

object $anyElementTrueF {
  def apply[EX[_], A](value: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$anyElementTrueF[A](value))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$anyElementTrueF(value) => value
    }
}
object $allElementsTrueF {
  def apply[EX[_], A](value: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$allElementsTrueF[A](value))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$anyElementTrueF(value) => value
    }
}

object $cmpF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$cmpF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$cmpF(left, right) => (left, right)
    }
}
object $eqF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$eqF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$eqF(left, right) => (left, right)
    }
}
object $gtF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$gtF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$gtF(left, right) => (left, right)
    }
}
object $gteF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$gteF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$gteF(left, right) => (left, right)
    }
}
object $ltF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$ltF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$ltF(left, right) => (left, right)
    }
}
object $lteF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$lteF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$lteF(left, right) => (left, right)
    }
}
object $neqF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$neqF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$neqF(left, right) => (left, right)
    }
}

object $addF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$addF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$addF(left, right) => (left, right)
    }
}
object $divideF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$divideF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$divideF(left, right) => (left, right)
    }
}
object $modF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$modF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$modF(left, right) => (left, right)
    }
}
object $multiplyF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$multiplyF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$multiplyF(left, right) => (left, right)
    }
}
object $subtractF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$subtractF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$subtractF(left, right) => (left, right)
    }
}

object $concatF {
  def apply[EX[_], A](first: A, second: A, others: A*)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$concatF[A](first, second, others: _*))
  def unapplySeq[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A, Seq[A])] =
    I.prj(expr) collect {
      case ExprOpCoreF.$concatF(first, second, others @ _*) => (first, second, others.toList)
    }
}
object $strcasecmpF {
  def apply[EX[_], A](left: A, right: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$strcasecmpF[A](left, right))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$strcasecmpF(left, right) => (left, right)
    }
}
object $substrF {
  def apply[EX[_], A](value: A, start: A, count: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$substrF[A](value, start, count))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$substrF(value, start, count) => (value, start, count)
    }
}
object $toLowerF {
  def apply[EX[_], A](value: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$toLowerF[A](value))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$toLowerF(value) => value
    }
}
object $toUpperF {
  def apply[EX[_], A](value: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$toUpperF[A](value))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$toUpperF(value) => value
    }
}

object $metaF {
  def apply[EX[_], A]()(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$metaF[A]())
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Boolean =
    I.prj(expr) match {
      case Some(ExprOpCoreF.$metaF()) => true
      case _                    => false
    }
}

object $sizeF {
  def apply[EX[_], A](array: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$sizeF[A](array))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$sizeF(array) => array
    }
}

object $arrayMapF {
  def apply[EX[_], A](input: A, as: DocVar.Name, in: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$arrayMapF[A](input, as, in))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, DocVar.Name, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$arrayMapF(input, as, in) => (input, as, in)
    }
}
object $letF {
  def apply[EX[_], A](vars: ListMap[DocVar.Name, A], in: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$letF[A](vars, in))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(ListMap[DocVar.Name, A], A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$letF(vars, in) => (vars, in)
    }
}
object $literalF {
  def apply[EX[_], A](value: Bson)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$literalF[A](value))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[Bson] =
    I.prj(expr) collect {
      case ExprOpCoreF.$literalF(value) => value
    }
}

object $dayOfYearF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$dayOfYearF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$dayOfYearF(date) => date
    }
}
object $dayOfMonthF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$dayOfMonthF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$dayOfMonthF(date) => date
    }
}
object $dayOfWeekF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$dayOfWeekF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$dayOfWeekF(date) => date
    }
}
object $yearF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$yearF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$yearF(date) => date
    }
}
object $monthF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$monthF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$monthF(date) => date
    }
}
object $weekF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$weekF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$weekF(date) => date
    }
}
object $hourF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$hourF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$hourF(date) => date
    }
}
object $minuteF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$minuteF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$minuteF(date) => date
    }
}
object $secondF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$secondF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$secondF(date) => date
    }
}
object $millisecondF {
  def apply[EX[_], A](date: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$millisecondF[A](date))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[A] =
    I.prj(expr) collect {
      case ExprOpCoreF.$millisecondF(date) => date
    }
}

object $condF {
  def apply[EX[_], A](predicate: A, ifTrue: A, ifFalse: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$condF[A](predicate, ifTrue, ifFalse))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$condF(pred, t, f) => (pred, t, f)
    }
}
object $ifNullF {
  def apply[EX[_], A](expr: A, replacement: A)(implicit I: ExprOpCoreF :<: EX): EX[A] =
    I.inj(ExprOpCoreF.$ifNullF[A](expr, replacement))
  def unapply[EX[_], A](expr: EX[A])(implicit I: ExprOpCoreF :<: EX): Option[(A, A)] =
    I.prj(expr) collect {
      case ExprOpCoreF.$ifNullF(expr, replacement) => (expr, replacement)
    }
}

// "Fixed" constructors/extractors inject/project to/from an arbitrary
// expression type, and also in/out of an arbitrary (Co)Recursive type.
// NB: for now, only the handful of extractors we actually use are defined here,
// and the constructors are defined in the companion's `fixpoint` class.
object $include {
  def unapply[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], EX: Functor[EX], I: ExprOpCoreF :<: EX): Boolean =
    $includeF.unapply(T.project(expr))
}
object $var {
  def unapply[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], EX: Functor[EX], I: ExprOpCoreF :<: EX): Option[DocVar] =
    $varF.unapply(T.project(expr))
}

object $and {
  def unapplySeq[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], EX: Functor[EX], I: ExprOpCoreF :<: EX): Option[(T, T, Seq[T])] =
    $andF.unapplySeq(T.project(expr))
}
object $or {
  def unapplySeq[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], EX: Functor[EX], I: ExprOpCoreF :<: EX): Option[(T, T, Seq[T])] =
    $orF.unapplySeq(T.project(expr))
}

object $lt {
  def unapply[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], EX: Functor[EX], I: ExprOpCoreF :<: EX): Option[(T, T)] =
    $ltF.unapply(T.project(expr))
}
object $lte {
  def unapply[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], EX: Functor[EX], I: ExprOpCoreF :<: EX): Option[(T, T)] =
    $lteF.unapply(T.project(expr))
}

object $add {
  def unapply[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], EX: Functor[EX], I: ExprOpCoreF :<: EX): Option[(T, T)] =
    $addF.unapply(T.project(expr))
}

object $literal {
  def unapply[T, EX[_]](expr: T)(implicit T: Recursive.Aux[T, EX], EX: Functor[EX], I: ExprOpCoreF :<: EX): Option[Bson] =
    $literalF.unapply(T.project(expr))
}
