package slamdata.engine

import scala.collection.immutable.ListMap

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}

import slamdata.engine.analysis.fixplate._
import slamdata.engine.fp._
import slamdata.engine.javascript.Js
import slamdata.engine.javascript.JsCore

sealed trait Data {
  def dataType: Type
  def toJs: Term[JsCore]
}

object Data {
  case object Null extends Data {
    def dataType = Type.Null
    def toJs = JsCore.Literal(Js.Null).fix
  }

  case class Str(value: String) extends Data {
    def dataType = Type.Str
    def toJs = JsCore.Literal(Js.Str(value)).fix
  }

  sealed trait Bool extends Data {
    def dataType = Type.Bool
  }
  object Bool extends (Boolean => Bool) {
    def apply(value: Boolean): Bool = if (value) True else False
    def unapply(value: Bool): Option[Boolean] = value match {
      case True => Some(true)
      case False => Some(false)
    }
  }
  case object True extends Bool {
    def toJs = JsCore.Literal(Js.Bool(true)).fix
  }
  case object False extends Bool {
    override def toString = "Data.False"
    def toJs = JsCore.Literal(Js.Bool(false)).fix
  }

  sealed trait Number extends Data
  object Number {
    def unapply(value: Data): Option[BigDecimal] = value match {
      case Int(value) => Some(BigDecimal(value))
      case Dec(value) => Some(value)
      case _ => None
    }
  }
  case class Dec(value: BigDecimal) extends Number {
    def dataType = Type.Dec
    def toJs = JsCore.Literal(Js.Num(value.doubleValue, true)).fix
  }
  case class Int(value: BigInt) extends Number {
    def dataType = Type.Int
    def toJs = JsCore.Literal(Js.Num(value.doubleValue, false)).fix
  }

  case class Obj(value: Map[String, Data]) extends Data {
    def dataType = (value.map {
      case (name, data) => Type.NamedField(name, data.dataType)
    }).foldLeft[Type](Type.Top)(_ & _)
    def toJs =
      JsCore.Obj(value.toList.map { case (k, v) => k -> v.toJs }.toListMap).fix
  }

  case class Arr(value: List[Data]) extends Data {
    def dataType = (value.zipWithIndex.map {
      case (data, index) => Type.IndexedElem(index, data.dataType)
    }).foldLeft[Type](Type.Top)(_ & _)
    def toJs = JsCore.Arr(value.map(_.toJs)).fix
  }

  case class Set(value: List[Data]) extends Data {
    def dataType = (value.headOption.map { head =>
      value.drop(1).map(_.dataType).foldLeft(head.dataType)(Type.lub _)
    }).getOrElse(Type.Bottom) // TODO: ???
    def toJs = JsCore.Arr(value.map(_.toJs)).fix
  }

  case class Timestamp(value: Instant) extends Data {
    def dataType = Type.Timestamp
    def toJs = JsCore.New("Date", List(JsCore.Literal(Js.Str(value.toString)).fix)).fix
  }

  case class Date(value: LocalDate) extends Data {
    def dataType = Type.Date
    def toJs = JsCore.New("Date", List(JsCore.Literal(Js.Str(value.toString)).fix)).fix
  }

  case class Time(value: LocalTime) extends Data {
    def dataType = Type.Time
    def toJs = JsCore.Literal(Js.Str(value.toString)).fix
  }

  case class Interval(value: Duration) extends Data {
    def dataType = Type.Interval
    def toJs = JsCore.Literal(Js.Num(value.getSeconds*1000 + value.getNano*1e-6, true)).fix
  }

  case class Binary(value: Array[Byte]) extends Data {
    def dataType = Type.Binary
    def toJs =
      JsCore.Arr(value.toList.map(x =>
        JsCore.Literal(Js.Num(x.doubleValue, false)).fix)).fix
  }
}
