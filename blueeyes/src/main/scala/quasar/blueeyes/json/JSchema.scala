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

import quasar.blueeyes._


/*
sealed trait JSchema { self =>
  def validate(value: JValue): Boolean

  def | (that: JSchema): JSchema = JEitherSchema(self, that)

  def |+| (that: JSchema): JSchema = (self | that).minimize

  def unfixed: JSchema = minimize(0L)

  def minimize(fixedBound: Long): JSchema = minimize(Some(fixedBound))

  def minimize: JSchema = minimize(None)

  def fuse(that: JSchema): Option[JSchema] = (self, that) match {
    case (x, y) if (x == y) => Some(x)

    case (JFixedSchema(_: JBool), JBoolSchema) => Some(JBoolSchema)
    case (JBoolSchema, JFixedSchema(_: JBool)) => Some(JBoolSchema)

    case (JFixedSchema(_: JNum), JNumSchema) => Some(JNumSchema)
    case (JNumSchema, JFixedSchema(_: JNum)) => Some(JNumSchema)

    case (JFixedSchema(_: JString), JStringSchema) => Some(JStringSchema)
    case (JStringSchema, JFixedSchema(_: JString)) => Some(JStringSchema)

    case (JFixedSchema(JNull), JNullSchema) => Some(JNullSchema)
    case (JNullSchema, JFixedSchema(JNull)) => Some(JNullSchema)

    case (JFixedSchema(JUndefined), JUndefinedSchema) => Some(JUndefinedSchema)
    case (JUndefinedSchema, JFixedSchema(JUndefined)) => Some(JUndefinedSchema)

    case (JUndefinedSchema, x) => Some(x)
    case (x, JUndefinedSchema) => Some(x)

    case (x: JObjectSchema, y: JObjectSchema) =>
      sys.error("TODO")

    case _ => None
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema
}

case object JSchema {
  lazy val JSON: JSchema = JNullSchema | JBoolSchema | JNumSchema | JStringSchema | JObjectValueSchema(JSON) | JSetSchema(JSON)

  def fixed(value: JValue): JSchema = value match {
    case JArray(elements) =>
      lazy val uniqueEls = Set(elements: _*)

      if (elements.size > 1 && uniqueEls.size == 1) JSetSchema(fixed(elements.head))
      else JArraySchema(Array(elements.map(v => Need(fixed(v))): _*))

    case JObject(fields) =>
      JObjectSchema(fields.mapValues(v => Need(fixed(v))))

    case x => JFixedSchema(value)
  }

  def unfixed(value: JValue): JSchema = value match {
    case JNull => JNullSchema

    case JUndefined => JUndefinedSchema

    case _ : JBool => JBoolSchema

    case _ : JNum => JNumSchema

    case _ : JString => JStringSchema

    case JArray(elements) =>
      lazy val elSchemas = elements.map(unfixed _)
      lazy val uniqSchemas = Set(elSchemas: _*)

      if (elements.size == 0) JArraySchema(Array.empty[Need[JSchema]])
      else if (elements.size > 1 && uniqSchemas.size == 1) JSetSchema(uniqSchemas.head)
      else JArraySchema(Array(elSchemas.map(v => Need(v)): _*))

    case JObject(fields) => JObjectSchema(fields.mapValues(v => Need(unfixed(v))))
  }
}

case class JFixedSchema(schema: JValue) extends JSchema {
  def validate(value: JValue): Boolean = (value == schema)

  private[json] def minimize(fixedBound: Option[Long]): JSchema = if (fixedBound.getOrElse(Long.MaxValue) <= 0) JSchema.unfixed(schema) else this
}

case class JObjectValueSchema(private val schema0: Need[JSchema]) extends JSchema {
  def validate(value: JValue): Boolean = value match {
    case JObject(fields) => fields.values.forall(schema0.value.validate)

    case _ => false
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = JObjectValueSchema(schema0.value.minimize(fixedBound))
}
object JObjectValueSchema {
  def apply(valueSchema: => JSchema): JObjectValueSchema = new JObjectValueSchema(Need(valueSchema))
}

case class JObjectSchema(private val schema0: Map[String, Need[JSchema]]) extends JSchema {
  lazy val schema: Map[String, JSchema] = schema0.mapValues(_.value)
  private val schemaKeysSet = schema0.keySet

  def validate(value: JValue): Boolean = {
     value match {
      case JObject(fields) =>
        fields forall {
          case (k, v) => schema.get(k).exists(_ validate v)
        }

      case _ => false
    }
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = JObjectSchema(schema.mapValues(v => Need(v.minimize(fixedBound))))
}

case class JSetSchema(private val schema0: Need[JSchema]) extends JSchema {
  def validate(value: JValue): Boolean = value match {
    case JArray(elements) => elements.forall(schema0.value.validate)

    case _ => false
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = JSetSchema(schema0.value.minimize)
}
object JSetSchema {
  def apply(schema: => JSchema): JSetSchema = new JSetSchema(Need(schema))
}

case class JArraySchema(private val schemas0: Array[Need[JSchema]]) extends JSchema {
  private lazy val schemas: Seq[JSchema] = schemas0.map(_.value)

  def validate(value: JValue): Boolean = value match {
    case JArray(elements) =>
      if (elements.length != schemas.length) false
      else (schemas zip elements) forall { case (s, e) => s validate e }

    case _ => false
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = new JArraySchema(schemas0.map(_.map(_.minimize(fixedBound))))
}

case object JArraySchema {
  def apply(schemas: Seq[Need[JSchema]]): JArraySchema = new JArraySchema(schemas.toArray)
}

case object JNumSchema extends JSchema {
  def validate(value: JValue): Boolean = value match {
    case _ : JNum => true

    case _ => false
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = this
}

case object JStringSchema extends JSchema {
  def validate(value: JValue): Boolean = value match {
    case _ : JString => true
    case _ => false
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = this
}

case object JNullSchema extends JSchema {
  def validate(value: JValue): Boolean = value match {
    case JNull => true

    case _ => false
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = this
}

case object JBoolSchema extends JSchema {
  def validate(value: JValue): Boolean = value match {
    case _ : JBool => true

    case _ => false
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = this
}

case object JUndefinedSchema extends JSchema {
  def validate(value: JValue): Boolean = value match {
    case JUndefined => true

    case _ => false
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = this
}

case class JEitherSchema(private val left0: Need[JSchema], private val right0: Need[JSchema]) extends JSchema {
  def validate(value: JValue): Boolean = left0.value.validate(value) || right0.value.validate(value)

  def flatten: Set[JSchema] = {
    def flatten0(cur: JSchema, acc: Set[JSchema]): Set[JSchema] = cur match {
      case JEitherSchema(left, right) => flatten0(right.value, flatten0(left.value, acc))
      case x => acc + x
    }

    flatten0(this, Set.empty)
  }

  private[json] def minimize(fixedBound: Option[Long]): JSchema = {
    val bound = fixedBound.getOrElse(Long.MaxValue)

    val disj = flatten.map(_.minimize(fixedBound))

    val fixed = disj.collect { case x : JFixedSchema => x }

    val disj2 = if (fixed.size > bound) {
      (disj -- fixed) union (fixed.map(s => JSchema.unfixed(s.schema)))
    } else disj

    val objs: Set[JObjectSchema] = disj2.collect { case x : JObjectSchema => x }

    val disj3 = if (objs.size > bound) {
      val allFields = objs.map(_.schema.values).flatten

      (disj2 -- objs) union Set(JObjectValueSchema(JEitherSchema(allFields).minimize(fixedBound)))
    } else disj2

    JEitherSchema(disj3)
  }
}

case object JEitherSchema {
  def apply(left: => JSchema, right: => JSchema): JEitherSchema = new JEitherSchema(Need(left), Need(right))

  private[json] def apply(set: Set[JSchema]): JSchema = {
    if (set.isEmpty) JUndefinedSchema
    else if (set.size == 1) set.head
    else set.reduce[JSchema]((a, b) => JEitherSchema(a, b))
  }
}
 */
