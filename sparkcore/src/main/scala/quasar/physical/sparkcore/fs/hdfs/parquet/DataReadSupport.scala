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

package quasar.physical.sparkcore.fs.hdfs.parquet

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.fp.ski._

import java.util.{Map => JMap}
import java.time._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListMap => MListMap, MutableList => MList}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api._
import org.apache.parquet.schema.{OriginalType, MessageType, GroupType}
import scalaz._, Scalaz._

/**
  * Parquet Mutable Dragons
  *
  * Due to the nature of java-like parquet-mr API we need to deal
  * with mutable data strucutres and "protocol" that requires
  * mutability (e.g see method getCurrentRecord())
  *
  * This is private to ParquetRDD as to not expose mutability
  *
  */
@SuppressWarnings(Array(
  "org.wartremover.warts.Overloading",
  "org.wartremover.warts.Var",
  "org.wartremover.warts.MutableDataStructures",
  "org.wartremover.warts.NonUnitStatements"
))
private[parquet] class DataReadSupport extends ReadSupport[Data] with Serializable {

  override def prepareForRead(conf: Configuration,
    metaData: JMap[String, String],
    schema: MessageType,
    context: ReadContext
  ): RecordMaterializer[Data] =
    new DataRecordMaterializer(schema)

  override def init(context: InitContext): ReadContext =
    new ReadContext(context.getFileSchema())

  private class DataRecordMaterializer(schema: MessageType) extends RecordMaterializer[Data] {

    val rootConverter = new DataGroupConverter(schema, None)

    override def getCurrentRecord(): Data = rootConverter.getCurrentRecord()

    override def getRootConverter(): GroupConverter = rootConverter
  }

  private trait ConverterLike {

    def schema: GroupType

    def save: (String, Data) => Unit

    val converters: List[Converter] =
      schema.getFields().asScala.map(field => field.getOriginalType() match {
        case OriginalType.UTF8 => new DataStringConverter(field.getName(), save)
        case OriginalType.JSON => new DataJsonConverter(field.getName(), save)
        case OriginalType.DATE => new DataDateConverter(field.getName(), save)
        case OriginalType.TIME_MILLIS => new DataTimeConverter(field.getName(), save)
        case OriginalType.TIME_MICROS => new DataTimeMicroConverter(field.getName(), save)
        case OriginalType.TIMESTAMP_MILLIS => new DataTimestampConverter(field.getName(), save)
        case OriginalType.TIMESTAMP_MICROS => new DataTimestampMicroConverter(field.getName(), save)
        case OriginalType.LIST =>
          new DataListConverter(field.asGroupType(), field.getName(), this)
        case OriginalType.MAP =>
          new DataMapConverter(field.asGroupType(), field.getName(), this)
        case OriginalType.MAP_KEY_VALUE =>
          new DataMapConverter(field.asGroupType(), field.getName(), this)
        case a if !field.isPrimitive() =>
          new DataGroupConverter(field.asGroupType(), Some((field.getName(), this)))
        case _ => new DataPrimitiveConverter(field.getName(), save)
      }).toList
  }

  private class DataGroupConverter(
    val schema: GroupType,
    parent: Option[(String, ConverterLike)]
  ) extends GroupConverter with ConverterLike {

    val values: MListMap[String, Data] = MListMap()
    var record: Data = Data.Null

    def save: (String, Data) => Unit = (name: String, data: Data) => {
      values += ((name, data))
      ()
    }

    override def getConverter(fieldIndex: Int): Converter = converters.apply(fieldIndex)
    override def start(): Unit = {}
    override def end(): Unit = {
      parent.fold {
        record = Data.Obj(values.toSeq:_*)
        ()
      } { case (name, p) =>
          p.save(name, Data.Obj(values.toSeq:_*))
      }
      values.clear()
    }

    def getCurrentRecord(): Data = record

  }

  private object ListElement {
    def unapply(d: Data): Option[Data] = d match {
      case Data.Obj(lm) if lm.size === 1 && lm.isDefinedAt("element") => lm("element").some
      case _ => none
    }
  }

  private class DataListConverter(
    val schema: GroupType,
    name: String,
    parent: ConverterLike
  ) extends GroupConverter with ConverterLike {

    val values: MList[Data] = MList()

    def save: (String, Data) => Unit = (name: String, data: Data) => {
      values += data
      ()
    }

    override def getConverter(fieldIndex: Int): Converter = converters.apply(fieldIndex)
    override def start(): Unit = {}
    override def end(): Unit = {
      val normalize = values.toList.map {
        case ListElement(el) => el
        case o => o
      }
      parent.save(name, Data.Arr(normalize))
      values.clear()
    }
  }

  private object MapKey {
    def unapply(t: (String, Data)): Option[String] = t match {
      case ("key" -> Data.Str(k)) => k.some
      case _ => none
    }
  }

  private object MapValue {
    def unapply(t: (String, Data)): Option[Data] = t match {
      case ("value" -> v) => v.some
      case _ => none
    }
  }

  private class DataMapConverter(
    val schema: GroupType,
    name: String,
    parent: ConverterLike
  ) extends GroupConverter with ConverterLike {

    val values: MList[Data] = MList()

    def save: (String, Data) => Unit = (name: String, data: Data) => {
      values += data
      ()
    }

    override def getConverter(fieldIndex: Int): Converter = converters.apply(fieldIndex)
    override def start(): Unit = {}
    override def end(): Unit = {
      val mapEntry: Data => Option[(String, Data)] = {
        case Data.Obj(lm) => lm.toList match {
          case MapKey(k) :: MapValue(v) :: Nil => (k, v).some
          case MapValue(v) :: MapKey(k) :: Nil => (k, v).some
          case _ => none
        }
        case o => none
      }
      val cached = values.toList
      val data = cached.traverse(mapEntry).cata(entries => Data.Obj(entries: _*), Data.Arr(cached))
      parent.save(name, data)

      values.clear()
    }
  }

  private class DataStringConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {

    override def addBinary(v: Binary): Unit =
      save(name, Data.Str(new String(v.getBytes())) : Data)
  }

  private class DataJsonConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {

    override def addBinary(v: Binary): Unit =
      save(name, DataCodec.parse(new String(v.getBytes()))(DataCodec.Precise).fold(error => Data.NA, ι))
  }

  private class DataDateConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addInt(v: Int): Unit =
      save(name, Data.Date(LocalDate.of(1970,1,1).plusDays(v.toLong)) : Data)
  }

  private class DataTimestampConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addLong(v: Long): Unit =
      save(name, Data.Timestamp(Instant.ofEpochMilli(v)) : Data)
  }

  private class DataTimestampMicroConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addLong(v: Long): Unit =
      save(name, Data.Timestamp(Instant.ofEpochMilli(v / 1000)) : Data)
  }

  private class DataTimeConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addInt(v: Int): Unit =
      save(name, Data.Time(LocalTime.ofNanoOfDay(v.toLong * 1000000)) : Data)
  }

  private class DataTimeMicroConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addLong(v: Long): Unit =
      save(name, Data.Time(LocalTime.ofNanoOfDay(v * 1000)) : Data)
  }

  private class DataPrimitiveConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {

    override def addBoolean(v: Boolean): Unit =
      save(name, Data.Bool(v) : Data)

    override def addLong(v: Long): Unit =
      save(name, Data.Int(v) : Data)

    override def addInt(v: Int): Unit =
      save(name, Data.Int(v) : Data)

    override def addDouble(v: Double): Unit =
      save(name, Data.Dec(v) : Data)

    override def addFloat(v: scala.Float): Unit =
      save(name, Data.Dec(v) : Data)

    override def addBinary(v: Binary): Unit =
      save(name, Data.Binary(ImmutableArray.fromArray(v.getBytes())) : Data)
  }




}
