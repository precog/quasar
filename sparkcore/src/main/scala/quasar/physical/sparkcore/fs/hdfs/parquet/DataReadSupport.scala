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

import quasar.Predef._
import quasar.Data

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
import scalaz._

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
  "org.wartremover.warts.NoNeedForMonad",
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
        case OriginalType.DATE => new DataDateConverter(field.getName(), save)
        case OriginalType.TIME_MILLIS => new DataTimeConverter(field.getName(), save)
        case OriginalType.TIMESTAMP_MILLIS => new DataTimestampConverter(field.getName(), save)
        case OriginalType.LIST =>
          new DataListConverter(field.asGroupType(), field.getName(), this)
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
      parent.save(name, Data.Arr(values.toList))
      values.clear()
    }
  }


  private class DataStringConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {

    override def addBinary(v: Binary): Unit =
      save(name, Data.Str(new String(v.getBytes())) : Data)
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

  private class DataTimeConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addInt(v: Int): Unit =
      save(name, Data.Time(LocalTime.ofNanoOfDay((v * 100).toLong)) : Data)
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
