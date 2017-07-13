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
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.ParquetRecordReader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.Reporter
import org.apache.spark._
import org.apache.spark.rdd._

import scala.reflect.ClassTag
import scala.collection.Iterator

class ParquetRDDPartition[T](val index: Int,
                             val readSupport: ReadSupport[T],
                             s: FileSplit,
                             c: Configuration)
    extends Partition {

  val split = new SerializableWritable[FileSplit](s)
  val conf  = new SerializableWritable[Configuration](c)
}

class RecordReaderIterator[T](prr: ParquetRecordReader[T]) extends Iterator[T] {
  override def hasNext: Boolean = prr.nextKeyValue()
  override def next(): T        = prr.getCurrentValue()
}

class ParquetRDD[T: ClassTag](
    @transient private val _sc: SparkContext,
    pathStr: String
) extends RDD[T](_sc, Nil) {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def compute(raw: Partition, context: TaskContext): Iterator[T] = {
    val partition = raw.asInstanceOf[ParquetRDDPartition[T]]
    val prr       = new ParquetRecordReader(partition.readSupport)
    prr.initialize(partition.split.value, partition.conf.value, Reporter.NULL)
    new RecordReaderIterator(prr)
  }

  override protected def getPartitions: Array[Partition] = {
    val path       = new Path(pathStr)
    val conf       = _sc.hadoopConfiguration
    val fs         = path.getFileSystem(conf)
    val fileStatus = fs.getFileStatus(path)
    val blocks     = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen())
    val readSupport = new DataReadSupport
    blocks.zipWithIndex.map {
      case (b, i) =>
        val split =
          new FileSplit(path, b.getOffset(), b.getLength(), b.getHosts())
        new ParquetRDDPartition(i, readSupport, split, conf)
    }
  }
}

object ParquetRDD {
  implicit class SparkContextOps(sc: SparkContext) {
    def parquet[T: ClassTag](pathStr: String): ParquetRDD[T] =
      new ParquetRDD[T](sc, pathStr)
  }
}
