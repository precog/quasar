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

package quasar.niflheim

import org.objectweb.howl.log._

import org.slf4s.Logging

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledExecutorService

import scala.collection.immutable.SortedMap

object CookStateLog {
  final val lockName = "txLog"
  final val logName = "CookStateLog"
}

class CookStateLog(baseDir: File, scheduler: ScheduledExecutorService) extends Logging {
  import CookStateLog._

  private[this] val workLock = FileLock(baseDir, lockName)

  private[this] val txLogConfig = new Configuration()
  txLogConfig.setLogFileDir(baseDir.getCanonicalPath)
  txLogConfig.setLogFileName(logName)
  txLogConfig.setLogFileMode("rwd") // Force file sync to underlying hardware
  txLogConfig.setChecksumEnabled(true)
  // txLogConfig.setScheduler(scheduler)

  private[this] val txLog = new Logger(txLogConfig)
  txLog.open()
  txLog.setAutoMark(false) // We only mark when we're ready to write to a new raw log

  def close() = {
    if (pendingCookIds0.size > 0) {
      log.warn("Closing txLog with pending cooks: " + pendingCookIds0.keys.mkString("[", ", ", "]"))
    }
    txLog.close()
    workLock.release()
  }

  // Maps from blockId to txKey
  private[this] var pendingCookIds0 = SortedMap.empty[Long, Long]

  private[this] var currentBlockId0 = -1l

  def pendingCookIds: List[Long] = pendingCookIds0.keys.toList

  def currentBlockId: Long = currentBlockId0

  // Run from the last mark to reconstruct state
  txLog.replay(new ReplayListener {
    // We need to provide the record that will be filled in on each callback
    // Currently all actions are 10 bytes of data
    val record = new LogRecord(10)

    def getLogRecord = record
    def onError(e: LogException) = {
      log.error("Error reading TX log", e)
      throw e
    }
    def onRecord(r: LogRecord) = {
      r.`type` match {
        case LogRecordType.END_OF_LOG =>
          log.debug("TXLog Replay complete in " + baseDir.getCanonicalPath)

        case LogRecordType.USER =>
          TXLogEntry(r) match {
            case StartCook(blockId) =>
              pendingCookIds0 += (blockId -> r.key)
              currentBlockId0 = currentBlockId0 max blockId

            case CompleteCook(blockId) =>
              pendingCookIds0 -= blockId
              currentBlockId0 = currentBlockId0 max blockId
          }

        case other =>
          log.warn("Unknown LogRecord type: " + other)
      }
    }
  })

  currentBlockId0 += 1

  def startCook(blockId: Long) = {
    val txKey = txLog.put(TXLogEntry.toBytes(StartCook(blockId)), true)
    pendingCookIds0 += (blockId -> txKey)

    // Redundant, but consistent
    currentBlockId0 = currentBlockId0 max (blockId + 1)
  }

  def completeCook(blockId: Long) = {
    assert(pendingCookIds0 contains blockId)

    val completeTxKey = txLog.put(TXLogEntry.toBytes(CompleteCook(blockId)), true)

    // Remove the entry from pending map and advance the mark to the
    // lowest remaining txKey, or the txKey of the completion if there
    // are no more outstanding cooks. This may not actually move the
    // mark if cooks are performed out-of-order.
    pendingCookIds0 -= blockId

    txLog.mark(pendingCookIds0.headOption match {
      case Some((_, txKey)) => txKey
      case None => completeTxKey
    })
  }
}


sealed trait TXLogEntry {
  def blockId: Long
}

case class StartCook(blockId: Long) extends TXLogEntry
case class CompleteCook(blockId: Long) extends TXLogEntry

object TXLogEntry extends Logging {
  def apply(record: LogRecord) = {
    val buffer = ByteBuffer.wrap(record.getFields()(0))

    buffer.getShort match {
      case 0x1 => StartCook(buffer.getLong)
      case 0x2 => CompleteCook(buffer.getLong)
      case other => log.error("Unknown TX log record type = %d, isCTRL = %s, isEOB = %s from %s".format(other, record.isCTRL, record.isEOB, record.data.mkString("[", ", ", "]")))
    }
  }

  def toBytes(entry: TXLogEntry): Array[Array[Byte]] = {
    val (tpe, size) = entry match {
      case StartCook(blockId) => (0x1, 42)
      case CompleteCook(blockId) => (0x2, 42)
    }

    val record = new Array[Byte](size)
    val buffer = ByteBuffer.wrap(record)
    buffer.clear
    buffer.putShort(tpe.toShort)
    buffer.putLong(entry.blockId)

    Array[Array[Byte]](record)
  }
}


