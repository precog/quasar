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

import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.{ FileChannel, FileLock => JFileLock }

trait FileLock {
  def release(): Unit
}

class FileLockException(message: String) extends Exception(message)

object FileLock {
  private case class LockHolder(channel: FileChannel, lock: JFileLock, lockFile: Option[File]) extends FileLock {
    def release() = {
      lock.release
      channel.close

      lockFile.foreach(_.delete)
    }
  }

  def apply(target: File, lockPrefix: String = "LOCKFILE"): FileLock = {
    val (lockFile, removeFile) = if (target.isDirectory) {
      val lockFile = new File(target, lockPrefix + ".lock")
      lockFile.createNewFile
      (lockFile, true)
    } else {
      (target, false)
    }

    val channel = new RandomAccessFile(lockFile, "rw").getChannel
    val lock    = channel.tryLock

    if (lock == null) {
      throw new FileLockException("Could not lock. Previous lock exists on " + target)
    }

    LockHolder(channel, lock, if (removeFile) Some(lockFile) else None)
  }
}

