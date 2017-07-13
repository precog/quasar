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

package quasar.concurrent

import slamdata.Predef._
import java.util.concurrent.{Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

/**
 * From playframework's NamedThreadFactory
 *
 * Thread factory that creates threads that are named.  Threads will be named with the format:
 *
 * {name}-{threadNo}
 *
 * where threadNo is an integer starting from one.
 */
final case class NamedDaemonThreadFactory(name: String) extends ThreadFactory {
  val threadNo = new AtomicInteger()
  val backingThreadFactory = Executors.defaultThreadFactory()

  def newThread(r: java.lang.Runnable) = {
    val thread = backingThreadFactory.newThread(r)
    thread.setName(name + "-" + threadNo.incrementAndGet().toString)
    thread.setDaemon(true)
    thread
  }
}
