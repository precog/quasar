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

package quasar.pkg

@java.lang.SuppressWarnings(scala.Array("org.wartremover.warts.Overloading"))
trait PackageMethods {
  self: quasar.Predef =>

  def Cmp(n: Int): Cmp = scalaz.Ordering fromInt n

  def byteBuffer(xs: Array[Byte]): ByteBuffer                        = java.nio.ByteBuffer.wrap(xs)
  def byteBuffer(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = java.nio.ByteBuffer.wrap(xs, offset, len)
  def charBuffer(size: Int): CharBuffer                              = java.nio.CharBuffer.allocate(size)
  def charBuffer(xs: String): CharBuffer                             = java.nio.CharBuffer.wrap(xs)
  def discard[A](value: A): Unit                                     = () // for avoiding "discarding non-Unit value" warnings
  def doto[A](x: A)(f: A => Any): A                                  = { discard(f(x)) ; x }
  def jClassLoader[A: CTag]: ClassLoader                             = jClass[A].getClassLoader
  def jClass[A: CTag]: jClass                                        = classTag[A].runtimeClass
  def jPath(path: String): jPath                                     = java.nio.file.Paths get path
  def jResource(c: jClass, name: String): InputStream                = c getResourceAsStream name
  def jResource[A: CTag](name: String): InputStream                  = jResource(jClass[A], name)
  def randomBool(): Boolean                                          = scala.util.Random.nextBoolean
  def randomDouble(): Double                                         = scala.util.Random.nextDouble
  def randomInt(end: Int): Int                                       = scala.util.Random.nextInt(end)
  def randomUuid(): UUID                                             = java.util.UUID.randomUUID
  def systemMillis(): Long                                           = java.lang.System.currentTimeMillis()
  def utf8Bytes(s: String): Array[Byte]                              = s getBytes utf8Charset
  def utf8Charset: Charset                                           = java.nio.charset.Charset forName "UTF-8"
  def uuid(s: String): UUID                                          = java.util.UUID fromString s

  @inline final def classTag[A](implicit z: CTag[A]): CTag[A] = z
  @inline final def implicitly[A](implicit value: A): A       = value

  implicit def quasarExtensionOps[A](x: A): QuasarExtensionOps[A] =
    new QuasarExtensionOps(x)

  /** Type parameter curriers. */
  def eqBy[A]   = new EqualBy[A]
  def showBy[A] = new ShowBy[A]
}
