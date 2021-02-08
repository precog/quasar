/*
 * Copyright 2020 Precog Data
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

package quasar.connector

import scala._, Predef._

trait Credentials extends Product with Serializable

object Credentials {
  /** An opaque token to be interpreted in a security context. */
  final case class Token(toByteArray: Array[Byte])
      extends Credentials

  /** An X.509 certificate and PKCS#8 private key, both encoded in PEM format. */
  final case class X509CertificatePair(
      certificate: Array[Byte],
      privateKey: Array[Byte],
      privateKeyPassword: Option[String])
      extends Credentials
}
