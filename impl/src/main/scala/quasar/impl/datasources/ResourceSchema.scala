/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.datasources

import quasar.api.SchemaConfig

import cats.Contravariant

/** Attempt to discover the schema of a resource. */
trait ResourceSchema[F[_], S <: SchemaConfig, R] {
  def apply(schemaConfig: S, resource: R): F[schemaConfig.Schema]
}

object ResourceSchema {
  implicit def contravariant[F[_], S <: SchemaConfig]: Contravariant[ResourceSchema[F, S, ?]] =
    new Contravariant[ResourceSchema[F, S, ?]] {
      def contramap[A, B](rs: ResourceSchema[F, S, A])(f: B => A): ResourceSchema[F, S, B] =
        new ResourceSchema[F, S, B] {
          def apply(schemaConfig: S, resource: B): F[schemaConfig.Schema] =
            rs(schemaConfig, f(resource))
        }
    }
}
