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

package quasar.fp

/** Builds nested scalaz Coproducts.
 *  Use like:
 *     *    type MarkLogicFs[A] = (
 *          Task
 *      :\: SessionIO
 *      :\: ContentSourceIO
 *      :\: GenUUID
 *      :\: MonotonicSeq
 *      :\: MLReadHandles
 *      :\: MLWriteHandles
 *      :/: MLResultHandles
 *    )#M[A]
 */

sealed abstract class CoM                             { type M[A]                               }
sealed abstract class :/:[F[_], G[_]]     extends CoM { type M[A] = scalaz.Coproduct[F, G, A]   }
sealed abstract class :\:[F[_], T <: CoM] extends CoM { type M[A] = scalaz.Coproduct[F, T#M, A] }
