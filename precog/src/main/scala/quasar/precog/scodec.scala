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

package quasar.precog

import scalaz._
import scodec._
import scodec.bits._
import scodec.interop.scalaz._

trait ScodecImplicits {
  implicit def ScodecErrDisjunctionSyntax[A](self: Err \/ A): ErrDisjunctionSyntax[A]                       = new ErrDisjunctionSyntax[A](self)
  implicit def ScodecAttemptSyntax[A](self: Attempt[A]): AttemptSyntax[A]                                   = new AttemptSyntax[A](self)
  implicit def ScodecCodecSyntax[A](self: Codec[A]): CodecSyntax[A]                                         = new CodecSyntax[A](self)
  implicit def ScodecBitVectorMonoidInstance: Monoid[BitVector]                                             = BitVectorMonoidInstance
  implicit def ScodecBitVectorShowInstance: Show[BitVector]                                                 = BitVectorShowInstance
  implicit def ScodecBitVectorEqualInstance: Equal[BitVector]                                               = BitVectorEqualInstance
  implicit def ScodecByteVectorMonoidInstance: Monoid[ByteVector]                                           = ByteVectorMonoidInstance
  implicit def ScodecByteVectorShowInstance: Show[ByteVector]                                               = ByteVectorShowInstance
  implicit def ScodecByteVectorEqualInstance: Equal[ByteVector]                                             = ByteVectorEqualInstance
  implicit def ScodecAttemptInstance: Monad[Attempt] with Traverse[Attempt]                                 = AttemptInstance
  implicit def ScodecAttemptShowInstance[A]: Show[Attempt[A]]                                               = AttemptShowInstance[A]
  implicit def ScodecAttemptEqualInstance[A]: Equal[Attempt[A]]                                             = AttemptEqualInstance[A]
  implicit def ScodecDecodeResultTraverseComonadInstance: Traverse[DecodeResult] with Comonad[DecodeResult] = DecodeResultTraverseComonadInstance
  implicit def ScodecDecodeResultShowInstance[A]: Show[DecodeResult[A]]                                     = DecodeResultShowInstance[A]
  implicit def ScodecDecodeResultEqualInstance[A]: Equal[DecodeResult[A]]                                   = DecodeResultEqualInstance[A]
  implicit def ScodecDecoderMonadInstance: Monad[Decoder]                                                   = DecoderMonadInstance
  implicit def ScodecDecoderMonoidInstance[A](implicit A: Monoid[A]): Monoid[Decoder[A]]                    = DecoderMonoidInstance[A]
  implicit def ScodecDecoderShowInstance[A]: Show[Decoder[A]]                                               = DecoderShowInstance[A]
  implicit def ScodecEncoderCovariantInstance: Contravariant[Encoder]                                       = EncoderCovariantInstance
  implicit def ScodecEncoderCorepresentableAttemptInstance: Corepresentable[Encoder, Attempt[BitVector]]    = EncoderCorepresentableAttemptInstance
  implicit def ScodecEncoderShowInstance[A]: Show[Encoder[A]]                                               = EncoderShowInstance[A]
  implicit def ScodecGenCodecProfunctorInstance: Profunctor[GenCodec]                                       = GenCodecProfunctorInstance
  implicit def ScodecGenCodecShowInstance[A, B]: Show[GenCodec[A, B]]                                       = GenCodecShowInstance[A, B]
  implicit def ScodecCodecInvariantFunctorInstance: InvariantFunctor[Codec]                                 = CodecInvariantFunctorInstance
  implicit def ScodecCodecShowInstance[A]: Show[Codec[A]]                                                   = CodecShowInstance[A]
}

