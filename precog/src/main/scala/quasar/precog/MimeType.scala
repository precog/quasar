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

/*
Usage:

import MimeTypes._
val mimeType = image/gif
val imageMimeType = image/gif
val javaScriptMimeType = text/javascript
 */

sealed trait MimeType {
  def maintype: String
  def subtype: String

  def value: String = maintype + "/" + subtype

  def extensions: List[String] = subtype :: Nil

  def defaultExtension = extensions.head

  override def hashCode = value.hashCode

  override def equals(o: Any) = o match {
    case that: MimeType => this.value.toLowerCase == that.value.toLowerCase

    case _ => false
  }

  def satisfiesRequestFor(desired: MimeType) = {
    val ourmain = maintype
    (desired.maintype, desired.subtype) match {
      case ("*", _)         => true
      case (`ourmain`, "*") => true
      case _                => equals(desired)
    }
  }

  override def toString = value
}

object MimeType {
  def apply(maintype0: String, subtype0: String): MimeType = new MimeType {
    val maintype = maintype0.toLowerCase
    val subtype  = subtype0.toLowerCase
  }
}

object MimeTypes {

  def parseMimeTypes(inString: String): Array[MimeType] = {
    def MimeTypeRegex = """((application)|(text)|(audio)|(multipart)|(image)|(video)|(\*))/(([a-z\-]+)|\*)""".r

    /* Split the string on commas, which separate the mimes */
    for {
      baseStr <- inString.toLowerCase.split(",")
      composite <- MimeTypeRegex.findFirstIn(baseStr.trim)
      mimeType <- composite.split("/") match {
                   case Array("*", "*") => Some(anymaintype / anysubtype)

                   case Array("application", "*")          => Some(application / anysubtype)
                   case Array("application", "javascript") => Some(application / javascript)
                   case Array("text", "javascript")        => Some(text / javascript)

                   case Array("application", "x-javascript")      => Some(application / `x-javascript`)
                   case Array("application", "soap+xml")          => Some(application / `soap+xml`)
                   case Array("application", "xhtml+xml")         => Some(application / `xhtml+xml`)
                   case Array("application", "xml-dtd")           => Some(application / `xml-dtd`)
                   case Array("application", "json")              => Some(application / json)
                   case Array("application", "x-latex")           => Some(application / `x-latex`)
                   case Array("application", "octet-stream")      => Some(application / `octet-stream`)
                   case Array("application", "ogg")               => Some(application / ogg)
                   case Array("application", "pdf")               => Some(application / `pdf`)
                   case Array("application", "postscript")        => Some(application / `postscript`)
                   case Array("application", "x-dvi")             => Some(application / `x-dvi`)
                   case Array("application", "x-shockwave-flash") => Some(application / `x-shockwave-flash`)
                   case Array("application", "x-tar")             => Some(application / `x-tar`)
                   case Array("application", "x-ttf")             => Some(application / `x-ttf`)
                   case Array("application", "zip")               => Some(application / `zip`)

                   /* Audio */
                   case Array("audio", "basic")       => Some(audio / basic)
                   case Array("audio", "mp4")         => Some(audio / mp4)
                   case Array("audio", "midi")        => Some(audio / midi)
                   case Array("audio", "mpeg")        => Some(audio / mpeg)
                   case Array("audio", "vorbis")      => Some(audio / vorbis)
                   case Array("audio", "x-ms-wma")    => Some(audio / `x-ms-wma`)
                   case Array("audio", "x-ms-wax")    => Some(audio / `x-ms-wax`)
                   case Array("audio", "x-realaudio") => Some(audio / `x-realaudio`)
                   case Array("audio", "x-wav")       => Some(audio / `x-wav`)

                   /* Image */
                   case Array("image", "gif")                => Some(image / gif)
                   case Array("image", "png")                => Some(image / png)
                   case Array("image", "jpeg")               => Some(image / jpeg)
                   case Array("image", "svg+xml")            => Some(image / `svg+xml`)
                   case Array("image", "tiff")               => Some(image / tiff)
                   case Array("image", "vnd.microsoft.icon") => Some(image / `vnd.microsoft.icon`)

                   /* Multipart */
                   case Array("multipart", "mixed")       => Some(multipart / mixed)
                   case Array("multipart", "alternative") => Some(multipart / alternative)
                   case Array("multipart", "related")     => Some(multipart / related)
                   case Array("multipart", "form-data")   => Some(multipart / `form-data`)
                   case Array("multipart", "signed")      => Some(multipart / signed)
                   case Array("multipart", "encrypted")   => Some(multipart / encrypted)

                   /* Text */
                   case Array("text", "css")   => Some(text / css)
                   case Array("text", "csv")   => Some(text / csv)
                   case Array("text", "html")  => Some(text / html)
                   case Array("text", "plain") => Some(text / plain)
                   case Array("text", "x-c")   => Some(text / `x-c`)
                   case Array("text", "xml")   => Some(text / xml)

                   /* Video */
                   case Array("video", "quicktime") => Some(video / quicktime)
                   case Array("video", "x-msvideo") => Some(video / `x-msvideo`)

                   case Array(tpe, subtpe) => Some(MimeType(tpe, subtpe))
                   case _                  => None
                 }
    } yield mimeType
  }

  trait GenericType extends ProductPrefixUnmangler {
    def subtype = unmangledName
  }

  sealed abstract class StarType(val extensions: List[String])        extends GenericType
  sealed abstract class ApplicationType(val extensions: List[String]) extends GenericType
  sealed abstract class AudioType(val extensions: List[String])       extends GenericType
  sealed abstract class ImageType(val extensions: List[String])       extends GenericType
  sealed abstract class MessageType(val extensions: List[String])     extends GenericType
  sealed abstract class MultipartType(val extensions: List[String])   extends GenericType
  sealed abstract class PrsType(val extensions: List[String])         extends GenericType
  sealed abstract class TextType(val extensions: List[String])        extends GenericType
  sealed abstract class VideoType(val extensions: List[String])       extends GenericType

  /* Star Type */
  case object anysubtype extends StarType(Nil) {
    override def subtype = "*"
  }

  /* Application Types */
  sealed abstract class JavaScriptApplicationType extends ApplicationType("js" :: Nil)
  sealed abstract class OggApplicationType        extends ApplicationType("ogg" :: Nil)

  case object javascript     extends JavaScriptApplicationType
  case object `x-javascript` extends JavaScriptApplicationType
  case object `soap+xml`     extends ApplicationType("soap+xml" :: Nil)
  case object `xhtml+xml`    extends ApplicationType("xhtml+xml" :: Nil)
  case object `xml-dtd`      extends ApplicationType("xml-dtd" :: Nil)

  case object json                    extends ApplicationType("json" :: Nil)
  case object `x-latex`               extends ApplicationType("latex" :: Nil)
  case object `octet-stream`          extends ApplicationType("bin" :: "class" :: "dms" :: "exe" :: "lha" :: "lzh" :: Nil)
  case object ogg                     extends JavaScriptApplicationType
  case object pdf                     extends ApplicationType("pdf" :: Nil)
  case object postscript              extends ApplicationType("ai" :: Nil)
  case object `x-dvi`                 extends ApplicationType("dvi" :: Nil)
  case object `x-shockwave-flash`     extends ApplicationType("swf" :: Nil)
  case object `x-tar`                 extends ApplicationType("tar" :: Nil)
  case object `x-ttf`                 extends ApplicationType("ttf" :: Nil)
  case object `x-www-form-urlencoded` extends ApplicationType("x-www-form-urlencoded" :: Nil)
  case object zip                     extends ApplicationType("zip" :: Nil)

  /* Audio Types */
  sealed abstract class MpegAudioType extends AudioType("mpg" :: "mpeg" :: "mpga" :: "mpe" :: "mp3" :: "mp2" :: Nil)
  sealed abstract class Mp4AudioType  extends AudioType("mp4" :: Nil)

  case object basic         extends AudioType("au" :: "snd" :: Nil)
  case object mp4           extends Mp4AudioType
  case object midi          extends AudioType("midi" :: "mid" :: "kar" :: Nil)
  case object mpeg          extends MpegAudioType
  case object vorbis        extends AudioType("vorbis" :: Nil)
  case object `x-ms-wma`    extends AudioType("wma" :: Nil)
  case object `x-ms-wax`    extends AudioType("wax" :: Nil)
  case object `x-realaudio` extends AudioType("ra" :: Nil)
  case object `x-wav`       extends AudioType("wav" :: Nil)

  /* Image Types */
  case object gif                  extends ImageType("gif" :: Nil)
  case object png                  extends ImageType("png" :: Nil)
  case object jpeg                 extends ImageType("jpg" :: "jpeg" :: "jpe" :: Nil)
  case object `svg+xml`            extends ImageType("svg+xml" :: Nil)
  case object tiff                 extends ImageType("tiff" :: Nil)
  case object `vnd.microsoft.icon` extends ImageType("ico" :: Nil)

  /* Message Types */
  case object http              extends MessageType("http" :: Nil)
  case object `delivery-status` extends MessageType("delivery-status" :: Nil)

  /* Multipart Types */
  case object mixed       extends MultipartType("mixed" :: Nil)
  case object alternative extends MultipartType("alternative" :: Nil)
  case object related     extends MultipartType("related" :: Nil)
  case object `form-data` extends MultipartType("form-data" :: Nil)
  case object signed      extends MultipartType("signed" :: Nil)
  case object encrypted   extends MultipartType("encrypted" :: Nil)

  /* Text Types */
  case object css   extends TextType("css" :: Nil)
  case object csv   extends TextType("csv" :: Nil)
  case object html  extends TextType("html" :: "htm" :: Nil)
  case object plain extends TextType("c" :: "c++" :: "cc" :: "com" :: "conf" :: "f" :: "h" :: "jav" :: "pl" :: "text" :: "txt" :: Nil)
  case object `x-c` extends TextType("c" :: Nil)
  case object xml   extends TextType("xml" :: Nil)

  /* Video Types */
  case object quicktime   extends VideoType("qt" :: "mov" :: Nil)
  case object `x-msvideo` extends VideoType("avi" :: Nil)

  /* Implicit Conversions */
  implicit def applicationTypeJavaScript2TextTypeJavaScript(appType: JavaScriptApplicationType): TextType = {
    case object TextTypeJavaScript extends TextType(appType.extensions) {
      override def subtype = appType.subtype
    }
    TextTypeJavaScript
  }

  implicit def applicationTypeOgg2AudioTypeOgg(appType: OggApplicationType): AudioType = {
    case object AudioTypeOgg extends AudioType(appType.extensions) {
      override def subtype = appType.subtype
    }
    AudioTypeOgg
  }

  implicit def applicationTypeOgg2VideoTypeOgg(appType: OggApplicationType): VideoType = {
    case object VideoTypeOgg extends VideoType(appType.extensions) {
      override def subtype = appType.subtype
    }
    VideoTypeOgg
  }

  implicit def audioTypeMpeg2VideoTypeMpeg(audioType: MpegAudioType): VideoType = {
    case object VideoTypeMpeg extends VideoType(audioType.extensions) {
      override def subtype = audioType.subtype
    }
    VideoTypeMpeg
  }

  implicit def audioTypeMp42VideoTypeMp4(audioType: Mp4AudioType): VideoType = {
    case object VideoTypeMp4 extends VideoType(audioType.extensions) {
      override def subtype = audioType.subtype
    }
    VideoTypeMp4
  }

  /* Implicit conversions for Star (*) */

  implicit def starTypeStar2ApplicationTypeStar(starType: StarType): ApplicationType = {
    case object ApplicationTypeStar extends ApplicationType(starType.extensions) {
      override def subtype = starType.subtype
    }
    ApplicationTypeStar
  }

  implicit def starTypeStar2AudioTypeStar(starType: StarType): AudioType = {
    case object AudioTypeStar extends AudioType(starType.extensions) {
      override def subtype = starType.subtype
    }
    AudioTypeStar
  }

  implicit def starTypeStar2ImageTypeStar(starType: StarType): ImageType = {
    case object ImageTypeStar extends ImageType(starType.extensions) {
      override def subtype = starType.subtype
    }
    ImageTypeStar
  }

  implicit def starTypeStar2MessageTypeStar(starType: StarType): MessageType = {
    case object MessageTypeStar extends MessageType(starType.extensions) {
      override def subtype = starType.subtype
    }
    MessageTypeStar
  }

  implicit def starTypeStar2MultipartTypeStar(starType: StarType): MultipartType = {
    case object MultipartTypeStar extends MultipartType(starType.extensions) {
      override def subtype = starType.subtype
    }
    MultipartTypeStar
  }

  implicit def starTypeStar2PrsTypeStar(starType: StarType): PrsType = {
    case object PrsTypeStar extends PrsType(starType.extensions) {
      override def subtype = starType.subtype
    }
    PrsTypeStar
  }

  implicit def starTypeStar2TextTypeStar(starType: StarType): TextType = {
    case object TextTypeStar extends TextType(starType.extensions) {
      override def subtype = starType.subtype
    }
    TextTypeStar
  }

  implicit def starTypeStar2VideoTypeStar(starType: StarType): VideoType = {
    case object VideoTypeStar extends VideoType(starType.extensions) {
      override def subtype = starType.subtype
    }
    VideoTypeStar
  }

  /* Constructor Methods */
  object anymaintype {
    def /(starType: StarType) = new MimeType {
      def maintype            = "*"
      def subtype             = starType.subtype
      override def extensions = starType.extensions
    }
  }

  object application {
    def /(applicationType: ApplicationType) = new MimeType {
      def maintype            = "application"
      def subtype             = applicationType.subtype
      override def extensions = applicationType.extensions
    }
  }

  object audio {
    def /(audioType: AudioType) = new MimeType {
      def maintype            = "audio"
      def subtype             = audioType.subtype
      override def extensions = audioType.extensions
    }
  }

  object image {
    def /(imageType: ImageType) = new MimeType {
      def maintype            = "image"
      def subtype             = imageType.subtype
      override def extensions = imageType.extensions
    }
  }

  object message {
    def /(messageType: MessageType) = new MimeType {
      def maintype            = "message"
      def subtype             = messageType.subtype
      override def extensions = messageType.extensions
    }
  }

  object multipart {
    def /(multipartType: MultipartType) = new MimeType {
      def maintype            = "multipart"
      def subtype             = multipartType.subtype
      override def extensions = multipartType.extensions
    }
  }

  object text {
    def /(textType: TextType) = new MimeType {
      def maintype            = "text"
      def subtype             = textType.subtype
      override def extensions = textType.extensions
    }
  }

  object video {
    def /(videoType: VideoType) = new MimeType {
      def maintype            = "video"
      def subtype             = videoType.subtype
      override def extensions = videoType.extensions
    }
  }
}

trait ProductPrefixUnmangler {
  def productPrefix: String

  lazy val unmangledName = unmangleName(productPrefix)
  private def unmangleName(name: String): String = operators.foldLeft(name)((n, o) => n.replace(o._1, o._2))

  private val operators = Map(
    "$eq"      -> "=",
    "$greater" -> ">",
    "$less"    -> "<",
    "$plus"    -> "+",
    "$minus"   -> "-",
    "$times"   -> "*",
    "$div"     -> "/",
    "$bang"    -> "!",
    "$at"      -> "@",
    "$hash"    -> "#",
    "$percent" -> "%",
    "$up"      -> "^",
    "$amp"     -> "&",
    "$tilde"   -> "~",
    "$qmark"   -> "?",
    "$bar"     -> "|",
    "$bslash"  -> "\\")
}
