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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xml.namespaces._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.auto._
import scalaz._, Scalaz._
import xml.name._

object lib {
  import expr.{emptySeq, for_, func, if_, let_}, axes.child
  import FunctionDecl._

  val fs = NamespaceDecl(filesystemNs)

  def formatConflictError(uri: XQuery) = {
    val msg = "A document with a different format exists at ".xs
    fn.error(filesystemError.xqy, some(fn.concat(msg, uri)))
  }

  /** filesystem:append-child-nodes($dst as node(), $nodes as node()*) as item()*
    *
    * Appends the given nodes as children of `dst` in a format-appropriate manner.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  def appendChildNodes[F[_]: PrologW: Monad, T](implicit SP: StructuralPlanner[F, T]): F[FunctionDecl2] =
    fs.declare[F]("append-child-nodes") flatMap (_(
      $("dst")   as ST("node()"),
      $("nodes") as ST("node()*")
    ).as(ST.Top) { (dst: XQuery, nodes: XQuery) =>
      val (dstType, node, key, base, i) = ($("dstType"), $("node"), $("key"), $("base"), $("i"))
      val unsupportedRoot = "Unsupported root node, expected an array or object, found: "
      val genBase = fn.string(xdmp.random(baseSeed.xqy))

      (
        SP.nodeType(dst)                                    |@|
        SP.singletonArray(~node)                            |@|
        SP.singletonObject(~key, ~node)                     |@|
        lpadToLength[F].apply("0".xs, 12.xqy, genBase)      |@|
        lpadToLength[F].apply("0".xs, 8.xqy, fn.string(~i))
      )((typ, arr, obj, pbase, pidx) =>
        let_(dstType := typ) return_ {
          if_(~dstType eq "array".xs)
          .then_(
            for_(node in nodes)
            .return_(xdmp.nodeInsertChild(dst, arr `/` child.node())))
          .else_(if_(~dstType eq "object".xs)
          .then_(
            let_(base := pbase)
            .for_(node at i in nodes)
            .let_(key := fn.concat("k".xs, ~base, pidx))
            .return_(xdmp.nodeInsertChild(dst, obj `/` child.node())))
          .else_(fn.error(filesystemError.xqy, some(fn.concat(unsupportedRoot.xs, ~dstType)))))
        })
    })

  // filesystem:descendants-having-format-exist($uri as xs:string) as xs:boolean
  def descendantsHavingFormatExist[F[_]: PrologW: Functor, T: SearchOptions]: F[FunctionDecl1] =
    fs.declare[F]("descendants-having-format-exist") map (_(
      $("uri") as ST("xs:string")
    ).as(ST("xs:boolean")) { uri: XQuery =>
      fn.exists(directoryDocuments[T](uri, true))
    })

  // filesystem:directory-contents($uri as xs:string) as xs:string*
  def directoryContents[F[_]: PrologW: Bind, T: SearchOptions](
    nextUri: (XQuery, XQuery) => XQuery
  ): F[FunctionDecl1] =
    (
      discoverChildren[F](nextUri).fn       |@|
      descendantsHavingFormatExist[F, T].fn
    ).tupled flatMap { case (discKids, descsExist) =>
      fs.declare[F]("directory-contents") map (_(
        $("uri") as ST("xs:string")
      ).as(ST("xs:string*")) { uri: XQuery =>
        val (kid, sdir) = ($("kid"), $("sdir"))

        val childFiles =
          fn.map(
            fn.ns(NCName("base-uri")) :# 1,
            directoryDocuments[T](uri, false))

        val childDirs =
          fn.filter(
            func(sdir.render) { descsExist(~sdir) },
            discKids(uri, emptySeq))

        fn.map(
          func(kid.render) { fn.substringAfter(~kid, uri) },
          mkSeq_(childDirs, childFiles))
      })
    }

  // filesystem:file-exists-in-other-format($uri as xs:string) as xs:boolean
  def fileExistsInOtherFormat[F[_]: PrologW: Functor, T: SearchOptions]: F[FunctionDecl1] =
    fs.declare[F]("file-exists-in-other-format") map (_(
      $("uri") as ST("xs:string")
    ).as(ST("xs:boolean")) { uri: XQuery =>
      if_(fn.docAvailable(uri))
      .then_(fn.empty(documentNode[T](uri)))
      .else_(fn.False)
    })

  // filesystem:lpad-to-length($padChar as xs:string, $length as xs:integer, $str as xs:string) as xs:string
  def lpadToLength[F[_]: PrologW: Functor]: F[FunctionDecl3] =
    fs.declare[F]("lpad-to-length") map (_(
      $("padchar") as ST("xs:string"),
      $("length")  as ST("xs:integer"),
      $("str")     as ST("xs:string")
    ).as(ST("xs:string")) { (padchar: XQuery, length: XQuery, str: XQuery) =>
      val (slen, padct, prefix) = ($("slen"), $("padct"), $("prefix"))
      let_(
        slen   := fn.stringLength(str),
        padct  := fn.max(mkSeq_("0".xqy, length - (~slen))),
        prefix := fn.stringJoin(for_($("_") in (1.xqy to ~padct)) return_ padchar, "".xs))
      .return_(
        fn.concat(~prefix, str))
    })

  // filesystem:move-file($srcUri as xs:string, $dstUri as xs:string) as item()*
  def moveFile[F[_]: PrologW: Bind, T: SearchOptions]: F[FunctionDecl2] =
    fileExistsInOtherFormat[F, T].fn flatMap { otherFmtExists =>
      fs.declare[F]("move-file") map (_(
        $("srcUri") as ST("xs:string"),
        $("dstUri") as ST("xs:string")
      ).as(ST.Top) { (srcUri: XQuery, dstUri: XQuery) =>
        if_(otherFmtExists(dstUri))
        .then_(formatConflictError(dstUri))
        .else_(
          let_(
            $("_1") := xdmp.documentInsert(dstUri, documentNode[T](srcUri)),
            $("_2") := xdmp.documentDelete(srcUri))
          .return_(emptySeq))
      })
    }

  // filesystem:descendant-uri-from-lexicon($parent as xs:string, $filter as cts:query) as xs:string?
  def descendantUriFromDocQuery[F[_]: PrologW: Functor]: F[FunctionDecl2] =
    fs.declare[F]("descendant-uri-from-doc-query") map (_(
      $("parent") as ST("xs:string"),
      $("filter") as ST("cts:query")
    ).as(ST("xs:string?")) { (parent: XQuery, filter: XQuery) =>
      val s = $("s")
      fn.filter(
        func(s.render) {
          fn.contains(fn.substringAfter(~s, parent), "/".xs)
        },
        fn.baseUri(cts.search(fn.doc(), cts.andQuery(mkSeq_(
          cts.directoryQuery(parent, "infinity".xs),
          cts.notQuery(cts.directoryQuery(parent, "1".xs)),
          filter))
        )(1.xqy)))
    })

  // filesystem:descendant-uri-from-lexicon($parent as xs:string, $filter as cts:query) as xs:string?
  def descendantUriFromLexicon[F[_]: PrologW: Functor]: F[FunctionDecl2] =
    fs.declare[F]("descendant-uri-from-lexicon") map (_(
      $("parent") as ST("xs:string"),
      $("filter") as ST("cts:query")
    ).as(ST("xs:string?")) { (parent: XQuery, filter: XQuery) =>
      cts.uriMatch(fn.concat(parent, "*/*".xs), IList("document".xs), some(filter))(1.xqy)
    })

  ////

  // filesystem:as-child-dir($parent as xs:string, $child as xs:string) as xs:string
  private def asChildDir[F[_]: PrologW: Functor]: F[FunctionDecl2] =
    fs.declare[F]("as-child-dir") map (_(
      $("parent") as ST("xs:string"),
      $("child")  as ST("xs:string")
    ).as(ST("xs:string")) { (parent: XQuery, child: XQuery) =>
      fn.concat(parent, fn.tokenize(fn.substringAfter(child, parent), "/".xs)(1.xqy), "/".xs)
    })

  /** filesystem:discover-children($parent as xs:string, $known as xs:string*)
    *
    * @param nextUri function that returns the next descendant URI given a parent directory
    *                and filtering cts:query.
    *
    * NB: Apparently, as of ML8, TCO doesn't kick in unless the function is untyped.
    * https://stackoverflow.com/questions/41746814/marklogic-xquery-tail-call-optimization
    */
  private def discoverChildren[F[_]: PrologW: Bind](nextUri: (XQuery, XQuery) => XQuery): F[FunctionDecl2] =
    (fs.name("discover-children").qn[F] |@| asChildDir[F].fn)((fname, childDir) =>
      declare(fname)(
        $("parent") as ST("xs:string"),
        $("known")  as ST("xs:string*")
      ) { (parent: XQuery, known: XQuery) =>
        val (knownq, uri, d) = ($("knownq"), $("uri"), $("d"))
        let_(
          knownq := cts.notQuery(cts.orQuery(fn.map(func(d.render) { cts.directoryQuery(~d, "infinity".xs) }, known))),
          uri    := nextUri(parent, ~knownq)
        ) return_ (
          if_(fn.exists(~uri))
          .then_(fname(parent, mkSeq_(childDir(parent, ~uri), known)))
          .else_(known)
        )
      })

  // 0xffffffffffff
  private val baseSeed: Long = 281474976710655L
}
