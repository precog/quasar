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

import quasar.Predef._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xml.{NCName, NSPrefix}
import quasar.physical.marklogic.xml.namespaces._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.auto._
import scalaz._, Scalaz._

object lib {
  import expr.{emptySeq, for_, func, if_, let_}, axes.child
  import FunctionDecl._

  val prop           = NSPrefix(NCName("prop"))
  val propProperties = prop(NCName("properties"))
  val propDirectory  = prop(NCName("directory"))

  val fs = NamespaceDecl(filesystemNs)

  def formatConflictError(uri: XQuery) = {
    val msg = "A document with a different format exists at ".xs
    fn.error(filesystemError.xqy, some(fn.concat(msg, uri)))
  }

  /** Appends the given nodes as children of `dst` in a format-appropriate manner. */
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

  def descendantsHavingFormatExist[F[_]: PrologW: Functor, T: SearchOptions]: F[FunctionDecl1] =
    fs.declare[F]("descendants-having-format-exist") map (_(
      $("uri") as ST("xs:string")
    ).as(ST("xs:boolean")) { uri: XQuery =>
      fn.exists(directoryDocuments[T](uri, true))
    })

  def directoryContents[F[_]: PrologW: Bind, T: SearchOptions]: F[FunctionDecl1] =
    (
      subDirectories[F].fn                  |@|
      descendantsHavingFormatExist[F, T].fn
    ).tupled flatMap { case (subDirs, descsExist) =>
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
            subDirs(uri, fn.False))

        fn.map(
          func(kid.render) { fn.substringAfter(~kid, uri) },
          mkSeq_(childDirs, childFiles))
      })
    }

  def directoryIsEmpty[F[_]: PrologW: Functor]: F[FunctionDecl1] =
    fs.declare[F]("directory-is-empty") map (_(
      $("uri") as ST("xs:string")
    ).as(ST.Top) { uri: XQuery =>
      fn.empty(cts.uriMatch(fn.concat(uri, "*".xs), IList("document".xs)))
    })

  def emptyDescendantDirectories[F[_]: PrologW: Bind]: F[FunctionDecl1] =
    (
      subDirectories[F].fn    |@|
      directoryIsEmpty[F].ref
    ).tupled flatMap { case (subDirs, dirIsEmptyRef) =>
      fs.declare[F]("empty-descendant-directories") map (_(
        $("uri") as ST("xs:string")
      ).as(ST.Top) { uri: XQuery =>
        fn.filter(dirIsEmptyRef, subDirs(uri, fn.True))
      })
    }

  def fileInOtherFormatExists[F[_]: PrologW: Functor, T: SearchOptions]: F[FunctionDecl1] =
    fs.declare[F]("file-exists-in-other-format") map (_(
      $("uri") as ST("xs:string")
    ).as(ST("xs:boolean")) { uri: XQuery =>
      if_(fn.docAvailable(uri))
      .then_(fn.empty(documentNode[T](uri)))
      .else_(fn.False)
    })

  def fileParent[F[_]: PrologW: Functor]: F[FunctionDecl1] =
    fs.declare[F]("file-parent") map (_(
      $("uri") as ST("xs:string")
    ).as(ST("xs:string")) { uri: XQuery =>
      val toks = $("toks")
      let_(toks := fn.tokenize(uri, "/".xs)) return_ {
        fn.stringJoin(mkSeq_((~toks)(1.xqy to (fn.last - 1.xqy)), "".xs), "/".xs)
      }
    })

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

  def moveFile[F[_]: PrologW: Monad, T: SearchOptions]: F[FunctionDecl2] =
    fileInOtherFormatExists[F, T].fn flatMap { otherFmtExists =>
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

  def subDirectories[F[_]: PrologW: Functor]: F[FunctionDecl2] =
    fs.declare[F]("sub-directories") map (_(
      $("uri")                as ST("xs:string"),
      $("includeDescendants") as ST("xs:boolean")
    ).as(ST("xs:string*")) { (uri: XQuery, includeDescendants: XQuery) =>
      val depth = if_(includeDescendants) then_ "infinity".xs else_ "1".xs
      fn.map(fn.ns(NCName("base-uri")) :# 1, xdmp.directoryProperties(uri, depth) `/` child(propProperties) `/` child(propDirectory))
    })

  ////

  // 0xffffffffffff
  private val baseSeed: Long = 281474976710655L
}
