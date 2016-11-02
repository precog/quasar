/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.xquery

import quasar.Predef._
import quasar.physical.marklogic.xml.namespaces._

import eu.timepit.refined.auto._
import scalaz.syntax.monad._

object ejson {
  import syntax._, expr.{attribute, element, emptySeq, every, for_, func, if_, let_, typeswitch}, axes.child
  import FunctionDecl.{FunctionDecl1, FunctionDecl2, FunctionDecl3}

  val ejs = NamespaceDecl(ejsonNs)

  val ejsonN    = ejs name ejsonEjson.local
  val arrayEltN = ejs name ejsonArrayElt.local
  val typeAttrN = ejs name ejsonType.local

  // <ejson:ejson ejson:type="null" />
  def null_[F[_]: PrologW]: F[XQuery] =
    (ejsonN.xs[F] |@|  typeAttrN.xs) { (ejsxs, tpexs) =>
      element { ejsxs } { attribute { tpexs } { "null".xs } }
    }

  // ejson:array-append($arr as element(), $item as item()*) as element()
  def arrayAppend[F[_]: PrologW]: F[FunctionDecl2] =
    ejs.declare("array-append") flatMap (_(
      $("arr")  as ST("element()"),
      $("item") as ST.Top
    ).as(ST("element()")) { (arr: XQuery, item: XQuery) =>
      mkArrayElt[F](item) flatMap (mem.nodeInsertChild(arr, _))
    })

  // ejson:array-concat($arr1 as element(), $arr2 as element()) as element(ejson:ejson)
  def arrayConcat[F[_]: PrologW]: F[FunctionDecl2] =
    (ejs.name("array-concat").qn[F] |@| ejsonN.qn |@| arrayEltN.qn) { (fname, ename, aelt) =>
      declare(fname)(
        $("arr1") as ST("element()"),
        $("arr2") as ST("element()")
      ).as(ST(s"element($ename)")) { (arr1: XQuery, arr2: XQuery) =>
        mkArray[F] apply (ename.xqy, mkSeq_(arr1 `/` child(aelt), arr2 `/` child(aelt)))
      }
    }.join

  // ejson:array-element-at($arr as element(), $idx as xs:integer) as element(ejson:array-element)?
  def arrayElementAt[F[_]: PrologW]: F[FunctionDecl2] =
    (ejs.name("array-element-at").qn[F] |@| arrayEltN.qn) { (fname, aelt) =>
      declare(fname)(
        $("arr") as ST("element()"),
        $("idx") as ST("xs:integer")
      ).as(ST(s"element($aelt)?")) { (arr: XQuery, idx: XQuery) =>
        arr `/` child(aelt)(idx)
      }
    }

  // ejson:array-dup-indices($arr as element()) as element()
  def arrayDupIndices[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("array-dup-indices").qn[F] |@| arrayEltN.qn) { (fname, aelt) =>
      declare(fname)(
        $("arr") as ST("element()")
      ).as(ST("element()")) { arr: XQuery =>
        val elts = "$elts"
        seqToArray[F].apply(fn.nodeName(arr), 0.xqy to mkSeq_(fn.count(elts.xqy) - 1.xqy)) map { inner =>
          let_(elts -> (arr `/` child(aelt))) return_ {
            if_ (fn.empty(elts.xqy)) then_ arr else_ inner
          }
        }
      }
    }.join

  // ejson:array-zip-indices($arr as element()) as element()
  def arrayZipIndices[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("array-zip-indices").qn[F] |@| arrayEltN.qn) { (fname, aelt) =>
      declare(fname)(
        $("arr") as ST("element()")
      ).as(ST("element()")) { arr: XQuery =>
        val (i, elts, zelts) = ("$i", "$elts", "$zelts")

        for {
          ixelt <- mkArrayElt[F](i.xqy)
          pair  <- mkArray_[F](mkSeq_(ixelt, elts.xqy(i.xqy)))
          zpair <- mkArrayElt[F](pair)
          zarr  <- mkArray[F] apply (fn.nodeName(arr), zelts.xqy)
        } yield {
          let_(elts -> (arr `/` child(aelt))) return_ {
            if_ (fn.empty(elts.xqy))
            .then_ { arr }
            .else_ {
              let_(zelts -> for_(i -> (1.xqy to fn.count(elts.xqy))).return_(zpair))
              .return_(zarr)
            }
          }
        }
      }
    }.join

  // ejson:ascribed-type($elt as element()) as xs:string?
  def ascribedType[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("ascribed-type").qn[F] |@| typeAttrN.qn) { (fname, tname) =>
      declare(fname)(
        $("elt") as ST("element()")
      ).as(ST("xs:string?")) { (elt: XQuery) =>
        elt `/` axes.attribute(tname)
      }
    }

  // TODO: DRY up these predicates, they have the same impl.
  // ejson:is-array($node as node()) as xs:boolean
  def isArray[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("is-array").qn[F] |@| typeAttrN.qn) { (fname, tname) =>
      declare(fname)(
        $("node") as ST("node()")
      ).as(ST("xs:boolean")) { node =>
        fn.not(fn.empty(node(axes.attribute(tname) === "array".xs)))
      }
    }

  // ejson:is-null($node as node()) as xs:boolean
  def isNull[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("is-null").qn[F] |@| typeAttrN.qn) { (fname, tname) =>
      declare(fname)(
        $("node") as ST("node()")
      ).as(ST("xs:boolean")) { node =>
        fn.not(fn.empty(node(axes.attribute(tname) === "null".xs)))
      }
    }

  // ejson:is-object($node as node()) as xs:boolean
  def isObject[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("is-object").qn[F] |@| typeAttrN.qn) { (fname, tname) =>
      declare(fname)(
        $("node") as ST("node()")
      ).as(ST("xs:boolean")) { node =>
        fn.not(fn.empty(node(axes.attribute(tname) === "object".xs)))
      }
    }

  // ejson:make-array($name as xs:QName, $elements as element(ejson:array-element)*) as element()
  def mkArray[F[_]: PrologW]: F[FunctionDecl2] =
    (ejs.name("make-array").qn[F] |@| arrayEltN.qn |@| typeAttrN.xs) { (fname, aelt, tpexs) =>
      declare(fname)(
        $("name"    ) as ST(s"xs:QName"),
        $("elements") as ST(s"element($aelt)*")
      ).as(ST(s"element()")) { (name, elts) =>
        element { name } { mkSeq_(attribute { tpexs } { "array".xs }, elts) }
      }
    }

  def mkArray_[F[_]: PrologW](elements: XQuery): F[XQuery] =
    ejsonN.qn[F] flatMap (ename => mkArray[F].apply(ename.xqy, elements))

  def mkArrayElt[F[_]: PrologW](value: XQuery): F[XQuery] =
    arrayEltN.qn[F] flatMap (name => renameOrWrap[F].apply(name.xqy, value))

  // ejson:make-object($entries as element()*) as element(ejson:ejson)
  def mkObject[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("make-object").qn[F] |@| ejsonN.qn |@| ejsonN.xs |@| typeAttrN.xs) {
      (fname, ename, ejsxs, tpexs) =>

      declare(fname)(
        $("entries") as ST(s"element()*")
      ).as(ST(s"element($ename)")) { entries =>
        element { ejsxs } { mkSeq_(attribute { tpexs } { "object".xs }, entries) }
      }
    }

  // ejson:object-concat($obj1 as element(), $obj2 as element()) as element()
  def objectConcat[F[_]: PrologW]: F[FunctionDecl2] =
    (ejs.name("object-concat").qn[F] |@| ejsonN.qn) { (fname, ename) =>
      declare(fname)(
        $("obj1") as ST("element()"),
        $("obj2") as ST("element()")
      ).as(ST(s"element($ename)")) { (obj1: XQuery, obj2: XQuery) =>
        val (xs, ys, names, e, n1, n2) = ("$xs", "$ys", "$names", "$e", "$n1", "$n2")

        mkObject[F] apply {
          let_(
            xs    -> (obj2 `/` child.element()),
            names -> fn.map("fn:node-name#1".xqy, xs.xqy),
            ys    -> fn.filter(func(e) {
                       every(n1 -> fn.nodeName(e.xqy), n2 -> names.xqy) satisfies (n1.xqy ne n2.xqy)
                     }, obj1 `/` child.element()))
          .return_(mkSeq_(ys.xqy, xs.xqy))
        }
      }
    }.join

  // ejson:object-insert($obj as element(), $key as xs:string, $value as item()*) as element()
  //
  // TODO: This assumes the `key` is not present in the object, for performance.
  //       may need to switch to `objectUpdate` if we need to check that here.
  //
  def objectInsert[F[_]: PrologW]: F[FunctionDecl3] =
    ejs.declare("object-insert") flatMap (_(
      $("obj")   as ST("element()"),
      $("key")   as ST("xs:QName"),
      $("value") as ST.Top
    ).as(ST("element()")) { (obj: XQuery, key: XQuery, value: XQuery) =>
      renameOrWrap[F].apply(key, value) flatMap (mem.nodeInsertChild(obj, _))
    })

  // ejson:rename-or-wrap($name as xs:QName, $value as item()*) as element()
  def renameOrWrap[F[_]: PrologW]: F[FunctionDecl2] =
    ejs.declare("rename-or-wrap") flatMap (_(
      $("name")  as ST("xs:QName"),
      $("value") as ST.Top
    ).as(ST(s"element()")) { (name: XQuery, value: XQuery) =>
      typeAttrFor[F].apply(value) map { typeAttr =>
        typeswitch(value)(
          $("e") as ST("element()") return_ (e =>
            element { name } { mkSeq_(e `/` axes.attribute.node(), e `/` child.node()) })
        ) default (element { name } { mkSeq_(typeAttr, value) })
      }
    })

  // ejson:seq-to-array($name as xs:QName, $items as item()*) as element()
  def seqToArray[F[_]: PrologW]: F[FunctionDecl2] =
    ejs.declare("seq-to-array") flatMap (_(
      $("name")  as ST("xs:QName"),
      $("items") as ST("item()*")
    ).as(ST(s"element()")) { (name: XQuery, items: XQuery) =>
      val x = "$x"
      for {
        arrElt <- mkArrayElt[F](x.xqy)
        arr    <- mkArray[F] apply (name, fn.map(func(x) { arrElt }, items))
      } yield arr
    })

  def seqToArray_[F[_]: PrologW](items: XQuery): F[XQuery] =
    ejsonN.qn[F] flatMap (ename => seqToArray[F].apply(ename.xqy, items))

  // ejson:singleton-array($value as item()*) as element(ejson:ejson)
  def singletonArray[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("singleton-array").qn[F] |@| ejsonN.qn) { (fname, ename) =>
      declare(fname)(
        $("value") as ST.Top
      ).as(ST(s"element($ename)")) { value: XQuery =>
        mkArrayElt[F](value) flatMap (mkArray[F].apply(ename.xqy, _))
      }
    }.join

  // ejson:singleton-object($key as xs:string, $value as item()*) as element(ejson:ejson)
  def singletonObject[F[_]: PrologW]: F[FunctionDecl2] =
    (ejs.name("singleton-object").qn[F] |@| ejsonN.qn) { (fname, ename) =>
      declare(fname)(
        $("key") as ST("xs:QName"),
        $("value") as ST.Top
      ).as(ST(s"element($ename)")) { (key: XQuery, value: XQuery) =>
        renameOrWrap[F].apply(key, value) flatMap (xqy => mkObject[F].apply(xqy))
      }
    }.join

  // ejson:type-attr-for($item as item()*) as attribute()?
  def typeAttrFor[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("type-attr-for").qn[F] |@| typeAttrN.qn) { (fname, tname) =>
      declare(fname)(
        $("item") as ST.Top
      ).as(ST("attribute()?")) { (item: XQuery) =>
        typeOf[F].apply(item) map { tpe =>
          fn.map(func("$x") { attribute { tname.xs } { "$x".xqy } }, tpe)
        }
      }
    }.join

  // ejson:type-of($item as item()*) as xs:string?
  def typeOf[F[_]: PrologW]: F[FunctionDecl1] =
    ejs.declare("type-of") flatMap (_(
      $("item") as ST.Top
    ).as(ST("xs:string?")) { item: XQuery =>
      ascribedType[F].apply(item) map { aType =>
        if_(fn.empty(item))
        .then_ { "na".xs }
        .else_ {
          typeswitch(item)(
            ST("element()")       return_ aType,
            ST("xs:boolean")      return_ "boolean".xs,
            ST("xs:dateTime")     return_ "timestamp".xs,
            ST("xs:integer")      return_ "integer".xs,
            ST("xs:decimal")      return_ "decimal".xs,
            ST("xs:double")       return_ "decimal".xs,
            ST("xs:float")        return_ "decimal".xs,
            ST("xs:base64Binary") return_ "binary".xs,
            ST("xs:hexBinary")    return_ "binary".xs
          ) default emptySeq
        }
      }
    })

  // ejson:unshift-object(
  //   $keyf as function(item()) as xs:string,
  //   $valf as function(item()) as item()*,
  //   $seq  as item()*
  // ) as element(ejson:ejson)
  def unshiftObject[F[_]: PrologW]: F[FunctionDecl3] =
    (ejs.name("unshift-object").qn[F] |@| ejsonN.qn) { (fname, ename) =>
      declare(fname)(
        $("keyf") as ST("function(item()) as xs:QName"),
        $("valf") as ST("function(item()) as item()*"),
        $("seq")  as ST("item()*")
      ).as(ST(s"element($ename)")) { (keyf: XQuery, valf: XQuery, seq: XQuery) =>
        val x = "$x"
        for {
          entry <- renameOrWrap[F].apply(keyf.fnapply(x.xqy), valf.fnapply(x.xqy))
          obj   <- mkObject[F].apply(fn.map(func(x)(entry), seq))
        } yield obj
      }
    }.join
}
