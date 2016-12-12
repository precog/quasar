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
  import syntax._, expr._, axes.child
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

  // ejson:array-concat($arr1 as element()?, $arr2 as element()?) as element()
  def arrayConcat[F[_]: PrologW]: F[FunctionDecl2] =
    ejs.declare("array-concat") flatMap (_(
      $("arr1") as ST("element()?"),
      $("arr2") as ST("element()?")
    ).as(ST(s"element()")) { (arr1: XQuery, arr2: XQuery) =>
      mkArray_[F](mkSeq_(arr1 `/` child.element(), arr2 `/` child.element()))
    })

  // ejson:array-element-at($arr as element()?, $idx as xs:integer) as element()?
  def arrayElementAt[F[_]: PrologW]: F[FunctionDecl2] =
    ejs.declare("array-element-at") map (_(
      $("arr") as ST("element()?"),
      $("idx") as ST("xs:integer")
    ).as(ST(s"element()?")) { (arr: XQuery, idx: XQuery) =>
      arr `/` child.element()(idx)
    })

  // ejson:ascribed-type($elt as element()) as xs:string?
  def ascribedType[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("ascribed-type").qn[F] |@| typeAttrN.qn) { (fname, tname) =>
      declare(fname)(
        $("elt") as ST("element()")
      ).as(ST("xs:string?")) { (elt: XQuery) =>
        elt `/` axes.attribute(tname)
      }
    }

  // ejson:attributes($elt as element()) as element()
  def attributes[F[_]: PrologW]: F[FunctionDecl1] =
    ejs.declare("attributes") flatMap (_(
      $("elt") as ST("element()")
    ).as(ST("element()")) { (elt: XQuery) =>
      val a       = $("a")
      val entries = renameOrWrap[F] apply (fn.nodeName(~a), fn.data(~a)) map { entry =>
                      fn.map(func(a.render)(entry), elt `/` axes.attribute.*)
                    }

      entries flatMap (mkObject[F] apply _)
    })

  // ejson:cast-as-ascribed($item as item()?) as item()?
  def castAsAscribed[F[_]: PrologW]: F[FunctionDecl1] =
    ejs.declare("cast-as-ascribed") flatMap (_(
      $("item") as ST("item()?")
    ).as(ST("item()?")) { (item: XQuery) =>
      val (elt, tpe) = ($("elt"), $("tpe"))

      ascribedType[F].apply(~elt) map { atpe =>
        typeswitch(item)(
          elt as ST("element()") return_ { e =>
            let_(tpe := atpe) return_ {
              if_(~tpe eq "boolean".xs)
              .then_(xs.boolean(e))
              .else_(if_(~tpe eq "timestamp".xs)
              .then_(xs.dateTime(e))
              .else_(if_(~tpe eq "date".xs)
              .then_(xs.date(e))
              .else_(if_(~tpe eq "time".xs)
              .then_(xs.time(e))
              .else_(if_(~tpe eq "interval".xs)
              .then_(xs.duration(fn.concat("PT".xs, e, "S".xs)))
              .else_(if_(~tpe eq "integer".xs)
              .then_(xs.integer(e))
              .else_(if_(~tpe eq "decimal".xs)
              .then_(xs.double(e))
              .else_(if_(~tpe eq "string".xs)
              .then_(fn.string(e))
              .else_(if_(~tpe eq "binary".xs)
              .then_ {
                if_(isCastable(e, ST("xs:hexBinary")))
                .then_(xs.base64Binary(xs.hexBinary(e)))
                .else_(xs.base64Binary(e))
              }
              .else_(e)))))))))
            }
          }
        ) default item
      }
    })

  // ejson:is-array($item as item()?) as xs:boolean
  def isArray[F[_]: PrologW]: F[FunctionDecl1] =
    ejs.declare("is-array") map (_(
      $("item") as ST("item()?")
    ).as(ST("xs:boolean")) { item: XQuery =>
      typeswitch(item)(
        $("elt") as ST("element()") return_ (elt =>
          fn.empty(elt `/` child.node()) or
          fn.not(fn.empty(elt `/` child.element())))
      ) default fn.False
    })

  // ejson:many-to-array($items as item()*) as item()*
  def manyToArray[F[_]: PrologW]: F[FunctionDecl1] =
    ejs.declare("many-to-array") flatMap (_(
      $("items") as ST.Top
    ).as(ST.Top) { items: XQuery =>
      seqToArray_[F](items) map { arr =>
        if_(fn.count(items) gt 1.xqy) then_ arr else_ items
      }
    })

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

  // ejson:make-object($entries as element()*) as element()
  def mkObject[F[_]: PrologW]: F[FunctionDecl1] =
    (ejs.name("make-object").qn[F] |@| ejsonN.xs |@| typeAttrN.xs) {
      (fname, ejsxs, tpexs) =>

      declare(fname)(
        $("entries") as ST(s"element()*")
      ).as(ST(s"element()")) { entries =>
        element { ejsxs } { mkSeq_(attribute { tpexs } { "object".xs }, entries) }
      }
    }

  // ejson:object-concat($obj1 as element()?, $obj2 as element()?) as element()
  def objectConcat[F[_]: PrologW]: F[FunctionDecl2] =
    ejs.declare("object-concat") flatMap (_(
      $("obj1") as ST("element()?"),
      $("obj2") as ST("element()?")
    ).as(ST(s"element()")) { (obj1: XQuery, obj2: XQuery) =>
      val (xs, ys, names, e, n1, n2) = ($("xs"), $("ys"), $("names"), $("e"), $("n1"), $("n2"))

      mkObject[F] apply {
        let_(
          xs    := (obj2 `/` child.element()),
          names := fn.map("fn:node-name#1".xqy, ~xs),
          ys    := fn.filter(func(e.render) {
                     every(n1 in fn.nodeName(~e), n2 in ~names) satisfies (~n1 ne ~n2)
                   }, obj1 `/` child.element()))
        .return_(mkSeq_(~ys, ~xs))
      }
    })

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

  // ejson:singleton-array($value as item()*) as element()
  def singletonArray[F[_]: PrologW]: F[FunctionDecl1] =
    ejs.declare("singleton-array") flatMap (_(
      $("value") as ST.Top
    ).as(ST(s"element()")) { value: XQuery =>
      mkArrayElt[F](value) flatMap (mkArray_[F])
    })

  // ejson:singleton-object($key as xs:string, $value as item()*) as element()
  def singletonObject[F[_]: PrologW]: F[FunctionDecl2] =
    ejs.declare("singleton-object") flatMap (_(
      $("key") as ST("xs:QName"),
      $("value") as ST.Top
    ).as(ST(s"element()")) { (key: XQuery, value: XQuery) =>
      renameOrWrap[F].apply(key, value) flatMap (xqy => mkObject[F].apply(xqy))
    })

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
            ST("xs:date")         return_ "date".xs,
            ST("xs:time")         return_ "time".xs,
            ST("xs:integer")      return_ "integer".xs,
            ST("xs:decimal")      return_ "decimal".xs,
            ST("xs:double")       return_ "decimal".xs,
            ST("xs:float")        return_ "decimal".xs,
            ST("xs:base64Binary") return_ "binary".xs,
            ST("xs:hexBinary")    return_ "binary".xs,
            ST("xs:QName")        return_ "string".xs,
            ST("xs:string")       return_ "string".xs
          ) default emptySeq
        }
      }
    })

  // ejson:unshift-object(
  //   $keyf as function(item()) as xs:string,
  //   $valf as function(item()) as item()*,
  //   $seq  as item()*
  // ) as element()
  def unshiftObject[F[_]: PrologW]: F[FunctionDecl3] =
    ejs.declare("unshift-object") flatMap (_(
      $("keyf") as ST("function(item()) as xs:QName"),
      $("valf") as ST("function(item()) as item()*"),
      $("seq")  as ST("item()*")
    ).as(ST(s"element()")) { (keyf: XQuery, valf: XQuery, seq: XQuery) =>
      val x = "$x"
      for {
        entry <- renameOrWrap[F].apply(keyf.fnapply(x.xqy), valf.fnapply(x.xqy))
        obj   <- mkObject[F].apply(fn.map(func(x)(entry), seq))
      } yield obj
    })
}
