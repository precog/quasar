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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.fp.ski.κ
import quasar.physical.marklogic.DocType
import quasar.physical.marklogic.xml.namespaces._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr._
import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.auto._
import scalaz._, Scalaz._
import xml.name.QName

private[qscript] final class XmlStructuralPlanner[F[_]: Monad: MonadPlanErr: PrologW: QNameGenerator]
  extends StructuralPlanner[F, DocType.Xml] {

  import axes.child, StructuralPlanner.ejs
  import FunctionDecl._

  val ejsonN    = ejs name ejsonEjson.localPart
  val arrayEltN = ejs name ejsonArrayElt.localPart
  val typeAttrN = ejs name ejsonType.localPart

  // <ejson:ejson ejson:type="null" />
  val null_ : F[XQuery] =
    (ejsonN.xs[F] |@| typeAttrN.xs[F]) { (ejsxs, tpexs) =>
      element { ejsxs } { attribute { tpexs } { "null".xs } }
    }

  def arrayAppend(array: XQuery, value: XQuery) =
    mkArrayElt(value) >>= (mem.nodeInsertChild[F](array, _))

  def arrayConcat(a1: XQuery, a2: XQuery) =
    mkArray(mkSeq_(a1 `/` child.element(), a2 `/` child.element()))

  def arrayElementAt(array: XQuery, index: XQuery) =
    (array `/` child.element()(index + 1.xqy)).point[F]

  def asSortKey(item: XQuery): F[XQuery] =
    castIfNode(item)

  def isArray(item: XQuery) =
    isContainer(item)

  def leftShift(node: XQuery) =
    leftShiftFn(node)

  def mkArray(elements: XQuery) =
    mkArrayFn(elements)

  def mkArrayElt(item: XQuery) =
    arrayEltN.qn[F] >>= (qn => renameOrWrap(qn.xqy, item))

  def mkObject(entries: XQuery) =
    mkObjectFn(entries)

  def mkObjectEntry(key: XQuery, value: XQuery) =
    key match {
      case XQuery.StringLit(QName.string(qn)) =>
        renameOrWrap(qn.xqy, value)
      case XQuery.StringLit(nonQNameKey) =>
        renameOrWrapEncoded(xs.string(nonQNameKey.xs), value)
      case _ =>
        guardQNameF(key, renameOrWrap(xs.QName(key), value), renameOrWrapEncoded(xs.string(key), value))
    }

  def nodeCast(node: XQuery) =
    castAsAscribed(node)

  def nodeMetadata(node: XQuery) =
    attributes(node)

  def nodeToString(node: XQuery): F[XQuery] =
    fn.string(node).point[F]

  def nodeType(node: XQuery) =
    xmlNodeType(node)

  def objectDelete(obj: XQuery, key: XQuery) =
    key match {
      case XQuery.StringLit(QName.string(qn)) => withoutNamed(obj, qn.xqy)
      case XQuery.StringLit(name) => withoutNamedEncoded(obj, name.xs)
      case _ => guardQNameF(key, withoutNamed(obj, xs.QName(key)), withoutNamedEncoded(obj, fn.string(key)))
    }

  // TODO: This assumes the `key` is not present in the object, for performance.
  //       may need to switch to `objectUpdate` if we need to check that here.
  def objectInsert(obj: XQuery, key: XQuery, value: XQuery) = {
    val elt = key match {
      case XQuery.StringLit(QName.string(qn)) => renameOrWrap(qn.xqy, value)
      case XQuery.StringLit(name) => renameOrWrapEncoded(xs.string(name.xs), value)
      case _ => guardQNameF(key, renameOrWrap(xs.QName(key), value), renameOrWrapEncoded(fn.string(key), value))
    }

    elt >>= (mem.nodeInsertChild[F](obj, _))
  }

  def objectLookup(obj: XQuery, key: XQuery) = {
    val prj = key match {
      case XQuery.Step(_) =>
        (obj `/` key).point[F]

      case XQuery.StringLit(QName.string(qn)) =>
        freshName[F] map ( m =>
          if (XQuery.flwor.nonEmpty(obj))
            let_(m := obj) return_ (~m `/` child(qn))
          else
            obj `/` child(qn))

      case XQuery.StringLit(s) =>
        freshName[F] map ( m =>
          if (XQuery.flwor.nonEmpty(obj))
            let_(m := obj) return_ childrenNamedEncoded(~m, s.xs)
          else
            childrenNamedEncoded(obj, s.xs))

      case _ => guardQNameF(key, childrenNamed(obj, xs.QName(key)), childrenNamedEncoded(obj, fn.string(key)).point[F])
    }

    prj >>= (manyToArray(_))
  }

  def objectMerge(o1: XQuery, o2: XQuery) =
    elementMerge(o1, o2)

  private def guardQNameF(candidate: XQuery, left: F[XQuery], right: F[XQuery]): F[XQuery] =
    (left |@| right)((l, r) => guardQName(candidate, expr.func()(l), expr.func()(r))).join

  private def childrenNamedEncoded(src: XQuery, field: XQuery): XQuery = {
    val pred = axes.attribute.attributeNamed(ejsonEncodedAttr.shows) === field
    src `/` child(ejsonEncodedName)(pred)
  }

  private def nonEncodedAttrs(elt: XQuery): XQuery =
    elt `/` axes.attribute.node()(fn.name() =/= ejsonEncodedAttr.shows.xs)

  ////

  // ejson:is-container($item as item()?) as xs:boolean
  lazy val isContainer: F[FunctionDecl1] =
    ejs.declare[F]("is-container") map (_(
      $("item") as ST("item()?")
    ).as(ST("xs:boolean")) { item: XQuery =>
      typeswitch(item)(
        $("elt") as ST("element()") return_ (elt =>
          fn.empty(elt `/` child.node()) or
          fn.not(fn.empty(elt `/` child.element())))
      ) default fn.False
    })

  // ejson:left-shift($node as node()?) as item()*
  lazy val leftShiftFn: F[FunctionDecl1] =
    ejs.declare[F]("left-shift") map (_(
      $("node") as ST("node()?")
    ).as(ST("item()*")) { node =>
      typeswitch(node)(
        ST("element()") return_ (node `/` child.element())
      ) default emptySeq
    })

  // ejson:make-array($elements as element()*) as element()
  lazy val mkArrayFn: F[FunctionDecl1] =
    (ejsonN.xs[F] |@| typeAttrN.xs[F])((ejsxs, tpexs) =>
      ejs.declare[F]("make-array") map (_(
        $("elements") as ST(s"element()*")
      ).as(ST(s"element()")) { elts: XQuery =>
        element { ejsxs } { mkSeq_(attribute { tpexs } { "array".xs }, elts) }
      })).join

  lazy val mkObjectFn: F[FunctionDecl1] =
    (ejsonN.xs[F] |@| typeAttrN.xs[F])((ejsxs, tpexs) =>
      ejs.declare[F]("make-object") map (_(
        $("entries") as ST(s"element()*")
      ).as(ST(s"element()")) { entries: XQuery =>
        element { ejsxs } { mkSeq_(attribute { tpexs } { "object".xs }, entries) }
      })).join

  lazy val elementMerge: F[FunctionDecl2] =
    ejs.declare[F]("element-merge") flatMap (_(
      $("obj1") as ST("element()?"),
      $("obj2") as ST("element()?")
    ).as(ST(s"element()")) { (obj1: XQuery, obj2: XQuery) =>
      val (xs, ys, names, e, n1, n2) = ($("xs"), $("ys"), $("names"), $("e"), $("n1"), $("n2"))
      val (elt, encodedNames) = ($("elt"), $("encodedNames"))

      mkObjectFn {
        let_(
          xs    := (obj2 `/` child.element()),
          names := fn.map("fn:node-name#1".xqy, ~xs),
          encodedNames := ~xs `/` axes.attribute.attributeNamed(ejsonEncodedAttr.shows),
          ys    := fn.filter(func(e.render) {
            typeswitch(~e)(
              elt as ST(s"element($ejsonEncodedName)") return_ (_ =>
                every(
                  n1 in ~elt `/` axes.attribute.attributeNamed(ejsonEncodedAttr.shows),
                  n2 in ~encodedNames) satisfies (~n1 ne ~n2))
            ) default (every(n1 in fn.nodeName(~e), n2 in ~names) satisfies (~n1 ne ~n2))
          }, obj1 `/` child.element()))
        .return_(mkSeq_(~ys, ~xs))
      }
    })

  // ejson:many-to-array($items as item()*) as item()*
  lazy val manyToArray: F[FunctionDecl1] =
    ejs.declare[F]("many-to-array") flatMap (_(
      $("items") as ST.Top
    ).as(ST.Top) { items: XQuery =>
      seqToArray(items) map { arr =>
        if_(fn.count(items) gt 1.xqy) then_ arr else_ items
      }
    })

  // ejson:children-named($src as element()?, $name as xs:QName?) as item()*
  lazy val childrenNamed: F[FunctionDecl2] =
    ejs.declare[F]("children-named") map (_(
      $("src")  as ST("element()?"),
      $("name") as ST("xs:QName?")
    ).as(ST.Top) { (src: XQuery, field: XQuery) =>
      val n = $("n")
      fn.filter(func(n.render)(fn.nodeName(~n) eq xs.QName(field)), src `/` child.element())
    })

  // ejson:without-named-encoded($src as element()?, $name as xs:string) as element()?
  lazy val withoutNamedEncoded: F[FunctionDecl2] =
    ejs.declare[F]("without-named-encoded") map (_(
      $("src")  as ST("element()?"),
      $("name") as ST("xs:string")
    ).as(ST("element()?")) { (src: XQuery, name: XQuery) =>
      val (s, n, e) = ($("s"), $("n"), $("e"))
      fn.map(func(s.render) {
        element { fn.nodeName(~s) } {
          mkSeq_(
            ~s `/` axes.attribute.node(),
            for_ (n in (~s `/` child.element()))
            .return_(
              typeswitch(~n)(
                e as ST(s"element($ejsonEncodedName)") return_ ((_: XQuery) =>
                  if_(~n `/` axes.attribute.attributeNamed(ejsonEncodedAttr.shows) eq name)
                  .then_(emptySeq)
                  .else_(~n))
              ) default ~n))
        }
      }, src)
    })

  // qscript:without-named($src as element()?, $name as xs:QName) as element()?
  lazy val withoutNamed: F[FunctionDecl2] =
    ejs.declare[F]("without-named") map (_(
      $("src")  as ST("element()?"),
      $("name") as ST("xs:QName")
    ).as(ST("element()?")) { (src: XQuery, field: XQuery) =>
      val (s, n) = ($("s"), $("n"))
      fn.map(func(s.render) {
        element { fn.nodeName(~s) } {
          mkSeq_(
            ~s `/` axes.attribute.node(),
            for_    (n in (~s `/` child.element()))
            .where_ (fn.nodeName(~n) ne field)
            .return_(~n))
        }
      }, src)
    })

  // ejson:type-attr-for($item as item()*) as attribute()?
  lazy val typeAttrFor: F[FunctionDecl1] =
    ejs.declare[F]("type-attr-for") flatMap (_(
      $("item") as ST.Top
    ).as(ST("attribute()?")) { (item: XQuery) =>
      (typeAttrN.xs[F] |@| typeOf(item)) { (tname, tpe) =>
        val x = $("x")
        fn.map(func(x.render) { attribute { tname } { ~x } }, tpe)
      }
    })

  // ejson:rename-or-wrap($name as xs:QName, $value as item()*) as element()
  lazy val renameOrWrap: F[FunctionDecl2] =
    typeAttrFor.fn flatMap { typeAttr =>
      ejs.declare[F]("rename-or-wrap") map (_(
        $("name")  as ST("xs:QName"),
        $("value") as ST.Top
      ).as(ST(s"element()")) { (name: XQuery, value: XQuery) =>
        typeswitch(value)(
          $("e") as ST("element()") return_ (e =>
            element { name }
              { mkSeq_(nonEncodedAttrs(e), e `/` child.node())})
        ) default (element { name } { mkSeq_(typeAttr(value), value) })
      })
    }

  // ejson:rename-or-wrap-encoded($name as xs:string, $value as item()*) as element()
  lazy val renameOrWrapEncoded: F[FunctionDecl2] =
    typeAttrFor.fn flatMap { typeAttr =>
      ejs.declare[F]("rename-or-wrap-encoded") map (_(
        $("name")  as ST("xs:string"),
        $("value") as ST.Top
      ).as(ST(s"element()")) { (name: XQuery, value: XQuery) =>
        typeswitch(value)(
          $("e") as ST("element()") return_ (e =>
            element { ejsonEncodedName.xqy } {
              mkSeq_(attribute { ejsonEncodedAttr.xqy } { name }, nonEncodedAttrs(e), e `/` child.node())
            })
        ) default (element { ejsonEncodedName.xqy }
          { mkSeq_(attribute { ejsonEncodedAttr.xqy } { name },
              typeAttr(value),
              value)})
      })
    }


  lazy val ascribedType: F[FunctionDecl1] =
    typeAttrN.qn[F] flatMap { tname =>
      ejs.declare[F]("ascribed-type") map (_(
        $("node") as ST("node()")
      ).as(ST("xs:string?")) { (node: XQuery) =>
        typeswitch(node)(
          $("elt") as ST("element()") return_ (_ `/` axes.attribute(tname))
        ) default emptySeq
      })
    }

  lazy val castAsAscribed: F[FunctionDecl1] =
    ejs.declare[F]("cast-as-ascribed") flatMap (_(
      $("node") as ST("node()?")
    ).as(ST("item()?")) { (node: XQuery) =>
      val (elt, tpe) = ($("elt"), $("tpe"))

      nodeType(~elt) map { atpe =>
        typeswitch(node)(
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
              .then_(xs.duration(e))
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
        ) default node
      }
    })

  // qscript:attributes($node as node()) as element()?
  lazy val attributes: F[FunctionDecl1] =
    ejs.declare[F]("attributes") flatMap (_(
      $("node") as ST("node()")
    ).as(ST("element()?")) { (node: XQuery) =>
      val (elt, a) = ($("elt"), $("a"))

      renameOrWrap(fn.nodeName(~a), fn.data(~a))
        .map(entry => fn.map(func(a.render)(entry), ~elt `/` axes.attribute.*))
        .flatMap(mkObjectFn(_))
        .map(attrs =>
          typeswitch(node)(
            elt as ST("element()") return_ κ(attrs)
          ) default emptySeq)
    })

  val xmlNodeType: F[FunctionDecl1] =
    ejs.declare[F]("xml-node-type") flatMap (_(
      $("node") as ST("node()")
    ).as(ST("xs:string?")) { node: XQuery =>
      val (ascribed, elt) = ($("ascribed"), $("elt"))
      (ascribedType(node) |@| isContainer(~elt))((typ, isObj) =>
        let_(ascribed := typ) return_ {
          if_(fn.empty(~ascribed))
          .then_(typeswitch(node)(
            elt as ST("element()") return_ { _ =>
              if_(isObj) then_ "object".xs else_ emptySeq
            }) default emptySeq)
          .else_(~ascribed)
        })
    })

  // ejson:guard-qname($candidate as item()*, $if-qname as item()*, $if-non-qname as item()*) as item()*
  lazy val guardQName: F[FunctionDecl3] =
    ejs.declare[F]("guard-qname") flatMap (_(
      $("candidate") as ST("item()*"),
      $("if-qname") as ST("(function() as item()*)"),
      $("if-non-qname") as ST("(function() as item()*)")
    ).as(ST.Top) { (candidate: XQuery, ifQName: XQuery, ifNonQName: XQuery) =>
      isQName(candidate) map (if_(_)
      .then_(ifQName.fnapply())
      .else_(ifNonQName.fnapply()))
    })

  // ejson:is-qname($candidate as item()?) as xs:boolean
  lazy val isQName: F[FunctionDecl1] =
    ejs.declare[F]("is-qname") map (_(
      $("candidate") as ST("item()?")
    ).as(ST("xs:boolean")) { candidate: XQuery =>
      xdmp.castableAs(
        "http://www.w3.org/2001/XMLSchema".xs,
        "QName".xs,
        candidate)
    })
}
