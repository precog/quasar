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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.fp.ski.κ
import quasar.physical.marklogic.DocType
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr._
import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.auto._
import scalaz._, Scalaz._

private[qscript] final class JsonStructuralPlanner[F[_]: Monad: MonadPlanErr: PrologW: QNameGenerator]
  extends StructuralPlanner[F, DocType.Json] {

  import StructuralPlanner.ejs, axes.child
  import FunctionDecl._

  private val empty = emptySeq.point[F]

  def null_ : F[XQuery] =
    nullNode.point[F]

  def arrayAppend(array: XQuery, value: XQuery): F[XQuery] =
    jsonEncode(value) >>= (jsonArrayAppend(array, _))

  def arrayConcat(a1: XQuery, a2: XQuery): F[XQuery] =
    jsonDeserialize(json.toArray(mkSeq_(
      a1 `/` child.node(),
      a2 `/` child.node()
    )))

  def arrayElementAt(array: XQuery, index: XQuery): F[XQuery] =
    (array `/` child.node()(index + 1.xqy)).point[F]

  def asSortKey(item: XQuery): F[XQuery] =
    jsonAsSortKey(item)

  def isArray(item: XQuery): F[XQuery] =
    jsonIsArray(item)

  def leftShift(node: XQuery): F[XQuery] =
    (node `/` child.node()).point[F]

  def mkArray(elements: XQuery): F[XQuery] =
    jsonDeserialize(json.toArray(elements))

  def mkArrayElt(item: XQuery): F[XQuery] =
    jsonEncode(item)

  def mkObject(entries: XQuery): F[XQuery] =
    jsonDeserialize(map.new_(entries))

  def mkObjectEntry(key: XQuery, value: XQuery): F[XQuery] =
    jsonEncode(value) map (map.entry(key, _))

  def nodeCast(node: XQuery): F[XQuery] =
    jsonNodeCast(node)

  def nodeMetadata(node: XQuery): F[XQuery] =
    emptySeq.point[F]

  def nodeToString(node: XQuery): F[XQuery] =
    jsonNodeToString(node)

  def nodeType(node: XQuery): F[XQuery] =
    jsonNodeType(node)

  def objectDelete(obj: XQuery, key: XQuery): F[XQuery] =
    jsonObjectDelete(obj, key)

  def objectInsert(obj: XQuery, key: XQuery, value: XQuery): F[XQuery] =
    jsonEncode(value) >>= (jsonObjectInsert(obj, key, _))

  def objectLookup(obj: XQuery, key: XQuery): F[XQuery] =
    key match {
      case XQuery.Step(_) =>
        (obj `/` key).point[F]

      case XQuery.StringLit(s) =>
        if (XQuery.flwor.isMatching(obj))
          freshName[F] map (m => let_(m := obj) return_ (~m `/` child.nodeNamed(s)))
        else
          (obj `/` child.nodeNamed(s)).point[F]

      case _ =>
        map.get(xdmp.fromJson(obj), key).point[F]
    }

  def objectMerge(o1: XQuery, o2: XQuery): F[XQuery] =
    jsonObjectMerge(o1, o2)

  ////

  val nullNode: XQuery =
    XQuery("null-node{}")

  def boolNode(bool: XQuery): XQuery =
    XQuery(s"boolean-node{$bool}")

  def numberNode(num: XQuery): XQuery =
    XQuery(s"number-node{$num}")

  // ejson:json-array-append($item as item()?) as xs:boolean
  val jsonArrayAppend: F[FunctionDecl2] =
    ejs.declare[F]("json-array-append") flatMap (_(
      $("arr")  as ST("array-node()"),
      $("item") as ST("item()?")
    ).as(ST("array-node()")) { (arr: XQuery, item: XQuery) =>
      val a = $("a")
      jsonDeserialize(~a) map { deserialized =>
        let_(
          a      := xdmp.fromJson(arr),
          $("_") := json.arrayPush(~a, item))
        .return_(deserialized)
      }
    })

  // ejson:json-as-sort-key($item as item()?) as item()*
  val jsonAsSortKey: F[FunctionDecl1] =
    ejs.declare[F]("json-as-sort-key") flatMap (_(
      $("item") as ST("item()?")
    ).as(ST.Top) { (item: XQuery) =>
      castIfNode(item) map { casted =>
        typeswitch(item)(
          ST("null-node()") return_ emptySeq
        ) default casted
      }
    })

  // ejson:json-deserialize($serialized as item()?) as node()?
  val jsonDeserialize: F[FunctionDecl1] =
    ejs.declare[F]("json-deserialize") map (_(
      $("serialized") as ST("item()?")
    ).as(ST("node()?")) { serialized: XQuery =>
      if_(fn.empty(serialized))
      .then_(emptySeq)
      .else_(xdmp.toJson(serialized) `/` child.node())
    })

  // ejson:json-encode($item as item()?) as item()*
  val jsonEncode: F[FunctionDecl1] =
    ejs.declare[F]("json-encode") flatMap (_(
      $("item") as ST("item()?")
    ).as(ST.Top) { item: XQuery =>
      val (jarr, jobj, t) = ($("jarr"), $("jobj"), $("t"))
      (typeOf(item) |@| jsonEncodeType(item)) { (tpe, encoded) =>
        typeswitch(item)(
          ST("text()")         return_ item,
          ST("boolean-node()") return_ item,
          ST("number-node()")  return_ item,
          ST("array-node()")   return_ item,
          ST("object-node()")  return_ item
        ) default (let_(t := tpe) return_ {
          if_(~t eq "string".xs)
          .then_(item)
          .else_(if_(~t eq "boolean".xs)
          .then_(boolNode(item))
          .else_(encoded))
        })
      }
    })

  // ejson:json-encode-type($item as item()?) as object-node()
  val jsonEncodeType: F[FunctionDecl1] =
    ejs.declare[F]("json-encode-type") flatMap (_(
      $("item") as ST("item()?")
    ).as(ST("object-node()")) { item: XQuery =>
      val t = $("t")
      typeOf(item) flatMap { tpe =>
        jsonDeserialize(let_(t := tpe) return_ {
          if_(fn.empty(~t))
          .then_(map.entry(EJsonTypeKey.xs, "na".xs))
          .else_(if_(~t eq "null".xs)
          .then_(map.entry(EJsonTypeKey.xs, "null".xs))
          .else_(map.new_(mkSeq_(
            map.entry(EJsonTypeKey.xs, ~t),
            map.entry(EJsonValueKey.xs, item)))))
        })
      }
    })

  // ejson:json-is-array($item as item()?) as xs:boolean
  val jsonIsArray: F[FunctionDecl1] =
    ejs.declare[F]("json-is-array") map (_(
      $("item") as ST("item()?")
    ).as(ST("xs:boolean")) { item: XQuery =>
      typeswitch(item)(ST("array-node()") return_ fn.True) default fn.False
    })

  // ejson:json-node-cast($node as node()) as item()*
  val jsonNodeCast: F[FunctionDecl1] =
    ejs.declare[F]("json-node-cast") flatMap (_(
      $("node") as ST("node()?")
    ).as(ST("item()?")) { (node: XQuery) =>
      val obj = $("obj")
      jsonObjectCast(~obj) map { objCast =>
        typeswitch(node)(
          obj as ST("object-node()") return_ κ(objCast)
        ) default node
      }
    })

  // ejson:json-node-to-string($node as node()) as xs:string?
  val jsonNodeToString: F[FunctionDecl1] =
    ejs.declare[F]("json-node-to-string") flatMap (_(
      $("node") as ST("node()")
    ).as(ST("xs:string?")) { node: XQuery =>
      val obj = $("obj")
      jsonObjectToString(~obj) map { objStr =>
        typeswitch(node)(
          obj as ST("object-node()") return_ κ(objStr)
        ) default fn.string(node)
      }
    })

  // ejson:json-node-type($node as node()) as xs:string?
  val jsonNodeType: F[FunctionDecl1] =
    ejs.declare[F]("json-node-type") flatMap (_(
      $("node") as ST("node()")
    ).as(ST("xs:string?")) { node: XQuery =>
      val (num, obj) = ($("num"), $("obj"))
      jsonObjectType(~obj) map { objType =>
        typeswitch(node)(
                 ST("text()")         return_ "string".xs,
                 ST("null-node()")    return_ "null".xs,
                 ST("boolean-node()") return_ "boolean".xs,
                 ST("number-node()")  return_ "decimal".xs,
                 ST("array-node()")   return_ "array".xs,
          obj as ST("object-node()")  return_ κ(objType)
        ) default emptySeq
      }
    })

  // ejson:json-object-cast($obj as object-node()) as item()*
  val jsonObjectCast: F[FunctionDecl1] =
    ejs.declare[F]("json-object-cast") flatMap (_(
      $("obj") as ST("object-node()")
    ).as(ST.Top) { (obj: XQuery) =>
      val (o, t, v) = ($("o"), $("t"), $("v"))

      jsonObjectType(obj) map { tpe =>
        let_(
          t := tpe,
          v := map.get(xdmp.fromJson(obj), EJsonValueKey.xs))
        .return_ {
          if_(~t eq "timestamp".xs)
          .then_(xs.dateTime(~v))
          .else_(if_(~t eq "date".xs)
          .then_(xs.date(~v))
          .else_(if_(~t eq "time".xs)
          .then_(xs.time(~v))
          .else_(if_(~t eq "interval".xs)
          .then_(xs.duration(~v))
          .else_(if_(~t eq "integer".xs)
          .then_(xs.integer(~v))
          .else_(if_(~t eq "decimal".xs)
          .then_(xs.double(~v))
          .else_(if_(~t eq "id".xs)
          .then_(xs.string(~v))
          .else_(if_(~t eq "null".xs)
          .then_(nullNode)
          .else_(if_(~t eq "na".xs)
          .then_(emptySeq)
          .else_(if_(~t eq "binary".xs)
          .then_ {
            if_(isCastable(~v, ST("xs:hexBinary")))
            .then_(xs.base64Binary(xs.hexBinary(~v)))
            .else_(xs.base64Binary(~v))
          }
          .else_(obj))))))))))
        }
      }
    })

  // ejson:json-object-delete($obj as object-node()?, $key as xs:string) as object-node()?
  val jsonObjectDelete: F[FunctionDecl2] =
    ejs.declare[F]("json-object-delete") flatMap (_(
      $("obj") as ST("object-node()?"),
      $("key") as ST("xs:string")
    ).as(ST(s"object-node()?")) { (obj: XQuery, key: XQuery) =>
      val o = $("o")
      jsonDeserialize(~o) map { deserialized =>
        let_(
          o      := xdmp.fromJson(obj),
          $("_") := map.delete(~o, key))
        .return_(deserialized)
      }
    })

  // ejson:json-object-insert($obj as object-node()?, $key as xs:string, $value as item()) as object-node()?
  val jsonObjectInsert: F[FunctionDecl3] =
    ejs.declare[F]("json-object-insert") flatMap (_(
      $("obj")   as ST("object-node()?"),
      $("key")   as ST("xs:string"),
      $("value") as ST("item()")
    ).as(ST(s"object-node()?")) { (obj: XQuery, key: XQuery, value: XQuery) =>
      val o = $("o")
      jsonDeserialize(~o) map { deserialized =>
        let_(
          o      := xdmp.fromJson(obj),
          $("_") := map.put(~o, key, value))
        .return_(deserialized)
      }
    })

  // ejson:json-object-merge($o1 as object-node()?, $o2 as object-node()?) as object-node()
  val jsonObjectMerge: F[FunctionDecl2] =
    ejs.declare[F]("json-object-merge") flatMap (_(
      $("obj1") as ST("object-node()?"),
      $("obj2") as ST("object-node()?")
    ).as(ST(s"object-node()")) { (obj1: XQuery, obj2: XQuery) =>
      jsonDeserialize(map.new_(mkSeq_(xdmp.fromJson(obj1), xdmp.fromJson(obj2))))
    })

  // ejson:json-object-type($obj as object-node()) as xs:string
  val jsonObjectType: F[FunctionDecl1] =
    ejs.declare[F]("json-object-type") map (_(
      $("obj") as ST("object-node()")
    ).as(ST("xs:string")) { obj: XQuery =>
      val o = $("o")
      let_(o := xdmp.fromJson(obj)) return_ {
        if_(map.contains(~o, EJsonTypeKey.xs))
        .then_(map.get(~o, EJsonTypeKey.xs))
        .else_("object".xs)
      }
    })

  // ejson:json-object-to-string($obj as object-node()) as xs:string
  val jsonObjectToString: F[FunctionDecl1] =
    ejs.declare[F]("json-object-to-string") map (_(
      $("obj") as ST("object-node()")
    ).as(ST("xs:string")) { obj: XQuery =>
      val o = $("o")
      let_(o := xdmp.fromJson(obj)) return_ {
        fn.string(if_(map.contains(~o, EJsonValueKey.xs))
        .then_(map.get(~o, EJsonValueKey.xs))
        .else_(obj))
      }
    })
}
