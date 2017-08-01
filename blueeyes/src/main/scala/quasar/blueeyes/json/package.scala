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

package quasar.blueeyes

import scalaz._, Scalaz._
import quasar.blueeyes.json.serialization.Decomposer

package object json {
  import JValue._

  type JFieldTuple = (String, JValue)

  def jarray(elements: JValue*): JValue                                    = JArray(elements.toList)
  def jobject(fields: JField*): JValue                                     = JObject(fields.toList)
  def jfield[A](name: String, value: A)(implicit d: Decomposer[A]): JField = JField(name, d(value))

  private def ppath(p: String) = if (p startsWith ".") p else "." + p

  implicit def liftJPathField(name: String): JPathNode = JPathField(name)
  implicit def liftJPathIndex(index: Int): JPathNode   = JPathIndex(index)
  implicit def liftJPath(path: String): JPath          = JPath(path)

  implicit val JPathNodeOrder: Order[JPathNode] = Order orderBy (x => x.optName -> x.optIndex)
  implicit val JPathNodeOrdering                = JPathNodeOrder.toScalaOrdering
  implicit val JPathOrder: Order[JPath]         = Order orderBy (_.nodes)
  implicit val JPathOrdering                    = JPathOrder.toScalaOrdering

  implicit val JObjectMergeMonoid = new Monoid[JObject] {
    val zero = JObject(Nil)

    def append(v1: JObject, v2: => JObject): JObject = v1.merge(v2).asInstanceOf[JObject]
  }

  val NoJPath     = JPath()
  type JPath      = quasar.precog.JPath
  type JPathNode  = quasar.precog.JPathNode
  type JPathField = quasar.precog.JPathField
  val JPathField  = quasar.precog.JPathField
  type JPathIndex = quasar.precog.JPathIndex
  val JPathIndex  = quasar.precog.JPathIndex

  private[json] def buildString(f: StringBuilder => Unit): String = {
    val sb = new StringBuilder
    f(sb)
    sb.toString
  }

  implicit class JPathOps(private val x: JPath) {
    import x._

    def parent: Option[JPath]        = if (nodes.isEmpty) None else Some(JPath(nodes dropRight 1: _*))
    def apply(index: Int): JPathNode = nodes(index)
    def head: Option[JPathNode]      = nodes.headOption
    def tail: JPath                  = JPath(nodes.tail)
    def path: String                 = x.to_s

    def ancestors: List[JPath] = {
      def loop(path: JPath, acc: List[JPath]): List[JPath] = path.parent.fold(acc)(p => loop(p, p :: acc))
      loop(x, Nil).reverse
    }

    def \(that: JPath): JPath   = JPath(nodes ++ that.nodes)
    def \(that: String): JPath  = JPath(nodes :+ JPathField(that))
    def \(that: Int): JPath     = JPath(nodes :+ JPathIndex(that))
    def \:(that: JPath): JPath  = JPath(that.nodes ++ nodes)
    def \:(that: String): JPath = JPath(JPathField(that) +: nodes)
    def \:(that: Int): JPath    = JPath(JPathIndex(that) +: nodes)

    def dropPrefix(p: JPath): Option[JPath] = {
      def remainder(nodes: List[JPathNode], toDrop: List[JPathNode]): Option[JPath] = {
        nodes match {
          case x :: xs =>
            toDrop match {
              case `x` :: ys => remainder(xs, ys)
              case Nil       => Some(JPath(nodes))
              case _         => None
            }

          case Nil =>
            if (toDrop.isEmpty) Some(JPath(nodes))
            else None
        }
      }

      remainder(nodes, p.nodes)
    }
    def extract(jvalue: JValue): JValue = {
      def extract0(path: List[JPathNode], d: JValue): JValue = path match {
        case Nil                     => d
        case JPathField(name) :: tl  => extract0(tl, d \ name)
        case JPathIndex(index) :: tl => extract0(tl, d(index))
      }
      extract0(nodes, jvalue)
    }
    def expand(jvalue: JValue): List[JPath] = {
      def expand0(current: List[JPathNode], right: List[JPathNode], d: JValue): List[JPath] = right match {
        case Nil                            => JPath(current) :: Nil
        case (hd @ JPathIndex(index)) :: tl => expand0(current :+ hd, tl, jvalue(index))
        case (hd @ JPathField(name)) :: tl  => expand0(current :+ hd, tl, jvalue \ name)
      }
      expand0(Nil, nodes, jvalue)
    }
  }

  implicit class JPathNodeOps(private val x: JPathNode) {
    def optName: Option[String] = x match {
      case JPathField(x) => Some(x)
      case _             => None
    }
    def optIndex: Option[Int] = x match {
      case JPathIndex(x) => Some(x)
      case _             => None
    }
    def \(that: JPath)     = JPath(x :: that.nodes)
    def \(that: JPathNode) = JPath(x :: that :: Nil)
  }

  private def renderJArray(self: JArray, sb: StringBuilder, mode: RenderMode, indent: String) {
    import self.elements
    mode match {
      case Compact =>
        // render as compactly as possible
        sb.append("[")
        var seen = false
        elements.foreach { v =>
          if (seen) sb.append(",") else seen = true
          renderJValue(v, sb, mode, "")
        }
        sb.append("]")

      case _ =>
        val indent2   = indent + "  "
        val oneLine   = !self.isNested
        val delimiter = if (oneLine) ", " else ",\n"

        sb.append("[")
        if (!oneLine) sb.append("\n")

        var seen = false
        elements.foreach { v =>
          if (seen) sb.append(delimiter) else seen = true
          if (!oneLine) sb.append(indent2)
          renderJValue(v, sb, mode, indent2)
        }

        if (!oneLine) {
          sb.append("\n")
          sb.append(indent)
        }
        sb.append("]")
    }
  }

  private def renderJValue(self: JValue, sb: StringBuilder, mode: RenderMode, indent: String): Unit = self match {
    case JString(value) => JString.internalEscape(sb, value)
    case x: JObject     => renderJObject(x, sb, mode, indent)
    case x: JArray      => renderJArray(x, sb, mode, indent)
    case _              => sb append self.renderCompact
  }

  private def renderJObject(self: JObject, sb: StringBuilder, mode: RenderMode, indent: String) {
    import self.fields
    mode match {
      case Compact =>
        // render as compactly as possible
        sb.append("{")
        var seen = false
        fields.foreach {
          case (k, v) =>
            if (seen) sb.append(",") else seen = true
            JString.internalEscape(sb, k)
            sb.append(":")
            renderJValue(v, sb, Compact, "")
        }
        sb.append("}")

      case _ =>
        val indent2   = indent + "  "
        val oneLine   = fields.size < 4 && !self.isNested
        val delimiter = if (oneLine) ", " else ",\n"

        sb.append("{")
        if (!oneLine) sb.append("\n")

        var seen = false
        def handlePair(tpl: (String, JValue)) {
          if (seen) sb.append(delimiter) else seen = true
          if (!oneLine) sb.append(indent2)
          JString.internalEscape(sb, tpl._1)
          sb.append(": ")
          renderJValue(tpl._2, sb, mode, indent2)
        }

        if (mode == Canonical)
          fields.toSeq.sortMe.foreach(handlePair)
        else
          fields.foreach(handlePair)

        if (!oneLine) {
          sb.append("\n")
          sb.append(indent)
        }
        sb.append("}")
    }
  }


  implicit class JValueOps(private val self: JValue) {
    import Validation._

    def diff(other: JValue)          = Diff.diff(self, other)
    def merge(other: JValue): JValue = Merge.merge(self, other)

    def typeIndex: Int = self match {
      case JUndefined => -1
      case JNull      => 0
      case _: JBool   => 1
      case _: JNum    => 4
      case _: JString => 5
      case _: JArray  => 6
      case _: JObject => 7
    }

    def isDefined = self != JUndefined

    def renderPretty: String = self match {
      case JString(s) => JString escape s
      case x: JObject => buildString(renderJObject(x, _, Pretty, ""))
      case x: JArray  => buildString(renderJArray(x, _, Pretty, ""))
      case _          => renderCompact
    }
    def renderCanonical: String = self match {
      case _: JArray | _: JObject => buildString(renderJValue(self, _, Canonical, ""))
      case _                      => renderCompact
    }
    def renderCompact: String = self match {
      case JNull      => "null"
      case JUndefined => "<undef>"
      case JTrue      => "true"
      case JFalse     => "false"
      case JString(s) => JString escape s
      case x: JNum    => x.toRawString
      case x: JObject => buildString(renderJObject(x, _, Compact, ""))
      case x: JArray  => buildString(renderJArray(x, _, Compact, ""))
    }

    def to_s = renderPretty

    def normalize: JValue = self match {
      case JUndefined       => sys error "Can't normalize JUndefined"
      case JObject(fields)  => JObject(fields filter (_._2.isDefined) mapValues (_.normalize) toMap)
      case JArray(elements) => JArray(elements filter (_.isDefined) map (_.normalize))
      case _                => self
    }

    def toOption                           = if (isDefined) Some(self) else None
    def getOrElse(that: => JValue): JValue = if (isDefined) self else that

    /** XPath-like expression to query JSON fields by name. Matches only fields on
      * next level.
      */
    def \(nameToFind: String): JValue = self match {
      case j @ JObject(fields) => j.get(nameToFind)
      case _                   => JUndefined
    }

    def \?(nameToFind: String): Option[JValue] = (self \ nameToFind).toOption

    /**
      * Returns the element as a JValue of the specified class.
      */
    def -->[A <: JValue](clazz: Class[A]): A = (self -->? clazz).getOrElse(sys.error("Expected class " + clazz + ", but found: " + self.getClass))

    /**
      * Returns the element as an option of a JValue of the specified class.
      */
    def -->?[A <: JValue](clazz: Class[A]): Option[A] = if (clazz.isAssignableFrom(self.getClass)) Some(self.asInstanceOf[A]) else None

    /**
      * Does a breadth-first traversal of all descendant JValues, beginning
      * with this one.
      */
    def breadthFirst: List[JValue] = {
      import scala.collection.immutable.Queue

      def breadthFirst0(cur: List[JValue], queue: Queue[JValue]): List[JValue] = {
        if (queue.isEmpty) cur
        else {
          val (head, nextQueue) = queue.dequeue

          breadthFirst0(head :: cur, head match {
            case JObject(fields) =>
              nextQueue.enqueue(fields.values.toList)

            case JArray(elements) =>
              nextQueue.enqueue(elements)

            case jvalue => nextQueue
          })
        }
      }

      breadthFirst0(Nil, Queue.empty.enqueue(self)).reverse
    }

    /** XPath-like expression to query JSON fields by name. Returns all matching fields.
      */
    def \\(nameToFind: String): JValue = {
      def find(json: JValue): List[JValue] = json match {
        case JObject(l) =>
          l.foldLeft(List[JValue]()) {
            case (acc, (`nameToFind`, v)) => v :: acc ::: find(v)
            case (acc, (_, v))            => acc ::: find(v)
          }

        case JArray(l) => l flatMap find
        case _         => Nil
      }
      find(self) match {
        case x :: Nil => x
        case x        => JArray(x)
      }
    }

    /** Gets the specified value located at the terminal of the specified path.
      */
    def get(path: JPath): JValue       = path extract self
    def apply(path: JPath): JValue     = path extract self
    def apply(node: JPathNode): JValue = apply(JPath(node))

    /** Return nth element from JSON Array.
      */
    def apply(i: Int): JValue = self match {
      case JArray(xs) => xs lift i getOrElse JUndefined
      case _          => JUndefined
    }

    def insert(path: JPath, value: JValue): Validation[Throwable, JValue] = value match {
      case JUndefined => success(self)
      case value      => Validation fromTryCatchNonFatal unsafeInsert(path, value)
    }

    /**
      * A safe merge function that ensures that values are not overwritten.
      */
    def insertAll(other: JValue): ValidationNel[Throwable, JValue] = {
      other.flattenWithPath.foldLeft[ValidationNel[Throwable, JValue]](success(self)) {
        case (acc, (path, value)) => acc flatMap { (_: JValue).insert(path, value).toValidationNel }
      }
    }

    def unsafeInsert(rootPath: JPath, rootValue: JValue): JValue = {
      JValue.unsafeInsert(self, rootPath, rootValue)
    }

    def set(path: JPath, value: JValue): JValue =
      if (path == NoJPath) value
      else {
        def arraySet(l: List[JValue], i: Int, rem: JPath, v: JValue): List[JValue] = {
          def update(l: List[JValue], j: Int): List[JValue] = l match {
            case x :: xs => (if (j == i) x.set(rem, v) else x) :: update(xs, j + 1)
            case Nil     => Nil
          }

          update(l.padTo(i + 1, JUndefined), 0)
        }

        self match {
          case obj @ JObject(fields) =>
            path.nodes match {
              case JPathField(name) :: nodes =>
                val (child, rest) = obj.partitionField(name)
                rest + JField(name, child.set(JPath(nodes), value))

              case x =>
                sys.error("Objects are not indexed: attempted to set " + path + " on " + self)
            }

          case arr @ JArray(elements) =>
            path.nodes match {
              case JPathIndex(index) :: nodes =>
                JArray(arraySet(elements, index, JPath(nodes), value))
              case x =>
                sys.error("Arrays have no fields: attempted to set " + path + " on " + self)
            }

          case _ =>
            path.nodes match {
              case Nil                => value
              case JPathIndex(_) :: _ => JArray(Nil).set(path, value)
              case JPathField(_) :: _ => JObject(Nil).set(path, value)
            }
        }
      }

    def delete(path: JPath): Option[JValue] = {
      path.nodes match {
        case JPathField(name) :: xs =>
          self match {
            case JObjectFields(fields) =>
              Some(
                JObject(fields flatMap {
                  case JField(`name`, value) =>
                    value.delete(JPath(xs: _*)) map { v =>
                      JField(name, v)
                    }
                  case unmodified => Some(unmodified)
                })
              )
            case unmodified => Some(unmodified)
          }

        case JPathIndex(idx) :: xs =>
          self match {
            case JArray(elements) => Some(JArray(elements.zipWithIndex.flatMap { case (v, i) => if (i == idx) v.delete(JPath(xs: _*)) else Some(v) }))
            case unmodified       => Some(unmodified)
          }

        case Nil => None
      }
    }

    /** Return direct child elements.
      */
    def children: Iterable[JValue] = self match {
      case JObject(fields) => fields.values
      case JArray(l)       => l
      case _               => List.empty
    }

    /** Return a combined value by folding over JSON by applying a function <code>f</code>
      * for each element. The initial value is <code>z</code>.
      */
    def foldDown[A](z: A)(f: (A, JValue) => A): A = foldDownWithPath(z) { (acc, p, v) =>
      f(acc, v)
    }

    /** Return a combined value by folding over JSON by applying a function `f`
      * for each element, passing along the path to the elements. The initial
      * value is `z`.
      */
    def foldDownWithPath[A](z: A)(f: (A, JPath, JValue) => A): A = {
      def rec(acc: A, p: JPath, v: JValue): A = {
        val newAcc = f(acc, p, v)

        v match {
          case JObject(l) =>
            l.foldLeft(newAcc) {
              case (acc, field) =>
                rec(acc, p \ field._1, field._2)
            }

          case JArray(l)                                         =>
            l.zipWithIndex.foldLeft(newAcc) { case (a, (e, idx)) => rec(a, p \ idx, e) }

          case _ => newAcc
        }
      }

      rec(z, NoJPath, self)
    }

    /** Return a combined value by folding over JSON by applying a function <code>f</code>
      * for each element. The initial value is <code>z</code>.
      */
    def foldUp[A](z: A)(f: (A, JValue) => A): A = foldUpWithPath(z) { (acc, p, v) =>
      f(acc, v)
    }

    /** Return a combined value by folding over JSON by applying a function `f`
      * for each element, passing along the path to the elements. The initial
      * value is `z`.
      */
    def foldUpWithPath[A](z: A)(f: (A, JPath, JValue) => A): A = {
      def rec(acc: A, p: JPath, v: JValue): A = {
        f(v match {
          case JObject(l) =>
            l.foldLeft(acc) { (acc, field) =>
              rec(acc, p \ field._1, field._2)
            }

          case JArray(l) =>
            l.zipWithIndex.foldLeft(acc) { (a, t) =>
              val (e, idx) = t; rec(a, p \ idx, e)
            }

          case _ => acc
        }, p, v)
      }

      rec(z, NoJPath, self)
    }

    /** Return a new JValue resulting from applying the given function <code>f</code>
      * to each element, moving from the bottom-up.
      */
    def mapUp(f: JValue => JValue): JValue = mapUpWithPath((p, j) => f(j))

    /** Return a new JValue resulting from applying the given function <code>f</code>
      * to each element and its path, moving from the bottom-up.
      */
    def mapUpWithPath(f: (JPath, JValue) => JValue): JValue = {
      def rec(p: JPath, v: JValue): JValue = v match {
        case JObject(l) =>
          f(p, JObject(l.flatMap { f =>
            val v2 = rec(p \ f._1, f._2)
            if (!v2.isDefined) Nil else JField(f._1, v2) :: Nil

          }))

        case JArray(l) =>
          f(
            p,
            JArray(l.zipWithIndex.flatMap(t =>
              rec(p \ t._2, t._1) match {
                case JUndefined => Nil
                case x          => x :: Nil
              })))

        case x => f(p, x)
      }
      rec(NoJPath, self)
    }

    /** Return a new JValue resulting from applying the given function <code>f</code>
      * to each element, moving from the top-down.
      */
    def mapDown(f: JValue => JValue): JValue = mapDownWithPath((p, j) => f(j))

    /** Return a new JValue resulting from applying the given function <code>f</code>
      * to each element and its path, moving from the top-down.
      */
    def mapDownWithPath(f: (JPath, JValue) => JValue): JValue = {
      def rec(p: JPath, v: JValue): JValue = {
        f(p, v) match {
          case JObject(l) =>
            JObject(l.flatMap {
              case (k, v) =>
                val v2 = rec(p \ k, v)
                if (v2 == JUndefined) Nil else JField(k, v2) :: Nil
            })

          case JArray(l) =>
            JArray(l.zipWithIndex flatMap {
              case (e, idx) =>
                rec(p \ idx, e) match {
                  case JUndefined => Nil
                  case x          => x :: Nil
                }
            })

          case x => x
        }
      }

      rec(NoJPath, self)
    }

    /** Return a new JValue resulting from applying the given partial function <code>f</code>
      * to each element in JSON.
      */
    def transform(f: PartialFunction[JValue, JValue]): JValue = mapUp { x =>
      if (f.isDefinedAt(x)) f(x) else x
    }

    /** Replaces the matched path values with the result of calling the
      * replacer function on the matches. If the path has no values, the
      * method has no effect -- i.e. it is not an error to specify paths
      * which do not exist.
      */
    def replace(target: JPath, replacer: JValue => JValue): JValue = {
      def replace0(target: JPath, j: JValue): JValue = target.nodes match {
        case Nil => replacer(j)

        case head :: tail =>
          head match {
            case JPathField(name1) =>
              j match {
                case JObjectFields(fields) =>
                  JObject(fields.map {
                    case JField(name2, value) if (name1 == name2) => JField(name1, replace0(JPath(tail: _*), value))

                    case field => field
                  })

                case jvalue => jvalue
              }

            case JPathIndex(index) =>
              j match {
                case JArray(elements) =>
                  val split  = elements.splitAt(index)
                  val prefix = split._1
                  val middle = replace0(JPath(tail: _*), split._2.head)
                  val suffix = split._2.drop(1)

                  JArray(prefix ++ (middle :: suffix))

                case jvalue => jvalue
              }
          }
      }

      target.expand(self).foldLeft(self) { (jvalue, expansion) =>
        replace0(expansion, jvalue)
      }
    }

    /** A shorthand for the other replacement in the case that the replacement
      * does not depend on the value being replaced.
      */
    def replace(target: JPath, replacement: JValue): JValue = replace(target, r => replacement)

    /** Return the first element from JSON which matches the given predicate.
      */
    def find(p: JValue => Boolean): Option[JValue] = {
      def find(json: JValue): Option[JValue] = {
        if (p(json)) Some(json)
        else {
          json match {
            case JObject(l) => l.flatMap({ case (_, v) => find(v) }).headOption
            case JArray(l)  => l.flatMap(find).headOption
            case _          => None
          }
        }
      }

      find(self)
    }

    /** Return a List of all elements which matches the given predicate.
      */
    def filter(p: JValue => Boolean): List[JValue] =
      foldDown(List.empty[JValue])((acc, e) => if (p(e)) e :: acc else acc).reverse

    def flatten: List[JValue] =
      foldDown(List.empty[JValue])((acc, e) => e :: acc).reverse

    /** Flattens the JValue down to a list of path to simple JValue primitive.
      */
    def flattenWithPath: List[(JPath, JValue)] = {
      def flatten0(path: JPath)(value: JValue): List[(JPath, JValue)] = value match {
        case JObject.empty | JArray.empty => List(path -> value)
        case JObject(fields)              => fields.flatMap({ case (k, v) => flatten0(path \ k)(v) })(collection.breakOut)
        case JArray(elements)             => elements.zipWithIndex.flatMap({ case (element, index) => flatten0(path \ index)(element) })
        case JUndefined                   => Nil
        case leaf                         => List(path -> leaf)
      }

      flatten0(NoJPath)(self)
    }

    /** Concatenate with another JSON.
      * This is a concatenation monoid: (JValue, ++, JUndefined)
      */
    def ++(other: JValue): JValue = {
      def append(value1: JValue, value2: JValue): JValue = (value1, value2) match {
        case (JUndefined, x)          => x
        case (x, JUndefined)          => x
        case (JArray(xs), JArray(ys)) => JArray(xs ::: ys)
        case (JArray(xs), v: JValue)  => JArray(xs ::: List(v))
        case (v: JValue, JArray(xs))  => JArray(v :: xs)
        case (x, y)                   => JArray(x :: y :: Nil)
      }
      append(self, other)
    }

    /** Return a JSON where all elements matching the given predicate are removed.
      */
    def remove(p: JValue => Boolean): JValue = self mapUp {
      case x if p(x) => JUndefined
      case x         => x
    }

    /**
      * Remove instances of Nothing from the data structure.
      */
    def minimize: Option[JValue] = {
      self match {
        case JObjectFields(fields) => Some(JObject(fields flatMap { case JField(k, v) => v.minimize.map(JField(k, _)) }))
        case JArray(elements)      => Some(JArray(elements.flatMap(_.minimize)))
        case JUndefined            => None
        case value                 => Some(value)
      }
    }
  }
}

package json {
  object JPath {
    def apply(path: String): JPath = {
      val PathPattern  = """[.]|(?=\[\d+\])""".r
      val IndexPattern = """^\[(\d+)\]$""".r
      def ppath(p: String) = if (p startsWith ".") p else "." + p
      JPath(
        PathPattern split ppath(path) map (_.trim) flatMap {
          case ""                  => None
          case IndexPattern(index) => Some(JPathIndex(index.toInt))
          case name                => Some(JPathField(name))
        } toList
      )
    }
    def apply(n: JPathNode*): JPath                 = new JPath(n.toList)
    def apply(n: List[JPathNode]): JPath            = new JPath(n)
    def unapply(path: JPath): Some[List[JPathNode]] = Some(path.nodes)
  }
}
