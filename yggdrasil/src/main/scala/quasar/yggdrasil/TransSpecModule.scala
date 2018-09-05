/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.yggdrasil

import quasar.precog.common._
import quasar.yggdrasil.bytecode.JType
import quasar.yggdrasil.table.{CF1, CF2, CFN, CMapper, CScanner}

object TransSpecModule {
  object paths {
    val Key     = CPathField("key")
    val Value   = CPathField("value")
    val Group   = CPathField("group")
    val SortKey = CPathField("sortkey")
  }

  sealed trait Definedness
  case object AnyDefined extends Definedness
  case object AllDefined extends Definedness
}

trait TransSpecModule {
  import TransSpecModule._

  object trans {
    sealed trait TransSpec[+A <: SourceType]
    sealed trait SourceType

    sealed trait ObjectSpec[+A <: SourceType] extends TransSpec[A]
    sealed trait ArraySpec[+A <: SourceType]  extends TransSpec[A]

    sealed trait Source1 extends SourceType
    case object Source extends Source1

    sealed trait Source2 extends SourceType
    case object SourceLeft  extends Source2
    case object SourceRight extends Source2

    case class Leaf[+A <: SourceType](source: A) extends TransSpec[A]

    case class Filter[+A <: SourceType](source: TransSpec[A], predicate: TransSpec[A]) extends TransSpec[A]

    // Adds a column to the output in the manner of scanLeft
    case class Scan[+A <: SourceType](source: TransSpec[A], scanner: CScanner) extends TransSpec[A]

    case class MapWith[+A <: SourceType](source: TransSpec[A], mapper: CMapper) extends TransSpec[A]

    case class Map1[+A <: SourceType](source: TransSpec[A], f: CF1) extends TransSpec[A]

    case class DeepMap1[+A <: SourceType](source: TransSpec[A], f: CF1) extends TransSpec[A]

    // apply a function to the cartesian product of the transformed left and right subsets of columns
    case class Map2[+A <: SourceType](left: TransSpec[A], right: TransSpec[A], f: CF2) extends TransSpec[A]

    // apply a function to an array
    case class MapN[+A <: SourceType](contents: TransSpec[A], f: CFN) extends TransSpec[A]

    // Perform the specified transformation on the all sources, and then create a new set of columns
    // containing all the resulting columns.
    case class InnerObjectConcat[+A <: SourceType](objects: TransSpec[A]*) extends ObjectSpec[A]

    case class OuterObjectConcat[+A <: SourceType](objects: TransSpec[A]*) extends ObjectSpec[A]

    case class ObjectDelete[+A <: SourceType](source: TransSpec[A], fields: Set[CPathField]) extends TransSpec[A]

    case class InnerArrayConcat[+A <: SourceType](arrays: TransSpec[A]*) extends ArraySpec[A]

    case class OuterArrayConcat[+A <: SourceType](arrays: TransSpec[A]*) extends ArraySpec[A]

    // Take the output of the specified TransSpec and prefix all of the resulting selectors with the
    // specified field.
    case class WrapObject[+A <: SourceType](source: TransSpec[A], field: String) extends ObjectSpec[A]

    case class WrapObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]

    case class WrapArray[+A <: SourceType](source: TransSpec[A]) extends ArraySpec[A]

    case class DerefObjectStatic[+A <: SourceType](source: TransSpec[A], field: CPathField) extends TransSpec[A]

    case class DerefMetadataStatic[+A <: SourceType](source: TransSpec[A], field: CPathMeta) extends TransSpec[A]

    case class DerefObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]

    case class DerefArrayStatic[+A <: SourceType](source: TransSpec[A], element: CPathIndex) extends TransSpec[A]

    case class DerefArrayDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]

    case class ArraySwap[+A <: SourceType](source: TransSpec[A], index: Int) extends TransSpec[A]

    // Filter out all the source columns whose selector and CType are not specified by the supplied JType
    case class Typed[+A <: SourceType](source: TransSpec[A], tpe: JType) extends TransSpec[A]

    // Filter out all the source columns whose selector and CType are not specified by the supplied JType
    // if the set of columns does not cover the JType specified, this will return the empty slice.
    case class TypedSubsumes[+A <: SourceType](source: TransSpec[A], tpe: JType) extends TransSpec[A]

    // return a Boolean column
    // returns true for a given row when all of the columns specified by the supplied JType are defined
    case class IsType[+A <: SourceType](source: TransSpec[A], tpe: JType) extends TransSpec[A]

    case class Equal[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]

    case class EqualLiteral[+A <: SourceType](left: TransSpec[A], right: CValue, invert: Boolean) extends TransSpec[A]

    // this has to be primitive because of how nutso equality is
    case class Within[+A <: SourceType](item: TransSpec[A], in: TransSpec[A]) extends TransSpec[A]

    // this has to be primitive because it produces an array
    case class Range[+A <: SourceType](lower: TransSpec[A], upper: TransSpec[A]) extends TransSpec[A]

    // target is the transspec that provides defineedness information. The resulting table will be defined
    // and have the constant value wherever a row provided by the target transspec has at least one member
    // that is not undefined
    case class ConstLiteral[+A <: SourceType](value: CValue, target: TransSpec[A]) extends TransSpec[A]

    case class FilterDefined[+A <: SourceType](source: TransSpec[A], definedFor: TransSpec[A], definedness: Definedness) extends TransSpec[A]

    case class IfUndefined[+A <: SourceType](source: TransSpec[A], default: TransSpec[A]) extends TransSpec[A]

    case class Cond[+A <: SourceType](pred: TransSpec[A], left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]

    case class ToNumber[+A <: SourceType](source: TransSpec[A]) extends TransSpec[A]

    type TransSpec1 = TransSpec[Source1]

    object TransSpec {

      import CPath._

      def concatChildren[A <: SourceType](tree: CPathTree[Int], leaf: TransSpec[A] = Leaf(Source)): TransSpec[A] = {
        def createSpecs(trees: Seq[CPathTree[Int]]): Seq[TransSpec[A]] = trees.map { child =>
          child match {
            case node @ RootNode(seq)                  => concatChildren(node, leaf)
            case node @ FieldNode(CPathField(name), _) => trans.WrapObject(concatChildren(node, leaf), name)
            case node @ IndexNode(CPathIndex(_), _)    => trans.WrapArray(concatChildren(node, leaf)) //assuming that indices received in order
            case LeafNode(idx)                         => trans.DerefArrayStatic(leaf, CPathIndex(idx))
          }
        }

        val initialSpecs = tree match {
          case RootNode(children)     => createSpecs(children)
          case FieldNode(_, children) => createSpecs(children)
          case IndexNode(_, children) => createSpecs(children)
          case LeafNode(_)            => Seq()
        }

        val result = initialSpecs reduceOption { (t1, t2) =>
          (t1, t2) match {
            case (t1: ObjectSpec[_], t2: ObjectSpec[_]) => trans.InnerObjectConcat(t1, t2)
            case (t1: ArraySpec[_], t2: ArraySpec[_])   => trans.InnerArrayConcat(t1, t2)
            case _                                      => sys.error("cannot have this")
          }
        }

        result getOrElse leaf
      }

      def mapSources[A <: SourceType, B <: SourceType](spec: TransSpec[A])(f: A => B): TransSpec[B] = {
        spec match {
          case Leaf(source)                      => Leaf(f(source))
          case trans.ConstLiteral(value, target) => trans.ConstLiteral(value, mapSources(target)(f))

          case trans.Filter(source, pred) => trans.Filter(mapSources(source)(f), mapSources(pred)(f))
          case trans.FilterDefined(source, definedFor, definedness) =>
            trans.FilterDefined(mapSources(source)(f), mapSources(definedFor)(f), definedness)

          case trans.IfUndefined(source, target) => trans.IfUndefined(mapSources(source)(f), mapSources(target)(f))

          case Scan(source, scanner)   => Scan(mapSources(source)(f), scanner)
          case MapWith(source, mapper) => MapWith(mapSources(source)(f), mapper)

          case trans.Map1(source, f1)      => trans.Map1(mapSources(source)(f), f1)
          case trans.DeepMap1(source, f1)  => trans.DeepMap1(mapSources(source)(f), f1)
          case trans.Map2(left, right, f2) => trans.Map2(mapSources(left)(f), mapSources(right)(f), f2)
          case trans.MapN(contents, f1)    => trans.MapN(mapSources(contents)(f), f1)

          case trans.OuterObjectConcat(objects @ _ *) => trans.OuterObjectConcat(objects.map(mapSources(_)(f)): _*)
          case trans.InnerObjectConcat(objects @ _ *) => trans.InnerObjectConcat(objects.map(mapSources(_)(f)): _*)
          case trans.ObjectDelete(source, fields)     => trans.ObjectDelete(mapSources(source)(f), fields)
          case trans.InnerArrayConcat(arrays @ _ *)   => trans.InnerArrayConcat(arrays.map(mapSources(_)(f)): _*)
          case trans.OuterArrayConcat(arrays @ _ *)   => trans.OuterArrayConcat(arrays.map(mapSources(_)(f)): _*)

          case trans.WrapObject(source, field)      => trans.WrapObject(mapSources(source)(f), field)
          case trans.WrapObjectDynamic(left, right) => trans.WrapObjectDynamic(mapSources(left)(f), mapSources(right)(f))
          case trans.WrapArray(source)              => trans.WrapArray(mapSources(source)(f))

          case DerefMetadataStatic(source, field) => DerefMetadataStatic(mapSources(source)(f), field)

          case DerefObjectStatic(source, field)  => DerefObjectStatic(mapSources(source)(f), field)
          case DerefObjectDynamic(left, right)   => DerefObjectDynamic(mapSources(left)(f), mapSources(right)(f))
          case DerefArrayStatic(source, element) => DerefArrayStatic(mapSources(source)(f), element)
          case DerefArrayDynamic(left, right)    => DerefArrayDynamic(mapSources(left)(f), mapSources(right)(f))

          case trans.ArraySwap(source, index) => trans.ArraySwap(mapSources(source)(f), index)

          case Typed(source, tpe)         => Typed(mapSources(source)(f), tpe)
          case TypedSubsumes(source, tpe) => TypedSubsumes(mapSources(source)(f), tpe)
          case IsType(source, tpe)        => IsType(mapSources(source)(f), tpe)

          case trans.Equal(left, right)                  => trans.Equal(mapSources(left)(f), mapSources(right)(f))
          case trans.EqualLiteral(source, value, invert) => trans.EqualLiteral(mapSources(source)(f), value, invert)

          case trans.Within(item, in) => trans.Within(mapSources(item)(f), mapSources(in)(f))
          case trans.Range(upper, lower) => trans.Range(mapSources(upper)(f), mapSources(lower)(f))

          case trans.Cond(pred, left, right) => trans.Cond(mapSources(pred)(f), mapSources(left)(f), mapSources(right)(f))
          case trans.ToNumber(source) => trans.ToNumber(mapSources(source)(f))
        }
      }

      def deepMap[A <: SourceType](spec: TransSpec[A])(f: PartialFunction[TransSpec[A], TransSpec[A]]): TransSpec[A] = spec match {
        case x if f isDefinedAt x => f(x)

        case x @ Leaf(_)                       => x
        case trans.ConstLiteral(value, target) => trans.ConstLiteral(value, deepMap(target)(f))

        case trans.Filter(source, pred) => trans.Filter(deepMap(source)(f), deepMap(pred)(f))
        case trans.FilterDefined(source, definedFor, definedness) =>
          trans.FilterDefined(deepMap(source)(f), deepMap(definedFor)(f), definedness)

        case trans.IfUndefined(source, target) => trans.IfUndefined(deepMap(source)(f), deepMap(target)(f))

        case Scan(source, scanner)   => Scan(deepMap(source)(f), scanner)
        case MapWith(source, mapper) => MapWith(deepMap(source)(f), mapper)

        case trans.Map1(source, f1)      => trans.Map1(deepMap(source)(f), f1)
        case trans.DeepMap1(source, f1)  => trans.DeepMap1(deepMap(source)(f), f1)
        case trans.Map2(left, right, f2) => trans.Map2(deepMap(left)(f), deepMap(right)(f), f2)
        case trans.MapN(contents, f1) => trans.MapN(deepMap(contents)(f), f1)

        case trans.OuterObjectConcat(objects @ _ *) => trans.OuterObjectConcat(objects.map(deepMap(_)(f)): _*)
        case trans.InnerObjectConcat(objects @ _ *) => trans.InnerObjectConcat(objects.map(deepMap(_)(f)): _*)
        case trans.ObjectDelete(source, fields)     => trans.ObjectDelete(deepMap(source)(f), fields)
        case trans.InnerArrayConcat(arrays @ _ *)   => trans.InnerArrayConcat(arrays.map(deepMap(_)(f)): _*)
        case trans.OuterArrayConcat(arrays @ _ *)   => trans.OuterArrayConcat(arrays.map(deepMap(_)(f)): _*)

        case trans.WrapObject(source, field)        => trans.WrapObject(deepMap(source)(f), field)
        case trans.WrapObjectDynamic(source, right) => trans.WrapObjectDynamic(deepMap(source)(f), deepMap(right)(f))
        case trans.WrapArray(source)                => trans.WrapArray(deepMap(source)(f))

        case DerefMetadataStatic(source, field) => DerefMetadataStatic(deepMap(source)(f), field)

        case DerefObjectStatic(source, field)  => DerefObjectStatic(deepMap(source)(f), field)
        case DerefObjectDynamic(left, right)   => DerefObjectDynamic(deepMap(left)(f), deepMap(right)(f))
        case DerefArrayStatic(source, element) => DerefArrayStatic(deepMap(source)(f), element)
        case DerefArrayDynamic(left, right)    => DerefArrayDynamic(deepMap(left)(f), deepMap(right)(f))

        case trans.ArraySwap(source, index) => trans.ArraySwap(deepMap(source)(f), index)

        case Typed(source, tpe)         => Typed(deepMap(source)(f), tpe)
        case TypedSubsumes(source, tpe) => TypedSubsumes(deepMap(source)(f), tpe)
        case IsType(source, tpe)        => IsType(deepMap(source)(f), tpe)

        case trans.Equal(left, right)                  => trans.Equal(deepMap(left)(f), deepMap(right)(f))
        case trans.EqualLiteral(source, value, invert) => trans.EqualLiteral(deepMap(source)(f), value, invert)

        case trans.Within(item, in)                    => trans.Within(deepMap(item)(f), deepMap(in)(f))
        case trans.Range(upper, lower)                 => trans.Range(deepMap(upper)(f), deepMap(lower)(f))

        case trans.Cond(pred, left, right) => trans.Cond(deepMap(pred)(f), deepMap(left)(f), deepMap(right)(f))
        case trans.ToNumber(source) => trans.ToNumber(deepMap(source)(f))
      }

      // reduce the TransSpec to a "normal form", in which nested *Concats are flattened into
      // single vararg calls and statically-known *DerefStatics are performed
      def normalize[A <: SourceType](ts: TransSpec[A], undef: TransSpec[A]): TransSpec[A] = {
        import scalaz.syntax.std.option._, scalaz.std.option._
        def flattenConcats: Option[TransSpec[A]] = {
          def flattenOuterArrayConcats[X <: SourceType](proj: TransSpec[X]): Option[List[TransSpec[X]]] = proj match {
            case OuterArrayConcat(ls@_*) =>
              Some(ls.toList.flatMap(a => flattenOuterArrayConcats(a).getOrElse(a :: Nil)))
            case _ => None
          }

          def flattenOuterObjectConcats[X <: SourceType](proj: TransSpec[X]): Option[List[TransSpec[X]]] = proj match {
            case OuterObjectConcat(ls@_*) =>
              Some(ls.toList.flatMap(a => flattenOuterObjectConcats(a).getOrElse(a :: Nil)))
            case _ => None
          }

          def flattenInnerArrayConcats[X <: SourceType](proj: TransSpec[X]): Option[List[TransSpec[X]]] = proj match {
            case InnerArrayConcat(ls@_*) =>
              Some(ls.toList.flatMap(a => flattenInnerArrayConcats(a).getOrElse(a :: Nil)))
            case _ => None
          }
          def flattenInnerObjectConcats[X <: SourceType](proj: TransSpec[X]): Option[List[TransSpec[X]]] = proj match {
            case InnerObjectConcat(ls@_*) =>
              Some(ls.toList.flatMap(a => flattenInnerObjectConcats(a).getOrElse(a :: Nil)))
            case _ => None
          }
          flattenOuterArrayConcats(ts).map(ks => OuterArrayConcat(ks.map(normalize(_, undef)): _*))
            .orElse(flattenOuterObjectConcats(ts).map(ks => OuterObjectConcat(ks.map(normalize(_, undef)): _*)))
            .orElse(flattenInnerArrayConcats(ts).map(ks => InnerArrayConcat(ks.map(normalize(_, undef)): _*)))
            .orElse(flattenInnerObjectConcats(ts).map(ks => InnerObjectConcat(ks.map(normalize(_, undef)): _*)))
        }

        flattenConcats.getOrElse {
          ts match {
            case WrapArray(t) =>
              WrapArray(normalize(t, undef))
            case WrapObject(t, f) =>
              WrapObject(normalize(t, undef), f)
            case WrapObjectDynamic(t, f) =>
              WrapObjectDynamic(normalize(t, undef), normalize(f, undef))
            case ConstLiteral(t, f) =>
              ConstLiteral(t, normalize(f, undef))
            case DerefArrayStatic(t, f@CPathIndex(i)) =>
              normalize[A](t, undef) match {
              case n@OuterArrayConcat(ks@_*) =>
                if (ks.length < (i + 1) && ks.forall(_.isInstanceOf[WrapArray[_]])) {
                  undef
                } else {
                  ks.foldLeft((0.some, none[TransSpec[A]])) {
                    case ((Some(a), b), WrapArray(nt)) =>
                      ((a + 1).some, if (a == i) normalize(nt, undef).some else b)
                    case ((_, b), _) => (none, b)
                  }._2.getOrElse(DerefArrayStatic(n, f))
                }
              case WrapArray(k) =>
                if (i == 0) {
                  k
                } else {
                  undef
                }
              case `undef` => undef
              case n@_ => DerefArrayStatic(n, f)
            }
            case DerefObjectStatic(t, f@CPathField(k)) =>
              normalize[A](t, undef) match {
              // ks is reversed before being folded, because keys are overriden
              // by operands on the right, not the left
              case n@OuterObjectConcat(ks@_*) =>
                ks.reverse.foldRight(undef) {
                case (WrapObject(s, k2), o) =>
                  if (k == k2) {
                    normalize(s, undef)
                  } else {
                    o
                  }
                case _ => DerefObjectStatic(n, f)
              }
              case WrapObject(s, `k`) => normalize(s, undef)
              case WrapObject(_, _) => undef
              case `undef` => undef
              case n@_ => DerefObjectStatic(n, f)
            }
            case DerefMetadataStatic(t, f) =>
              DerefMetadataStatic(normalize[A](t, undef), f)
            case DerefObjectDynamic(s, f) =>
              normalize(s, undef) match {
                case `undef` => undef
                case sn => normalize(f, undef) match {
                  case `undef` => undef
                  case fn => DerefObjectDynamic(sn, fn)
                }
              }
            case DerefArrayDynamic(s, f) =>
              normalize(s, undef) match {
                case `undef` => undef
                case sn => normalize(f, undef) match {
                  case `undef` => undef
                  case fn => DerefArrayDynamic(sn, fn)
                }
              }
            case IsType(s, t)            => IsType(normalize(s, undef), t)
            case Equal(f, s)             => Equal(normalize(f, undef), normalize(s, undef))
            case EqualLiteral(f, v, i)   => EqualLiteral(normalize(f, undef), v, i)
            case Within(item, in)        => Within(normalize(item, undef), normalize(in, undef))
            case Range(upper, lower)     => Range(normalize(upper, undef), normalize(lower, undef))
            case Cond(p, l, r)           => Cond(normalize(p, undef), normalize(l, undef), normalize(r, undef))
            case Filter(s, t)            => Filter(normalize(s, undef), normalize(t, undef))
            case FilterDefined(s, df, t) => FilterDefined(normalize(s, undef), normalize(df, undef), t)
            case IfUndefined(s, t)       => IfUndefined(normalize(s, undef), normalize(t, undef))
            case Typed(s, t)             => Typed(normalize(s, undef), t)
            case TypedSubsumes(s, t)     => TypedSubsumes(normalize(s, undef), t)
            case Map1(s, fun)            => Map1(normalize(s, undef), fun)
            case DeepMap1(s, fun)        => DeepMap1(normalize(s, undef), fun)
            case Map2(s, f, fun)         => Map2(normalize(s, undef), normalize(f, undef), fun)
            case MapN(s, fun)            => MapN(normalize(s, undef), fun)
            case ArraySwap(s, i)         => ArraySwap(normalize(s, undef), i)
            case ToNumber(s)             => ToNumber(normalize(s, undef))
            case _                       => ts
          }
        }
      }

      // rephrase(p, r)(p(a)) --> r(a), if this is possible.
      // rephrase(p, r) "pulls back" r from p.

      // selectors:
      // rephrase(.a.b, .a.b.c)(x) --> x.c
      // ==> rephrase(.a.b, .a.b.c)(x.a.b) --> x.a.b.c

      // rephrase(.[0], .[0].[1])(x) --> [[undef] ++ [x]].[0].[1]
      // ==> rephrase(.[0], .[0].[1])(x.[0]) --> x.[0].[1]

      // rephrase({k: f}, f)(x) --> x.k
      // ==> rephrase({k: f}, f)({k: f(x))) --> f(x)

      // arrays:
      // rephrase([a] ++ [b] ++ [c] ++ [d], a)(x) --> x.[0]
      // rephrase([a] ++ [b] ++ ([c] ++ [d], d)(x) --> x.[3]

      // constants:
      // rephrase(1, f) --> none
      // rephrase(f, 1) --> 1

      def rephrase[A <: SourceType](projection: TransSpec[A],
                                    rootSource: A,
                                    root: TransSpec1): Option[TransSpec1] = {
        import scalaz.syntax.std.option._, scalaz.std.option._
        // every iteration of peelInvert returns:
        // - a change in the current index inside a nested *ArrayConcat, or None if the index information has been lost
        //   (e.g., by concatting with Leaf(Source))
        // - a change in the set of keys known to be inside a nested *ObjectConcat, or None if the key information has been lost
        //   (e.g., by concatting with Leaf(Source))
        // - a map of substitutions, which maps from subtrees of `root` that have been found in `projection`
        //   to the accumulated inverse of the TransSpec layers seen inside `projection` upwards of that point
        final case class PeelState(delta: Option[Int], keys: Option[Set[String]], substitutions: Map[TransSpec1, TransSpec1])

        val rootWithSourceReplaced = TransSpec.mapSources(root)(_ => rootSource)

        // find all paths (subtrees) through a `TransSpec[A]`
        // note we don't need to include `r` in the set of paths when `r` is a `WrapArray`, `WrapObject`, or `*Concat`,
        // because actually substituting that subtree messes up the index handling while traversing `projection`.
        def paths(r: TransSpec[A]): Set[TransSpec[A]] = r match {
          case OuterArrayConcat(rs@_*)   => rs.flatMap(paths).toSet
          case InnerArrayConcat(rs@_*)   => rs.flatMap(paths).toSet
          case OuterObjectConcat(rs@_*)  => rs.flatMap(paths).toSet
          case InnerObjectConcat(rs@_*)  => rs.flatMap(paths).toSet
          case WrapArray(s)              => paths(s)
          case WrapObject(s, _)          => paths(s)
          case DerefObjectStatic(s, _)   => paths(s) + r
          case DerefObjectDynamic(f, s)  => paths(f) ++ paths(s) + r
          case DerefArrayStatic(s, _)    => paths(s) + r
          case DerefArrayDynamic(f, s)   => paths(f) ++ paths(s) + r
          case DerefMetadataStatic(s, _) => paths(s) + r
          case ArraySwap(s, _)           => paths(s)
          case Cond(f, s, _)             => paths(f) ++ paths(s) + r
          case ConstLiteral(_, s)        => paths(s) + r
          case Equal(f, s)               => paths(f) ++ paths(s) + r
          case EqualLiteral(s, _, _)     => paths(s) + r
          case Within(item, in)          => paths(item) ++ paths(in) + r
          case Range(upper, lower)       => paths(upper) ++ paths(lower) + r
          case Filter(f, p)              => paths(f) ++ paths(p) + r
          case FilterDefined(s, p, _)    => paths(s) ++ paths(p) + r
          case IfUndefined(s, d)         => paths(s) ++ paths(d) + r
          case IsType(s, _)              => paths(s) + r
          case Map1(s, _)                => paths(s) + r
          case Map2(f, s, _)             => paths(f) ++ paths(s) + r
          case MapN(s, _)                => paths(s) + r
          case DeepMap1(s, _)            => paths(s) + r
          case MapWith(s, _)             => paths(s) + r
          case ObjectDelete(s, _)        => paths(s) + r
          case Scan(s, _)                => paths(s) + r
          case Typed(s, _)               => paths(s) + r
          case TypedSubsumes(s, _)       => paths(s) + r
          case WrapObjectDynamic(f, s)   => paths(f) ++ paths(s) + r
          case ToNumber(s)               => paths(s) + r
          case Leaf(`rootSource`)        => Set(r)
          case Leaf(_)                   => sys.error("impossible")
        }

        // find all subtrees of `root`, so that they can be substituted with inverses of the
        // outer layers of `projection` from inside `root` when they're encountered inside `projection`.
        val allRootPaths = paths(rootWithSourceReplaced)

        // peels layers off of `projection`, building up the inverses of every layer and substituting those layers for common
        // occurrences of subtrees of `root` inside `projection` when they're reached.
        def peelInvert(projection: TransSpec[A], currentIndex: Int, keys: Set[String], inverseLayers: TransSpec1 => TransSpec1): PeelState = {

          // folds over every branch in a *ArrayConcat, from the *left*, collecting substitutions.
          // the state carried along consists of:
          // a) the index into the array formed by the *ArrayConcat (and outer *ArrayConcats, because it's passed into peelInvert)
          // b) the Option[TransSpec1] returned by the first successfully rephrased branch of the *ArrayConcat.
          // If the index returned by any rephrase call is none before a branch is successfully rephrased,
          // we have insufficient information to continue searching for a successful branch,
          // so we have to halt and return none. Otherwise, the first successfully rephrased branch is returned.
          def arrayConcat(rs: Seq[TransSpec[A]]) = {
            val (delta, substsOut) = rs.foldLeft((0.some, Map.empty[TransSpec1, TransSpec1])) {
              case ((Some(i), substs), ts) =>
                val PeelState(newDelta, _, newSubsts) = peelInvert(ts, currentIndex = currentIndex + i, keys = Set.empty, inverseLayers)
                (newDelta.map(_ + i), newSubsts ++ substs)
              case ((None, substs), _) => (none, substs)
            }
            PeelState(delta, Set.empty[String].some, substsOut)
          }

          // folds over every branch in a *ObjectConcat, from the *right*, collecting substitutions.
          // the state carried along consists of:
          // a) the set of keys which have been observed in the *ObjectConcat (and outer *ObjectConcats, because it's passed into peelInvert)
          // b) the Option[TransSpec1] returned by the first successfully rephrased branch of the *ObjectConcat.
          // if the set of keys returned by any rephrase call is none before a branch is successfully rephrased,
          // we have insufficient information to continue searching for a successful branch,
          // so we have to halt and return none. Otherwise, the first successfully rephrased branch is returned.
          def objectConcat(rs: Seq[TransSpec[A]]) = {
            val (resultKeys, out) = rs.foldRight((Set.empty[String].some, Map.empty[TransSpec1, TransSpec1])) {
              case (ts, (Some(ks), substs)) =>
                val PeelState(_, newKeys, newSubsts) = peelInvert(ts, currentIndex = 0, keys = keys ++ ks, inverseLayers)
                (newKeys.map(_ ++ ks), newSubsts ++ substs)
              case (_, (None, substs)) => (none, substs)
            }
            PeelState(0.some, resultKeys, out)
          }

          projection match {
            // if the key has already been spotted (to the right in a nested *ObjectConcat of this WrapObject),
            // the object resulting from the *ObjectConcat has had this WrapObject call's result overriden.
            // so this branch's value is inaccessible, so we return none.
            case WrapObject(s, k) =>
              if (keys(k)) {
                PeelState(none, Set.empty[String].some, Map.empty)
              } else {
                val PeelState(_, _, nestedSubstitutions) =
                  peelInvert(s, currentIndex = 0, Set.empty, inverseLayers andThen (DerefObjectStatic(_, CPathField(k))))
                PeelState(0.some, Set(k).some, nestedSubstitutions)
              }
            case ObjectDelete(s, k) =>
              val PeelState(_, nestedKeys, nestedSubstitutions) = peelInvert(s, currentIndex = 0, keys, inverseLayers)
              PeelState(0.some, nestedKeys.map(_ -- k.map(_.name)), nestedSubstitutions)
            // encountering a WrapArray inside a *ArrayConcat requires shifting the index by 1.
            // however inside the WrapArray, the index is reset to 0.
            case WrapArray(s) =>
              val PeelState(_, _, nestedSubstitutions) = peelInvert(s, currentIndex = 0, Set.empty, inverseLayers andThen (DerefArrayStatic(_, CPathIndex(currentIndex))))
              PeelState(1.some, none, nestedSubstitutions)
            case OuterArrayConcat(rs@_*)  => arrayConcat(rs)
            case InnerArrayConcat(rs@_*)  => arrayConcat(rs)
            case OuterObjectConcat(rs@_*) => objectConcat(rs)
            case InnerObjectConcat(rs@_*) => objectConcat(rs)
            case ArraySwap(s, i)          => peelInvert(s, currentIndex = 0, Set.empty, inverseLayers andThen (ArraySwap(_, i)))
            // this branch of `projection` is a subtree of `root`, so we can substitute it with the inverted layers of
            // `projection` we've encountered so far.
            case rootSubtree if allRootPaths(rootSubtree) =>
              val rootSubtreeAsTransSpec1 =
                if (rootSource == Source) {
                  rootSubtree.asInstanceOf[TransSpec1]
                } else {
                  TransSpec.mapSources(rootSubtree)(_ => Source)
                }
              PeelState(0.some, Set.empty[String].some, Map(rootSubtreeAsTransSpec1 -> inverseLayers(Leaf(Source))))
            // this won't be Leaf(rootSource), because that would be a subtree of root
            case Leaf(_) => PeelState(none, none, Map.empty)
            case _       => PeelState(none, none, Map.empty)
          }
        }
        val peelSubstitutions =
          peelInvert(projection, currentIndex = 0, keys = Set.empty, inverseLayers = identity[TransSpec1]).substitutions
        if (peelSubstitutions.isEmpty) {
          // there wasn't even an occurrence of `Leaf(rootSource)` in `projection`,
          // so `projection` has definitely destroyed the information necessary to get back to root
          none
        } else {
          // deepMap takes care of running substitutions over the largest subtrees first
          val substitutedRoot = TransSpec.deepMap(root)(peelSubstitutions)
          // normalize the output, so that equivalent sort orders are more likely to be comparable
          normalize(substitutedRoot, TransSpec1.Undef).some
        }
      }
    }

    object TransSpec1 {
      import constants._

      val Id: TransSpec1 = Leaf(Source)

      // fakes an undefinedness literal by derefing an empty object
      val Undef: TransSpec1 =
        DerefObjectStatic(ConstLiteral(CEmptyObject, Id), CPathField("bogus"))

      val DerefArray0 = DerefArrayStatic(Leaf(Source), CPathIndex(0))
      val DerefArray1 = DerefArrayStatic(Leaf(Source), CPathIndex(1))
      val DerefArray2 = DerefArrayStatic(Leaf(Source), CPathIndex(2))

      val PruneToKeyValue = InnerObjectConcat(WrapObject(SourceKey.Single, paths.Key.name), WrapObject(SourceValue.Single, paths.Value.name))

      val DeleteKeyValue = ObjectDelete(Leaf(Source), Set(paths.Key, paths.Value))
    }

    type TransSpec2 = TransSpec[Source2]

    object TransSpec2 {
      val LeftId: TransSpec2 = Leaf(SourceLeft)
      val RightId: TransSpec2 = Leaf(SourceRight)

      // fakes an undefinedness literal by derefing an empty object
      val Undef: TransSpec2 =
        DerefObjectStatic(ConstLiteral(CEmptyObject, LeftId), CPathField("bogus"))

      /** Flips all `SourceLeft`s to `SourceRight`s and vice versa. */
      def flip(spec: TransSpec2): TransSpec2 = TransSpec.mapSources(spec) {
        case SourceLeft  => SourceRight
        case SourceRight => SourceLeft
      }

      def DerefArray0(source: Source2) = DerefArrayStatic(Leaf(source), CPathIndex(0))
      def DerefArray1(source: Source2) = DerefArrayStatic(Leaf(source), CPathIndex(1))
      def DerefArray2(source: Source2) = DerefArrayStatic(Leaf(source), CPathIndex(2))

      val DeleteKeyValueLeft  = ObjectDelete(Leaf(SourceLeft), Set(paths.Key, paths.Value))
      val DeleteKeyValueRight = ObjectDelete(Leaf(SourceRight), Set(paths.Key, paths.Value))
    }

    sealed trait GroupKeySpec

    /**
      * Definition for a single (non-composite) key part.
      *
      * @param key The key which will be used by `merge` to access this particular tic-variable (which may be refined by more than one `GroupKeySpecSource`)
      * @param spec A transform which defines this key part as a function of the source table in `GroupingSource`.
      */
    case class GroupKeySpecSource(key: CPathField, spec: TransSpec1) extends GroupKeySpec

    case class GroupKeySpecAnd(left: GroupKeySpec, right: GroupKeySpec) extends GroupKeySpec
    case class GroupKeySpecOr(left: GroupKeySpec, right: GroupKeySpec)  extends GroupKeySpec

    object GroupKeySpec {
      def dnf(keySpec: GroupKeySpec): GroupKeySpec = {
        keySpec match {
          case GroupKeySpecSource(key, spec)                  => GroupKeySpecSource(key, spec)
          case GroupKeySpecAnd(GroupKeySpecOr(ol, or), right) => GroupKeySpecOr(dnf(GroupKeySpecAnd(ol, right)), dnf(GroupKeySpecAnd(or, right)))
          case GroupKeySpecAnd(left, GroupKeySpecOr(ol, or))  => GroupKeySpecOr(dnf(GroupKeySpecAnd(left, ol)), dnf(GroupKeySpecAnd(left, or)))

          case gand @ GroupKeySpecAnd(left, right) =>
            val leftdnf  = dnf(left)
            val rightdnf = dnf(right)
            if (leftdnf == left && rightdnf == right) gand else dnf(GroupKeySpecAnd(leftdnf, rightdnf))

          case gor @ GroupKeySpecOr(left, right) =>
            val leftdnf  = dnf(left)
            val rightdnf = dnf(right)
            if (leftdnf == left && rightdnf == right) gor else dnf(GroupKeySpecOr(leftdnf, rightdnf))
        }
      }

      def toVector(keySpec: GroupKeySpec): Vector[GroupKeySpec] = {
        keySpec match {
          case GroupKeySpecOr(left, right) => toVector(left) ++ toVector(right)
          case x                           => Vector(x)
        }
      }
    }

    object constants {
      import paths._

      object SourceKey {
        val Single = DerefObjectStatic(Leaf(Source), Key)

        val Left  = DerefObjectStatic(Leaf(SourceLeft), Key)
        val Right = DerefObjectStatic(Leaf(SourceRight), Key)
      }

      object SourceValue {
        val Single = DerefObjectStatic(Leaf(Source), Value)

        val Left  = DerefObjectStatic(Leaf(SourceLeft), Value)
        val Right = DerefObjectStatic(Leaf(SourceRight), Value)
      }
    }

    def transRValue[A <: SourceType](rvalue: RValue, target: TransSpec[A]): TransSpec[A] = {
      RValue.toCValue(rvalue) map { cvalue =>
        trans.ConstLiteral(cvalue, target)
      } getOrElse {
        rvalue match {
          case RArray(elements) =>
            InnerArrayConcat(elements map { element =>
              trans.WrapArray(transRValue(element, target))
            }: _*)
          case RObject(fields) =>
            InnerObjectConcat(fields.toSeq map {
              case (key, value) => trans.WrapObject(transRValue(value, target), key)
            }: _*)
          case _ =>
            sys.error("Can't handle RValue")
        }
      }
    }

  }

  import trans._

  type TableTransSpec[+A <: SourceType] = Map[CPathField, TransSpec[A]]
  type TableTransSpec1                  = TableTransSpec[Source1]
  type TableTransSpec2                  = TableTransSpec[Source2]

  def makeTableTrans(tableTrans: TableTransSpec1): TransSpec1 = {
    val wrapped = for ((key @ CPathField(fieldName), value) <- tableTrans) yield {
      val mapped = TransSpec.deepMap(value) {
        case Leaf(_) => DerefObjectStatic(Leaf(Source), key)
      }

      trans.WrapObject(mapped, fieldName)
    }

    wrapped.foldLeft[TransSpec1](ObjectDelete(Leaf(Source), Set(tableTrans.keys.toSeq: _*))) { (acc, ts) =>
      trans.InnerObjectConcat(acc, ts)
    }
  }

  def liftToValues(trans: TransSpec1): TransSpec1 =
    makeTableTrans(Map(paths.Value -> trans))

  def buildConstantWrapSpec[A <: SourceType](source: TransSpec[A]): TransSpec[A] = {
    val bottomWrapped = trans.WrapObject(trans.ConstLiteral(CEmptyArray, source), paths.Key.name)
    trans.InnerObjectConcat(bottomWrapped, trans.WrapObject(source, paths.Value.name))
  }
}
