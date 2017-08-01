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

package quasar.mimir

import quasar.yggdrasil.bytecode._

import quasar.blueeyes.json.JNum

import quasar.precog.common._
import quasar.precog.util.{ IdGen, Identifier }
import quasar.yggdrasil._

import scala.collection.mutable

import scalaz._, Scalaz._
import scalaz.Free.Trampoline
import java.math.MathContext

trait DAG extends Instructions {
  type TS1

  import instructions._
  import library._
  import TableModule.CrossOrder // TODO: Move cross order out of yggdrasil.
  import dag.BucketSpec

//   type Either[+A, +B] = scala.util.Either[A, B]
//   val Either          = scala.util.Either
//   val Left            = scala.util.Left
//   val Right           = scala.util.Right

//   type StackErrorT[+A] = Either[StackError, A]
  type SpecOrGraph     = Either[BucketSpec, DepGraph]
  type Step            = List[SpecOrGraph] => Either[StackError, List[SpecOrGraph]]
  type DecorateResult  = Either[StackError, DepGraph]
  type LoopResult      = Trampoline[DecorateResult]

  def left[A](x: StackError): Either[StackError, A] = scala.util.Left(x)

  def decorate(stream: Vector[Instruction]): DecorateResult = {
    import dag._
    implicit val M = eitherMonad[StackError]

    def loop(loc: Line, roots: List[SpecOrGraph], splits: List[OpenSplit], stream: Vector[Instruction]): LoopResult = {
      @inline def continue(f: Step): LoopResult =
        Trampoline.suspend(M.sequence(f(roots) map { roots2 => loop(loc, roots2, splits, stream.tail) }) map (_.joinRight))

      def processJoinInstr(instr: JoinInstr): LoopResult = {
        val maybeOpSort = Some(instr) collect {
          case instructions.Map2Match(op) => (op, IdentitySort)
          case instructions.Map2Cross(op) => (op, Cross(None))
        }

        val eitherRootsOp = maybeOpSort map {
          case (op, joinSort) =>
            continue {
              case Right(right) :: Right(left) :: tl => Right(Right(Join(op, joinSort, left, right)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _  => left(OperationOnBucket(instr))
              case _                                 => left(StackUnderflow(instr))
            }
        }

        val eitherRootsAbom = Some(instr) collect {
          case instr @ instructions.Observe =>
            continue {
              case Right(samples) :: Right(data) :: tl => Right(Right(Observe(data, samples)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _    => left(OperationOnBucket(instr))
              case _                                   => left(StackUnderflow(instr))
            }

          case instr @ instructions.Assert =>
            continue {
              case Right(child) :: Right(pred) :: tl => Right(Right(Assert(pred, child)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _  => left(OperationOnBucket(instr))
              case _                                 => left(StackUnderflow(instr))
            }

          case instr @ (instructions.IIntersect | instructions.IUnion) =>
            continue {
              case Right(right) :: Right(left) :: tl => Right(Right(IUI(instr == instructions.IUnion, left, right)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _  => left(OperationOnBucket(instr))
              case _                                 => left(StackUnderflow(instr))
            }

          case instructions.SetDifference =>
            continue {
              case Right(right) :: Right(left) :: tl => Right(Right(Diff(left, right)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _  => left(OperationOnBucket(instr))
              case _                                 => left(StackUnderflow(instr))
            }
        }

        eitherRootsOp orElse eitherRootsAbom get // assertion
      }

      def processFilter(instr: Instruction, joinSort: JoinSort): LoopResult = {
        val (args, roots2) = roots splitAt 2

        if (args.lengthCompare(2) < 0) {
          Trampoline delay left(StackUnderflow(instr))
        } else {
          val rightArgs = args flatMap { _.right.toOption }

          if (rightArgs.lengthCompare(2) < 0) {
            Trampoline delay left(OperationOnBucket(instr))
          } else {
            val (boolean :: target :: predRoots) = rightArgs
            loop(loc, Right(Filter(joinSort, target, boolean)(loc)) :: roots2, splits, stream.tail)
          }
        }
      }

      val tail: Option[LoopResult] = stream.headOption map {
        case instr @ Map1(instructions.New) => {
          continue {
            case Right(hd) :: tl => Right(Right(New(hd)(loc)) :: tl)
            case Left(_) :: _    => left(OperationOnBucket(instr))
            case _               => left(StackUnderflow(instr))
          }
        }

        case instr @ Map1(op) => {
          continue {
            case Right(hd) :: tl => Right(Right(Operate(op, hd)(loc)) :: tl)
            case Left(_) :: _    => left(OperationOnBucket(instr))
            case _               => left(StackUnderflow(instr))
          }
        }

        case instr: JoinInstr => processJoinInstr(instr)

        case instr @ instructions.Morph1(BuiltInMorphism1(m1)) => {
          continue {
            case Right(hd) :: tl => Right(Right(Morph1(m1, hd)(loc)) :: tl)
            case Left(_) :: _    => left(OperationOnBucket(instr))
            case _               => left(StackUnderflow(instr))
          }
        }

        case instr @ instructions.Morph2(BuiltInMorphism2(m2)) => {
          continue {
            case Right(right) :: Right(left) :: tl => Right(Right(Morph2(m2, left, right)(loc)) :: tl)
            case Left(_) :: _                      => left(OperationOnBucket(instr))
            case _ :: Left(_) :: _                 => left(OperationOnBucket(instr))
            case _                                 => left(StackUnderflow(instr))
          }
        }

        case instr @ instructions.Reduce(BuiltInReduction(red)) => {
          continue {
            case Right(hd) :: tl => Right(Right(Reduce(red, hd)(loc)) :: tl)
            case Left(_) :: _    => left(OperationOnBucket(instr))
            case _               => left(StackUnderflow(instr))
          }
        }

        case instructions.Distinct => {
          continue {
            case Right(hd) :: tl => Right(Right(Distinct(hd)(loc)) :: tl)
            case Left(_) :: _    => left(OperationOnBucket(instructions.Distinct))
            case _               => left(StackUnderflow(instructions.Distinct))
          }
        }

        case instr @ instructions.Group(id) => {
          continue {
            case Right(target) :: Left(child) :: tl => Right(Left(Group(id, target, child)) :: tl)
            case Left(_) :: _                       => left(OperationOnBucket(instr))
            case Right(_) :: Right(_) :: _          => left(BucketOperationOnSets(instr))
            case _                                  => left(StackUnderflow(instr))
          }
        }

        case instr @ MergeBuckets(and) => {
          val const = if (and) IntersectBucketSpec else UnionBucketSpec

          continue {
            case Left(right) :: Left(left) :: tl => Right(Left(const(left, right)) :: tl)
            case Right(_) :: _ :: _              => left(BucketOperationOnSets(instr))
            case _ :: Right(_) :: _              => left(BucketOperationOnSets(instr))
            case _                               => left(StackUnderflow(instr))
          }
        }

        case instr @ KeyPart(id) => {
          continue {
            case Right(parent) :: tl => Right(Left(UnfixedSolution(id, parent)) :: tl)
            case Left(_) :: _        => left(OperationOnBucket(instr))
            case _                   => left(StackUnderflow(instr))
          }
        }

        case instructions.Extra => {
          continue {
            case Right(parent) :: tl => Right(Left(Extra(parent)) :: tl)
            case Left(_) :: _        => left(OperationOnBucket(instructions.Extra))
            case _                   => left(StackUnderflow(instructions.Extra))
          }
        }

        case instructions.Split => {
          roots match {
            case Left(spec) :: tl => loop(loc, tl, OpenSplit(loc, spec, tl, new Identifier) :: splits, stream.tail)
            case Right(_) :: _    => Trampoline delay left(OperationOnBucket(instructions.Split))
            case _                => Trampoline delay left(StackUnderflow(instructions.Split))
          }
        }

        case Merge => {
          def isOk(oldTail: Seq[SpecOrGraph], newTail: Seq[SpecOrGraph]): Boolean = newTail forall (oldTail contains _)

          val eitherRoots = (splits, roots) match {
            case (Nil, _)                                                                   => left(UnmatchedMerge)
            case (_, Nil)                                                                   => left(StackUnderflow(Merge))
            case (OpenSplit(_, _, old, _) :: stail, Right(_) :: rtail) if !isOk(old, rtail) => left(MergeWithUnmatchedTails)
            case (OpenSplit(loc, spec, _, id) :: stail, Right(child) :: rtail)              => Right(Right(Split(spec, child, id)(loc)) :: rtail)
          }

          M.sequence(eitherRoots.right map (r2 => loop(loc, r2, splits drop 1, stream.tail))) map (_.joinRight)
        }

        case instr @ FilterMatch => processFilter(instr, IdentitySort)
        case instr @ FilterCross => processFilter(instr, Cross(None))

        case Dup => {
          roots match {
            case hd :: tl => loop(loc, hd :: hd :: tl, splits, stream.tail)
            case _        => Trampoline delay left(StackUnderflow(Dup))
          }
        }

        case instr @ Swap(depth) => {
          if (depth > 0) {
            if (roots.lengthCompare(depth + 1) < 0) {
              Trampoline delay left(StackUnderflow(instr))
            } else {
              val (span, rest)         = roots splitAt (depth + 1)
              val (spanInit, spanTail) = span splitAt depth
              val roots2               = spanTail ::: spanInit.tail ::: (span.head :: rest)
              loop(loc, roots2, splits, stream.tail)
            }
          } else {
            Trampoline delay left(NonPositiveSwapDepth(instr))
          }
        }

        case Drop => {
          roots match {
            case hd :: tl => loop(loc, tl, splits, stream.tail)
            case _        => Trampoline delay left(StackUnderflow(Drop))
          }
        }

        case loc: Line => loop(loc, roots, splits, stream.tail)

        case instr @ instructions.AbsoluteLoad => {
          continue {
            case Right(hd) :: tl => Right(Right(AbsoluteLoad(hd)(loc)) :: tl)
            case Left(_) :: _    => left(OperationOnBucket(instr))
            case _               => left(StackUnderflow(instr))
          }
        }

        case instr @ instructions.RelativeLoad => {
          continue {
            case Right(hd) :: tl => Right(Right(RelativeLoad(hd)(loc)) :: tl)
            case Left(_) :: _    => left(OperationOnBucket(instr))
            case _               => left(StackUnderflow(instr))
          }
        }

        case PushUndefined => {
          loop(loc, Right(Undefined(loc)) :: roots, splits, stream.tail)
        }

        case PushKey(id) => {
          val openPoss = splits find { open =>
            findGraphWithId(id)(open.spec).isDefined
          }
          openPoss map { open =>
            loop(loc, Right(SplitParam(id, open.id)(loc)) :: roots, splits, stream.tail)
          } getOrElse (Trampoline delay left(UnableToLocateSplitDescribingId(id)))
        }

        case PushGroup(id) => {
          val openPoss = splits find { open =>
            findGraphWithId(id)(open.spec).isDefined
          }
          openPoss map { open =>
            val graph = findGraphWithId(id)(open.spec).get
            loop(loc, Right(SplitGroup(id, graph.identities, open.id)(loc)) :: roots, splits, stream.tail)
          } getOrElse (Trampoline delay Left(UnableToLocateSplitDescribingId(id)))
        }

        case instr @ RootInstr() => {
          val rvalue = instr match {
            case PushString(str) => CString(str)
            case PushNum(num)    => CType.toCValue(JNum(BigDecimal(num, MathContext.UNLIMITED)))
            case PushTrue        => CBoolean(true)
            case PushFalse       => CBoolean(false)
            case PushNull        => CNull
            case PushObject      => RObject.empty
            case PushArray       => RArray.empty
            case _               => sys.error("Not a root instr")
          }
          loop(loc, Right(Const(rvalue)(loc)) :: roots, splits, stream.tail)
        }
      }
      tail getOrElse {
        Trampoline delay {
          if (!splits.isEmpty) {
            Left(UnmatchedSplit)
          } else {
            roots match {
              case Right(hd) :: Nil => Right(hd)
              case Left(_) :: Nil   => left(BucketAtEnd)
              case _ :: _ :: _      => left(MultipleStackValuesAtEnd)
              case Nil              => left(EmptyStackAtEnd)
            }
          }
        }
      }
    }

    def findFirstRoot(line: Option[Line], stream: Vector[Instruction]): Either[StackError, (Root, Vector[Instruction])] = {
      def buildConstRoot(instr: Instruction): Either[StackError, (Root, Vector[Instruction])] = {
        val rvalue = instr match {
          case PushString(str) => CString(str)
          case PushNum(num)    => CType.toCValue(JNum(BigDecimal(num, MathContext.UNLIMITED)))
          case PushTrue        => CBoolean(true)
          case PushFalse       => CBoolean(false)
          case PushNull        => CNull
          case PushObject      => RObject.empty
          case PushArray       => RArray.empty
          case _               => sys.error("impossible")
        }

        line map { ln =>
          Right((Const(rvalue)(ln), stream.tail))
        } getOrElse Left(UnknownLine)
      }

      val back = stream.headOption collect {
        case ln: Line => findFirstRoot(Some(ln), stream.tail)

        case i: PushString => buildConstRoot(i)
        case i: PushNum    => buildConstRoot(i)
        case PushTrue      => buildConstRoot(PushTrue)
        case PushFalse     => buildConstRoot(PushFalse)
        case PushNull      => buildConstRoot(PushNull)
        case PushObject    => buildConstRoot(PushObject)
        case PushArray     => buildConstRoot(PushArray)

        case PushUndefined =>
          line map { ln =>
            Right((Undefined(ln), stream.tail))
          } getOrElse Left(UnknownLine)

        case instr => Left(StackUnderflow(instr))
      }

      back getOrElse Left(EmptyStream)
    }

    if (stream.isEmpty) {
      Left(EmptyStream)
    } else {
      M.sequence(findFirstRoot(None, stream).right map { case (root, tail) => loop(root.loc, Right(root) :: Nil, Nil, tail) }).map(_.joinRight).run
    }
  }

  private def findGraphWithId(id: Int)(spec: BucketSpec): Option[DepGraph] = spec match {
    case dag.UnionBucketSpec(left, right)     => findGraphWithId(id)(left) orElse findGraphWithId(id)(right)
    case dag.IntersectBucketSpec(left, right) => findGraphWithId(id)(left) orElse findGraphWithId(id)(right)
    case dag.Group(`id`, target, _)           => Some(target)
    case dag.Group(_, _, child)               => findGraphWithId(id)(child)
    case dag.UnfixedSolution(`id`, target)    => Some(target)
    case dag.UnfixedSolution(_, _)            => None
    case dag.Extra(_)                         => None
  }

  private case class OpenSplit(loc: Line, spec: BucketSpec, oldTail: List[SpecOrGraph], id: Identifier)

  sealed trait Identities {
    def ++(other: Identities): Identities = (this, other) match {
      case (Identities.Undefined, _)                  => Identities.Undefined
      case (_, Identities.Undefined)                  => Identities.Undefined
      case (Identities.Specs(a), Identities.Specs(b)) => Identities.Specs(a ++ b)
    }

    def length: Int
    def distinct: Identities
    def fold[A](identities: Vector[dag.IdentitySpec] => A, undefined: A): A
  }

  object Identities {
    object Specs extends (Vector[dag.IdentitySpec] => Identities.Specs) {
      def empty = Specs(Vector.empty)
    }
    case class Specs(specs: Vector[dag.IdentitySpec]) extends Identities {
      override def length                                                   = specs.length
      override def distinct                                                 = Specs(specs map { _.canonicalize } distinct)
      override def fold[A](identities: Vector[dag.IdentitySpec] => A, b: A) = identities(specs)
    }
    case object Undefined extends Identities {
      override def length                                                           = 0
      override def distinct                                                         = Identities.Undefined
      override def fold[A](identities: Vector[dag.IdentitySpec] => A, undefined: A) = undefined
    }
  }

  sealed trait DepGraph {
    import dag._

    val loc: Line

    def identities: Identities

    /** Returns true if the identities are guaranteed to be unique. */
    def uniqueIdentities: Boolean

    /** The set of available value-sorted keys. */
    def valueKeys: Set[Int]

    def isSingleton: Boolean //true implies that the node is a singleton; false doesn't imply anything

    def containsSplitArg: Boolean

    def mapDown(body: (DepGraph => DepGraph) => PartialFunction[DepGraph, DepGraph]): DepGraph = {
      val memotable = mutable.Map[DepGraphWrapper, DepGraph]()

      def memoized(node: DepGraph): DepGraph = {
        lazy val pf: PartialFunction[DepGraph, DepGraph] = body(memoized)

        def inner(graph: DepGraph): DepGraph = graph match {
          case x if pf isDefinedAt x => pf(x)

          // not using extractors due to bug
          case s: dag.SplitParam =>
            dag.SplitParam(s.id, s.parentId)(s.loc)

          // not using extractors due to bug
          case s: dag.SplitGroup =>
            dag.SplitGroup(s.id, s.identities, s.parentId)(s.loc)

          case dag.Const(_) => graph

          case dag.Undefined() => graph

          case graph @ dag.New(parent) => dag.New(memoized(parent))(graph.loc)

          case graph @ dag.Morph1(m, parent) => dag.Morph1(m, memoized(parent))(graph.loc)

          case graph @ dag.Morph2(m, left, right) => dag.Morph2(m, memoized(left), memoized(right))(graph.loc)

          case graph @ dag.Distinct(parent) => dag.Distinct(memoized(parent))(graph.loc)

          case graph @ dag.AbsoluteLoad(parent, jtpe) => dag.AbsoluteLoad(memoized(parent), jtpe)(graph.loc)

          case graph @ dag.RelativeLoad(parent, jtpe) => dag.RelativeLoad(memoized(parent), jtpe)(graph.loc)

          case graph @ dag.Operate(op, parent) => dag.Operate(op, memoized(parent))(graph.loc)

          case graph @ dag.Reduce(red, parent) => dag.Reduce(red, memoized(parent))(graph.loc)

          case dag.MegaReduce(reds, parent) => dag.MegaReduce(reds, memoized(parent))

          case s @ dag.Split(spec, child, id) => {
            val spec2  = memoizedSpec(spec)
            val child2 = memoized(child)
            dag.Split(spec2, child2, id)(s.loc)
          }

          case graph @ dag.Assert(pred, child) => dag.Assert(memoized(pred), memoized(child))(graph.loc)

          case graph @ dag.Cond(pred, left, leftJoin, right, rightJoin) =>
            dag.Cond(memoized(pred), memoized(left), leftJoin, memoized(right), rightJoin)(graph.loc)

          case graph @ dag.Observe(data, samples) => dag.Observe(memoized(data), memoized(samples))(graph.loc)

          case graph @ dag.IUI(union, left, right) => dag.IUI(union, memoized(left), memoized(right))(graph.loc)

          case graph @ dag.Diff(left, right) => dag.Diff(memoized(left), memoized(right))(graph.loc)

          case graph @ dag.Join(op, joinSort, left, right) => dag.Join(op, joinSort, memoized(left), memoized(right))(graph.loc)

          case graph @ dag.Filter(joinSort, target, boolean) => dag.Filter(joinSort, memoized(target), memoized(boolean))(graph.loc)

          case dag.AddSortKey(parent, sortField, valueField, id) => dag.AddSortKey(memoized(parent), sortField, valueField, id)

          case dag.Memoize(parent, priority) => dag.Memoize(memoized(parent), priority)
        }

        def memoizedSpec(spec: BucketSpec): BucketSpec = spec match { //TODO generalize?
          case dag.UnionBucketSpec(left, right) =>
            dag.UnionBucketSpec(memoizedSpec(left), memoizedSpec(right))

          case dag.IntersectBucketSpec(left, right) =>
            dag.IntersectBucketSpec(memoizedSpec(left), memoizedSpec(right))

          case dag.Group(id, target, child) =>
            dag.Group(id, memoized(target), memoizedSpec(child))

          case dag.UnfixedSolution(id, target) =>
            dag.UnfixedSolution(id, memoized(target))

          case dag.Extra(target) =>
            dag.Extra(memoized(target))
        }

        memotable.get(new DepGraphWrapper(node)) getOrElse {
          val result = inner(node)
          memotable += (new DepGraphWrapper(node) -> result)
          result
        }
      }

      memoized(this)
    }

    trait ScopeUpdate[S] {
      def update(node: DepGraph): Option[S]       = None
      def update(spec: BucketSpec): Option[S] = None
    }
    object ScopeUpdate {
      def scopeUpdate[S: ScopeUpdate] = implicitly[ScopeUpdate[S]]

      implicit def depGraphScopeUpdate = new ScopeUpdate[DepGraph] {
        override def update(node: DepGraph) = Some(node)
      }
      implicit def bucketSpecScopeUpdate = new ScopeUpdate[BucketSpec] {
        override def update(spec: BucketSpec) = Some(spec)
      }
    }

    trait EditUpdate[E] {
      def edit[T](inScope: Boolean, from: DepGraph, edit: (E, E), replace: DepGraph => T, retain: DepGraph => T): T
      def edit[T](inScope: Boolean, from: BucketSpec, edit: (E, E), replace: BucketSpec => T, retain: BucketSpec => T): T
      def bimap[T](e: E)(fg: DepGraph => T, fs: BucketSpec => T): T
    }
    object EditUpdate {
      def editUpdate[E: EditUpdate]: EditUpdate[E] = implicitly[EditUpdate[E]]

      implicit def depGraphEditUpdate: EditUpdate[DepGraph] = new EditUpdate[DepGraph] {
        def edit[T](inScope: Boolean, from: DepGraph, edit: (DepGraph, DepGraph), replace: DepGraph => T, retain: DepGraph => T): T =
          if (inScope && from == edit._1) replace(edit._2) else retain(from)
        def edit[T](inScope: Boolean, from: BucketSpec, edit: (DepGraph, DepGraph), replace: BucketSpec => T, retain: BucketSpec => T): T =
          retain(from)

        def bimap[T](e: DepGraph)(fg: DepGraph => T, fs: BucketSpec => T): T = fg(e)
      }
      implicit def bucketSpecEditUpdate: EditUpdate[BucketSpec] = new EditUpdate[BucketSpec] {
        def edit[T](inScope: Boolean, from: DepGraph, edit: (BucketSpec, BucketSpec), replace: DepGraph => T, retain: DepGraph => T): T = retain(from)
        def edit[T](inScope: Boolean,
                    from: BucketSpec,
                    edit: (BucketSpec, BucketSpec),
                    replace: BucketSpec => T,
                    retain: BucketSpec => T): T =
          if (inScope && from == edit._1) replace(edit._2) else retain(from)

        def bimap[T](e: BucketSpec)(fg: DepGraph => T, fs: BucketSpec => T): T = fs(e)
      }
    }

    /**
      * Performs a scoped node substitution on this DepGraph.
      *
      * The substitution is represented as a pair from -> to both of type E. The replacement is performed everywhere in
      * the DAG below the scope node of type S. Both S and E are constained to be one of DepGraph or BucketSpec by
      * the availability of instances of the type classes EditUpdate[E] and ScopeUpdate[S] as defined above.
      *
      * @return A pair of the whole rewritten DAG and the rewritten scope node.
      */
    def substituteDown[S: ScopeUpdate, E: EditUpdate](scope: S, edit: (E, E)): (DepGraph, S) = {
      import ScopeUpdate._
      import EditUpdate._

      case class SubstitutionState(inScope: Boolean, rewrittenScope: Option[S])

      val monadState = StateT.stateMonad[SubstitutionState]
      val init       = SubstitutionState(this == scope, None)

      type BucketSpecState = State[SubstitutionState, BucketSpec]
      type DepGraphState   = State[SubstitutionState, DepGraph]

      val memotable = mutable.Map[DepGraph, DepGraphState]()

      def memoized(node: DepGraph): DepGraphState = {

        def inner(graph: DepGraph): DepGraphState = {

          val inScopeM = for {
            state <- monadState.gets(identity)
            inScope = state.inScope || graph == scope
            _ <- monadState.modify(_.copy(inScope = inScope))
          } yield inScope

          def fn1(rep: DepGraph): DepGraphState = monadState gets identity map (_ => rep)
          def fn2(rep: DepGraph): DepGraphState = rep match {
            // not using extractors due to bug
            case s: dag.SplitParam =>
              for { state <- monadState.gets(identity) } yield dag.SplitParam(s.id, s.parentId)(s.loc)

            // not using extractors due to bug
            case s: dag.SplitGroup =>
              for { state <- monadState.gets(identity) } yield dag.SplitGroup(s.id, s.identities, s.parentId)(s.loc)

            case graph @ dag.Const(_) =>
              for { _ <- monadState.gets(identity) } yield graph

            case graph @ dag.Undefined() =>
              for { _ <- monadState.gets(identity) } yield graph

            case graph @ dag.New(parent) =>
              for { newParent <- memoized(parent) } yield dag.New(newParent)(graph.loc)

            case graph @ dag.Morph1(m, parent) =>
              for { newParent <- memoized(parent) } yield dag.Morph1(m, newParent)(graph.loc)

            case graph @ dag.Morph2(m, left, right) =>
              for {
                newLeft <- memoized(left)
                newRight <- memoized(right)
              } yield dag.Morph2(m, newLeft, newRight)(graph.loc)

            case graph @ dag.Distinct(parent) =>
              for { newParent <- memoized(parent) } yield dag.Distinct(newParent)(graph.loc)

            case graph @ dag.AbsoluteLoad(parent, jtpe) =>
              for { newParent <- memoized(parent) } yield dag.AbsoluteLoad(newParent, jtpe)(graph.loc)

            case graph @ dag.Operate(op, parent) =>
              for { newParent <- memoized(parent) } yield dag.Operate(op, newParent)(graph.loc)

            case graph @ dag.Reduce(red, parent) =>
              for { newParent <- memoized(parent) } yield dag.Reduce(red, newParent)(graph.loc)

            case dag.MegaReduce(reds, parent) =>
              for { newParent <- memoized(parent) } yield dag.MegaReduce(reds, newParent)

            case s @ dag.Split(spec, child, id) => {
              for {
                newSpec <- memoizedSpec(spec)
                newChild <- memoized(child)
              } yield dag.Split(newSpec, newChild, id)(s.loc)
            }

            case graph @ dag.Assert(pred, child) =>
              for {
                newPred <- memoized(pred)
                newChild <- memoized(child)
              } yield dag.Assert(newPred, newChild)(graph.loc)

            case graph @ dag.Observe(data, samples) =>
              for {
                newData <- memoized(data)
                newSamples <- memoized(samples)
              } yield dag.Observe(newData, newSamples)(graph.loc)

            case graph @ dag.IUI(union, left, right) =>
              for {
                newLeft <- memoized(left)
                newRight <- memoized(right)
              } yield dag.IUI(union, newLeft, newRight)(graph.loc)

            case graph @ dag.Diff(left, right) =>
              for {
                newLeft <- memoized(left)
                newRight <- memoized(right)
              } yield dag.Diff(newLeft, newRight)(graph.loc)

            case graph @ dag.Join(op, joinSort, left, right) =>
              for {
                newLeft <- memoized(left)
                newRight <- memoized(right)
              } yield dag.Join(op, joinSort, newLeft, newRight)(graph.loc)

            case graph @ dag.Filter(joinSort, target, boolean) =>
              for {
                newTarget <- memoized(target)
                newBoolean <- memoized(boolean)
              } yield dag.Filter(joinSort, newTarget, newBoolean)(graph.loc)

            case dag.AddSortKey(parent, sortField, valueField, id) =>
              for { newParent <- memoized(parent) } yield dag.AddSortKey(newParent, sortField, valueField, id)

            case dag.Memoize(parent, priority) =>
              for { newParent <- memoized(parent) } yield dag.Memoize(newParent, priority)
          }

          val rewritten: DepGraphState = inScopeM flatMap (inScope => editUpdate[E].edit(inScope, graph, edit, fn1 _, fn2 _))

          if (graph != scope)
            rewritten
          else
            for {
              node <- rewritten
              _ <- monadState.modify(_.copy(rewrittenScope = scopeUpdate[S].update(node)))
            } yield node
        }

        memotable.get(node) getOrElse {
          val result = inner(node)
          memotable += (node -> result)
          result
        }
      }

      def memoizedSpec(spec: BucketSpec): BucketSpecState = {
        val inScopeM = for {
          state <- monadState.gets(identity)
          inScope = state.inScope || spec == scope
          _ <- monadState.modify(_.copy(inScope = inScope))
        } yield inScope

        val rewritten: BucketSpecState = inScopeM flatMap { inScope =>
          def fn1(rep: BucketSpec): BucketSpecState = monadState gets identity map (_ => rep)
          def fn2(rep: BucketSpec): BucketSpecState = rep match {
            case UnionBucketSpec(left, right)     => memoizedSpec(left) flatMap (nl => memoizedSpec(right) map (nr => UnionBucketSpec(nl, nr)))
            case IntersectBucketSpec(left, right) => memoizedSpec(left) flatMap (nl => memoizedSpec(right) map (nr => IntersectBucketSpec(nl, nr)))
            case Group(id, target, child)         => memoized(target) flatMap (t => memoizedSpec(child) map (c => Group(id, t, c)))
            case UnfixedSolution(id, target)      => memoized(target) map (UnfixedSolution(id, _))
            case Extra(target)                    => memoized(target) map (Extra(_))
          }
          editUpdate[E].edit(inScope, spec, edit, fn1 _, fn2 _)
        }

        if (spec != scope)
          rewritten
        else
          for (n <- rewritten; _ <- monadState.modify(_.copy(rewrittenScope = scopeUpdate[S].update(n)))) yield n
      }

      val resultM = for {
        preMemo <- editUpdate[E].bimap(edit._2)(memoized _, memoizedSpec _)
        result <- memoized(this)
      } yield result

      val (state, graph) = resultM(init)
      (graph, state.rewrittenScope.getOrElse(scope))
    }

    def foldDown[Z](enterSplitChild: Boolean)(f0: PartialFunction[DepGraph, Z])(implicit monoid: Monoid[Z]): Z = {
      val f: PartialFunction[DepGraph, Z] = f0.orElse { case _ => monoid.zero }

      def foldThroughSpec(spec: BucketSpec, acc: Z): Z = spec match {
        case dag.UnionBucketSpec(left, right) =>
          foldThroughSpec(right, foldThroughSpec(left, acc))

        case dag.IntersectBucketSpec(left, right) =>
          foldThroughSpec(right, foldThroughSpec(left, acc))

        case dag.Group(_, target, forest) =>
          foldThroughSpec(forest, foldDown0(target, acc |+| f(target)))

        case dag.UnfixedSolution(_, solution) => foldDown0(solution, acc |+| f(solution))
        case dag.Extra(expr)                  => foldDown0(expr, acc |+| f(expr))
      }

      def foldDown0(node: DepGraph, acc: Z): Z = node match {
        case dag.SplitParam(_, _) => acc

        case dag.SplitGroup(_, identities, _) => acc

        case node @ dag.Const(_) => acc

        case dag.Undefined() => acc

        case dag.New(parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Morph1(_, parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Morph2(_, left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Distinct(parent) => foldDown0(parent, acc |+| f(parent))

        case dag.AbsoluteLoad(parent, _) => foldDown0(parent, acc |+| f(parent))

        case dag.RelativeLoad(parent, _) => foldDown0(parent, acc |+| f(parent))

        case dag.Operate(_, parent) => foldDown0(parent, acc |+| f(parent))

        case node @ dag.Reduce(_, parent) => foldDown0(parent, acc |+| f(parent))

        case node @ dag.MegaReduce(_, parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Split(specs, child, _) =>
          val specsAcc = foldThroughSpec(specs, acc)
          if (enterSplitChild)
            foldDown0(child, specsAcc |+| f(child))
          else
            specsAcc

        case dag.Assert(pred, child) =>
          val acc2 = foldDown0(pred, acc |+| f(pred))
          foldDown0(child, acc2 |+| f(child))

        case dag.Cond(pred, left, _, right, _) =>
          val acc2 = foldDown0(pred, acc |+| f(pred))
          val acc3 = foldDown0(left, acc2 |+| f(left))
          foldDown0(right, acc3 |+| f(right))

        case dag.Observe(data, samples) =>
          val acc2 = foldDown0(data, acc |+| f(data))
          foldDown0(samples, acc2 |+| f(samples))

        case dag.IUI(_, left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Diff(left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Join(_, _, left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Filter(_, target, boolean) =>
          val acc2 = foldDown0(target, acc |+| f(target))
          foldDown0(boolean, acc2 |+| f(boolean))

        case dag.AddSortKey(parent, _, _, _) => foldDown0(parent, acc |+| f(parent))

        case dag.Memoize(parent, _) => foldDown0(parent, acc |+| f(parent))
      }

      foldDown0(this, f(this))
    }

    final def size: Int = {
      import scalaz.std.anyVal._
      val seen = mutable.Set[DepGraph]()

      foldDown(true) {
        case n if !seen(n) =>
          seen += n
          1
      }
    }
  }

  class DepGraphWrapper(val graph: DepGraph) {
    override def equals(that: Any) = that match {
      case (that: DepGraphWrapper) => this.graph eq that.graph
      case _                       => false
    }

    override def hashCode: Int = System identityHashCode graph
  }

  object dag {
    sealed trait StagingPoint extends DepGraph
    sealed trait Root         extends DepGraph

    object ConstString {
      def unapply(graph: Const): Option[String] = graph match {
        case Const(CString(str)) => Some(str)
        case _                   => None
      }
    }

    object ConstDecimal {
      def unapply(graph: Const): Option[BigDecimal] = graph match {
        case Const(CNum(d))    => Some(d)
        case Const(CLong(d))   => Some(d)
        case Const(CDouble(d)) => Some(d)
        case _                 => None
      }
    }

    //tic variable node
    case class SplitParam(id: Int, parentId: Identifier)(val loc: Line) extends DepGraph {
      val identities = Identities.Specs.empty

      def uniqueIdentities = false

      def valueKeys = Set.empty

      val isSingleton = true

      val containsSplitArg = true
    }

    //grouping node (e.g. foo where foo.a = 'b)
    case class SplitGroup(id: Int, identities: Identities, parentId: Identifier)(val loc: Line) extends DepGraph {
      def valueKeys = Set.empty

      def uniqueIdentities = false

      val isSingleton = false

      val containsSplitArg = true
    }

    case class Const(value: RValue)(val loc: Line) extends DepGraph with Root {
      lazy val identities = Identities.Specs.empty

      def uniqueIdentities = false

      def valueKeys = Set.empty

      val isSingleton = true

      val containsSplitArg = false
    }

    // TODO
    class Undefined(val loc: Line) extends DepGraph with Root {
      lazy val identities = Identities.Undefined

      def uniqueIdentities = false

      def valueKeys = Set.empty

      val isSingleton = false

      val containsSplitArg = false

      override def equals(that: Any) = that match {
        case that: Undefined => true
        case _               => false
      }

      override def hashCode = 42
    }

    object Undefined {
      def apply(loc: Line): Undefined        = new Undefined(loc)
      def unapply(undef: Undefined): Boolean = true
    }

    case class New(parent: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = Identities.Specs(Vector(SynthIds(IdGen.nextInt())))

      def uniqueIdentities = true

      def valueKeys = parent.valueKeys

      lazy val isSingleton = parent.isSingleton

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Morph1(mor: Morphism1, parent: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      private def specs(policy: IdentityPolicy): Vector[IdentitySpec] = policy match {
        case IdentityPolicy.Product(left, right) => (specs(left) ++ specs(right)).distinct // keeps first instance seen of the id
        case (_: IdentityPolicy.Retain)          => parent.identities.fold(Predef.identity, Vector.empty)
        case IdentityPolicy.Synthesize           => Vector(SynthIds(IdGen.nextInt()))
        case IdentityPolicy.Strip                => Vector.empty
      }

      lazy val identities = Identities.Specs(specs(mor.idPolicy))

      def uniqueIdentities = false

      def valueKeys = Set.empty

      lazy val isSingleton = false

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Morph2(mor: Morphism2, left: DepGraph, right: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {

      private def specs(policy: IdentityPolicy): Vector[IdentitySpec] = policy match {
        case IdentityPolicy.Product(left, right) => (specs(left) ++ specs(right)).distinct // keeps first instance seen of the id
        case IdentityPolicy.Retain.Left          => left.identities.fold(Predef.identity, Vector.empty)
        case IdentityPolicy.Retain.Right         => right.identities.fold(Predef.identity, Vector.empty)
        case IdentityPolicy.Retain.Merge =>
          IdentityMatch(left, right).identities.fold(Predef.identity, Vector.empty)
        case IdentityPolicy.Retain.Cross =>
          (left.identities ++ right.identities).fold(Predef.identity, Vector.empty)
        case IdentityPolicy.Synthesize => Vector(SynthIds(IdGen.nextInt()))
        case IdentityPolicy.Strip      => Vector.empty
      }

      lazy val identities = Identities.Specs(specs(mor.idPolicy))

      def uniqueIdentities = false

      def valueKeys = Set.empty

      lazy val isSingleton = false

      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }

    case class Distinct(parent: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs(Vector(SynthIds(IdGen.nextInt())))

      def uniqueIdentities = true

      def valueKeys = parent.valueKeys

      lazy val isSingleton = parent.isSingleton

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class AbsoluteLoad(parent: DepGraph, jtpe: JType = JType.JUniverseT)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = parent match {
        case Const(CString(path))                     => Identities.Specs(Vector(LoadIds(path)))
        case Morph1(expandGlob, Const(CString(path))) => Identities.Specs(Vector(LoadIds(path)))
        case _                                        => Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      }

      def uniqueIdentities = true

      def valueKeys = Set.empty

      val isSingleton = false

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class RelativeLoad(parent: DepGraph, jtpe: JType = JType.JUniverseT)(val loc: Line) extends DepGraph with StagingPoint {
      // FIXME we need to use a special RelLoadIds to avoid ambiguities in provenance
      lazy val identities = parent match {
        case Const(CString(path))                     => Identities.Specs(Vector(LoadIds(path)))
        case Morph1(expandGlob, Const(CString(path))) => Identities.Specs(Vector(LoadIds(path)))
        case _                                        => Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      }

      def uniqueIdentities = true

      def valueKeys = Set.empty

      val isSingleton = false

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Operate(op: UnaryOperation, parent: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = parent.identities

      def uniqueIdentities = parent.uniqueIdentities

      def valueKeys = parent.valueKeys

      lazy val isSingleton = parent.isSingleton

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Reduce(red: Reduction, parent: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs.empty

      def uniqueIdentities = false

      def valueKeys = Set.empty

      val isSingleton = true

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class MegaReduce(reds: List[(TS1, List[Reduction])], parent: DepGraph) extends DepGraph with StagingPoint {
      val loc = parent.loc

      lazy val identities = Identities.Specs.empty

      def uniqueIdentities = false

      def valueKeys = Set.empty

      val isSingleton = false

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Split(spec: BucketSpec, child: DepGraph, id: Identifier)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs(Vector(SynthIds(IdGen.nextInt())))

      def uniqueIdentities = true

      def valueKeys = Set.empty

      lazy val isSingleton = false

      lazy val containsSplitArg = {
        def loop(spec: BucketSpec): Boolean = spec match {
          case UnionBucketSpec(left, right) =>
            loop(left) || loop(right)

          case IntersectBucketSpec(left, right) =>
            loop(left) || loop(right)

          case Group(_, target, child) =>
            target.containsSplitArg || loop(child)

          case UnfixedSolution(_, target) => target.containsSplitArg
          case Extra(target)              => target.containsSplitArg
        }

        loop(spec)
      }
    }

    case class Assert(pred: DepGraph, child: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = child.identities

      def uniqueIdentities = child.uniqueIdentities

      def valueKeys = child.valueKeys

      lazy val isSingleton = child.isSingleton

      lazy val containsSplitArg = pred.containsSplitArg || child.containsSplitArg
    }

    // note: this is not a StagingPoint, though it *could* be; this is an optimization for the common case (transpecability)
    case class Cond(pred: DepGraph, left: DepGraph, leftJoin: JoinSort, right: DepGraph, rightJoin: JoinSort)(val loc: Line) extends DepGraph {
      val peer = IUI(true, Filter(leftJoin, left, pred)(loc), Filter(rightJoin, right, Operate(Comp, pred)(loc))(loc))(loc)

      lazy val identities = peer.identities

      def uniqueIdentities = peer.uniqueIdentities

      def valueKeys = peer.valueKeys

      lazy val isSingleton = peer.isSingleton

      lazy val containsSplitArg = peer.containsSplitArg
    }

    case class Observe(data: DepGraph, samples: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = data.identities

      def uniqueIdentities = data.uniqueIdentities

      def valueKeys = Set.empty

      lazy val isSingleton = data.isSingleton

      lazy val containsSplitArg = data.containsSplitArg || samples.containsSplitArg
    }

    case class IUI(union: Boolean, left: DepGraph, right: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = (left.identities, right.identities) match {
        case (Identities.Specs(a), Identities.Specs(b)) => Identities.Specs((a, b).zipped map CoproductIds)
        case _                                          => Identities.Undefined
      }

      def uniqueIdentities = false

      def valueKeys = Set.empty // TODO not correct!

      lazy val isSingleton = left.isSingleton && right.isSingleton

      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }

    case class Diff(left: DepGraph, right: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = left.identities

      def uniqueIdentities = left.uniqueIdentities

      def valueKeys = left.valueKeys

      lazy val isSingleton = left.isSingleton

      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }

    // TODO propagate AOT value computation
    case class Join(op: BinaryOperation, joinSort: JoinSort, left: DepGraph, right: DepGraph)(val loc: Line) extends DepGraph {

      lazy val identities = joinSort match {
        case Cross(_) => left.identities ++ right.identities
        case _        => IdentityMatch(left, right).identities
      }

      def uniqueIdentities = joinSort match {
        case Cross(_) | IdentitySort => left.uniqueIdentities && right.uniqueIdentities
        case _                       => false
      }

      lazy val valueKeys = left.valueKeys ++ right.valueKeys

      lazy val isSingleton = left.isSingleton && right.isSingleton

      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg

    }

    case class Filter(joinSort: JoinSort, target: DepGraph, boolean: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = joinSort match {
        case Cross(_) => target.identities ++ boolean.identities
        case _        => IdentityMatch(target, boolean).identities
      }

      def uniqueIdentities = joinSort match {
        case IdentitySort | Cross(_) => target.uniqueIdentities && boolean.uniqueIdentities
        case _                       => false
      }

      lazy val valueKeys = target.valueKeys ++ boolean.valueKeys

      lazy val isSingleton = target.isSingleton

      lazy val containsSplitArg = target.containsSplitArg || boolean.containsSplitArg
    }

    /**
      * Evaluator will deref by `sortField` to get the sort ordering and `valueField`
      * to get the actual value set that is being sorted.  Thus, `parent` is
      * assumed to evaluate to a set of objects containing `sortField` and `valueField`.
      * The identity of the sort should be stable between other sorts that are
      * ''logically'' the same.  Thus, if one were to sort set `foo` by `userId`
      * for later joining with set `bar` sorted by `personId`, those two sorts would
      * be semantically very different, but logically identitical and would thus
      * share the same identity.  This is very important to ensure correctness in
      * evaluation of the `Join` node.
      */
    case class AddSortKey(parent: DepGraph, sortField: String, valueField: String, id: Int) extends DepGraph {
      val loc = parent.loc

      lazy val identities = parent.identities

      def uniqueIdentities = parent.uniqueIdentities

      lazy val valueKeys = Set(id)

      lazy val isSingleton = parent.isSingleton

      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Memoize(parent: DepGraph, priority: Int) extends DepGraph with StagingPoint {
      val loc = parent.loc

      lazy val identities = parent.identities

      def uniqueIdentities = parent.uniqueIdentities

      def valueKeys = parent.valueKeys

      lazy val isSingleton = parent.isSingleton

      lazy val containsSplitArg = parent.containsSplitArg
    }

    sealed trait BucketSpec

    case class UnionBucketSpec(left: BucketSpec, right: BucketSpec)     extends BucketSpec
    case class IntersectBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec

    case class Group(id: Int, target: DepGraph, forest: BucketSpec) extends BucketSpec

    case class UnfixedSolution(id: Int, solution: DepGraph) extends BucketSpec
    case class Extra(expr: DepGraph)                        extends BucketSpec

    sealed trait IdentitySpec {
      def canonicalize: IdentitySpec       = this
      def possibilities: Set[IdentitySpec] = Set(this)
    }

    case class LoadIds(path: String) extends IdentitySpec
    case class SynthIds(id: Int)     extends IdentitySpec

    case class CoproductIds(left: IdentitySpec, right: IdentitySpec) extends IdentitySpec {
      override def canonicalize = {
        val left2  = left.canonicalize
        val right2 = right.canonicalize
        val this2  = CoproductIds(left2, right2)

        val pos = this2.possibilities

        pos reduceOption CoproductIds orElse pos.headOption getOrElse this2
      }

      override def possibilities: Set[IdentitySpec] = {
        val leftPos = left match {
          case left: CoproductIds => left.possibilities
          case _                  => Set(left)
        }

        val rightPos = right match {
          case right: CoproductIds => right.possibilities
          case _                   => Set(right)
        }

        leftPos ++ rightPos
      }
    }

    sealed trait JoinSort
    sealed trait TableSort extends JoinSort

    case object IdentitySort extends TableSort
    case class ValueSort(id: Int) extends TableSort

    // sealed trait Join
    // case object IdentityJoin(ids: Vetor[Int]) extends Join
    // case class PartialIdentityJoin(ids: Vector[Int]) extends Join
    // case class ValueJoin(id: Int) extends Join
    case class Cross(hint: Option[CrossOrder] = None) extends JoinSort

    case class IdentityMatch(left: DepGraph, right: DepGraph) {
      def identities: Identities = (left.identities, right.identities) match {
        case (Identities.Specs(lSpecs), Identities.Specs(rSpecs)) =>
          val specs = (sharedIndices map { case (lIdx, _) => lSpecs(lIdx) }) ++
              (leftIndices map lSpecs) ++ (rightIndices map rSpecs)
          Identities.Specs(specs map (_.canonicalize))
        case (_, _) => Identities.Undefined
      }

      private def canonicalize(identities: Identities) = identities match {
        case Identities.Specs(ids) => Identities.Specs(ids map (_.canonicalize))
        case other                 => other
      }

      private val leftIdentities  = canonicalize(left.identities)
      private val rightIdentities = canonicalize(right.identities)

      private def intersects(a: IdentitySpec, b: IdentitySpec): Boolean =
        !(a.possibilities intersect b.possibilities).isEmpty

      private def union(a: IdentitySpec, b: IdentitySpec): IdentitySpec = (a, b) match {
        case (CoproductIds(_, _), CoproductIds(_, _)) =>
          (a.possibilities ++ b.possibilities).reduceRight(CoproductIds(_, _))
        case (CoproductIds(_, _), _) => a
        case (_, CoproductIds(_, _)) => b
        case _                       => a
      }

      private def findMatch(specs: Vector[IdentitySpec])(spec: (IdentitySpec, Int)): Option[(IdentitySpec, (Int, Int))] = {
        val idx = specs indexWhere { s =>
          intersects(spec._1, s)
        }
        if (idx < 0) None else Some((union(spec._1, specs(idx)), (spec._2, idx)))
      }

      private def matches = (leftIdentities, rightIdentities) match {
        case (Identities.Specs(a), Identities.Specs(b)) =>
          a.zipWithIndex flatMap findMatch(b)
        case (Identities.Undefined, _) | (_, Identities.Undefined) => Vector.empty
      }

      private def extras(identities: Identities): Vector[Int] = identities match {
        case Identities.Specs(ids) =>
          ids.zipWithIndex collect {
            case (id, index) if !(sharedIds contains id) =>
              index
          }
        case _ => Vector.empty
      }

      val (sharedIds, sharedIndices) = matches.unzip
      val leftIndices                = extras(leftIdentities)
      val rightIndices               = extras(rightIdentities)

      def mapLeftIndex(i: Int): Int = {
        val j = sharedIndices.unzip._1.indexOf(i)
        if (j < 0) leftIndices.indexOf(i) + sharedIndices.size else j
      }

      def mapRightIndex(i: Int): Int = {
        val j = sharedIndices.unzip._2.indexOf(i)
        if (j < 0) rightIndices.indexOf(i) + sharedIndices.size + leftIndices.size else j
      }
    }
  }

  sealed trait StackError

  case object EmptyStream extends StackError
  case class StackUnderflow(instr: Instruction) extends StackError
  case object UnknownLine extends StackError

  case object EmptyStackAtEnd          extends StackError
  case object MultipleStackValuesAtEnd extends StackError

  case class NonPositiveSwapDepth(instr: Instruction) extends StackError

  case object MergeWithUnmatchedTails extends StackError
  case object UnmatchedMerge          extends StackError
  case object UnmatchedSplit          extends StackError

  case class OperationOnBucket(instr: Instruction) extends StackError
  case class BucketOperationOnSets(instr: Instruction) extends StackError
  case object BucketAtEnd extends StackError

  case class UnableToLocateSplitDescribingId(id: Int) extends StackError
}
