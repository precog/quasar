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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.jscore._
import quasar.fp._
import quasar.fp.ski._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.selector.Selector
import quasar.physical.mongodb.workflow._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._

package object optimize {
  object pipeline {
    import fixExprOp._

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    private def deleteUnusedFields0[F[_]: Functor: Refs](op: Fix[F], usedRefs: Option[Set[DocVar]])
      (implicit I: WorkflowOpCoreF :<: F): Fix[F] = {
      def getRefs[A](op: F[_], prev: Option[Set[DocVar]]):
          Option[Set[DocVar]] = op match {
        case I($GroupF(_, _, _))           => Some(Refs[F].refs(op).toSet)
        // FIXME: Since we can’t reliably identify which fields are used by a
        //        JS function, we need to assume they all are, until we hit the
        //        next $GroupF or $ProjectF.
        case I($MapF(_, _, _))             => None
        case I($SimpleMapF(_, _, _))       => None
        case I($FlatMapF(_, _, _))         => None
        case I($ReduceF(_, _, _))          => None
        case I($ProjectF(_, _, IncludeId)) => Some(Refs[F].refs(op).toSet + IdVar)
        case I($ProjectF(_, _, _))         => Some(Refs[F].refs(op).toSet)
        case I($FoldLeftF(_, _))           => prev.map(_ + IdVar)
        case _                             => prev.map(_ ++ Refs[F].refs(op))
      }

      def unused(defs: Set[DocVar], refs: Set[DocVar]): Set[DocVar] =
        defs.filterNot(d => refs.exists(ref => d.startsWith(ref) || ref.startsWith(d)))

      def getDefs[A](op: F[A]): Set[DocVar] = (I.prj(op) match {
        case Some(p @ $ProjectF(_, _, _))   => p.getAll.map(_._1)
        case Some(g @ $GroupF(_, _, _))     => g.getAll.map(_._1)
        case Some(s @ $SimpleMapF(_, _, _)) => s.getAll.getOrElse(Nil)
        case _                             => Nil
      }).map(DocVar.ROOT(_)).toSet

      val pruned =
        usedRefs.fold(op.unFix) { usedRefs =>
          val unusedRefs =
            unused(getDefs(op.unFix), usedRefs).toList.flatMap(_.deref.toList)
          op.unFix match {
            case I(p @ $ProjectF(_, _, _))   =>
              val p1 = p.deleteAll(unusedRefs)
              if (p1.shape.value.isEmpty) p1.pipeline.src.unFix
              else I.inj(p1)
            case I(g @ $GroupF(_, _, _))     => I.inj(g.deleteAll(unusedRefs.map(_.flatten.head)))
            case I(s @ $SimpleMapF(_, _, _)) => I.inj(s.deleteAll(unusedRefs))
            case o                           => o
          }
        }

      Fix(pruned.map(deleteUnusedFields0(_, getRefs(pruned, usedRefs))))
    }

    def deleteUnusedFields[F[_]: Functor: Refs](op: Fix[F])(implicit ev: WorkflowOpCoreF :<: F): Fix[F] =
      deleteUnusedFields0(op, None)

    /** Converts a \$group with fields that duplicate keys into a \$group
      * without those fields followed by a \$project that projects those fields
      * from the key.
      */
    def simplifyGroupƒ[F[_]: Coalesce: Functor](implicit ev: WorkflowOpCoreF :<: F):
        F[Fix[F]] => Option[F[Fix[F]]] = {
      case ev($GroupF(src, Grouped(cont), id)) =>
        val (newCont, proj) =
          cont.foldLeft[(ListMap[BsonField.Name, AccumOp[Fix[ExprOp]]], ListMap[BsonField.Name, Reshape.Shape[ExprOp]])](
            (ListMap(), ListMap())) {
            case ((newCont, proj), (k, v)) =>
              lazy val default =
                (newCont + (k -> v), proj + (k -> $include().right))
              def maybeSimplify(loc: Option[DocVar]) =
                loc.fold(
                  default)(
                  l => v match {
                    case $addToSet(_) | $push(_) => default
                    case _ => (newCont, proj + (k -> $var(l).right))
                  })

              id.fold(
                re => maybeSimplify(re.find(v.copoint).map(f => DocField(IdName \ f))),
                expr =>
                  maybeSimplify(
                    if (v.copoint == expr) DocField(IdName).some
                    else None))
          }

        if (newCont == cont)
          None
        else
          chain(src,
            $group[F](Grouped(newCont), id),
            $project[F](
              Reshape(proj),
              IgnoreId)).project.some
      case _ => None
    }

    def simplifyGroup[F[_]: Coalesce: Functor](op: Fix[F])(implicit ev: WorkflowOpCoreF :<: F): Fix[F] =
      op.transCata[Fix[F]](orOriginal(simplifyGroupƒ[F]))

    def simplifyExprOpsƒ[F[_]: ExprOpTraversal]: F[Fix[F]] => F[Fix[F]] =
      ExprOpTraversal[F].exprOps.modify(
        _.cata[Fix[ExprOp]](ExprOpOps.simplifyAlg[ExprOp]))

    private def reorderOpsƒ[F[_]: Coalesce: Functor](implicit I: WorkflowOpCoreF :<: F)
        : F[Fix[F]] => Option[F[Fix[F]]] = {
      def rewriteSelector(sel: Selector, defs: Map[DocVar, DocVar]): Option[Selector] =
        sel.mapUpFieldsM { f =>
          defs.toList.map {
            case (DocVar(_, Some(lhs)), DocVar(_, rhs)) =>
              if (f == lhs) rhs
              else (f relativeTo lhs).map(rel => rhs.fold(rel)(_ \ rel))
            case _ => None
          }.collectFirst { case Some(x) => x }
        }

      {
        case I($SkipF(Embed(I($ProjectF(src0, shape, id))), count)) =>
          chain(src0,
            $skip[F](count),
            $project[F](shape, id)).project.some
        case I($SkipF(Embed(I($SimpleMapF(src0, fn @ NonEmptyList(MapExpr(_), INil()), scope))), count)) =>
          chain(src0,
            $skip[F](count),
            $simpleMap[F](fn, scope)).project.some

        case I($LimitF(Embed(I($ProjectF(src0, shape, id))), count)) =>
          chain(src0,
            $limit[F](count),
            $project[F](shape, id)).project.some
        case I($LimitF(Embed(I($SimpleMapF(src0, fn @ NonEmptyList(MapExpr(_), INil()), scope))), count)) =>
          chain(src0,
            $limit[F](count),
            $simpleMap[F](fn, scope)).project.some

        case I($MatchF(Embed(I(p @ $ProjectF(src0, shape, id))), sel)) =>
          val defs = p.getAll.collect {
            case (n, $var(x))    => DocField(n) -> x
            case (n, $include()) => DocField(n) -> DocField(n)
          }.toMap
          rewriteSelector(sel, defs).map(sel =>
            chain(src0,
              $match[F](sel),
              $project[F](shape, id)).project)

        case I($MatchF(Embed(I($SimpleMapF(src0, exprs @ NonEmptyList(MapExpr(jsFn), INil()), scope))), sel)) => {
          import quasar.javascript._
          @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
          def defs(expr: JsCore): Map[DocVar, DocVar] =
            expr.simplify match {
              case Obj(values) =>
                values.toList.collect {
                  case (n, Ident(jsFn.param)) => DocField(BsonField.Name(n.value)) -> DocVar.ROOT()
                  case (n, Access(Ident(jsFn.param), Literal(Js.Str(x)))) => DocField(BsonField.Name(n.value)) -> DocField(BsonField.Name(x))
                }.toMap
              case SpliceObjects(srcs) => srcs.map(defs).foldLeft(Map[DocVar, DocVar]())(_++_)
              case _ => Map.empty
            }
          rewriteSelector(sel, defs(jsFn.expr)).map(sel =>
            chain(src0,
              $match[F](sel),
              $simpleMap[F](exprs, scope)).project)
        }

        case op => None
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def reorderOps[F[_]: Functor: Coalesce](wf: Fix[F])
      (implicit I: WorkflowOpCoreF :<: F)
      : Fix[F] = {
      val reordered = wf.transCata[Fix[F]](orOriginal(reorderOpsƒ[F]))
      if (reordered == wf) wf else reorderOps(reordered)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def get0(leaves: List[BsonField.Name], rs: List[Reshape[ExprOp]]): Option[Reshape.Shape[ExprOp]] = {
      (leaves, rs) match {
        case (_, Nil) => $var(BsonField(leaves).map(DocVar.ROOT(_)).getOrElse(DocVar.ROOT())).right.some

        case (Nil, r :: rs) => inlineProject0(r, rs).map(_.left)

        case (l :: ls, r :: rs) => r.get(l).flatMap {
          case -\/ (r)          => get0(ls, r :: rs)
          case  \/-($include()) => get0(leaves, rs)
          case  \/-($var(d))    => get0(d.path ++ ls, rs)
          case  \/-(e) => if (ls.isEmpty) fixExpr(e, rs).map(_.right) else none
        }
      }
    }

    private def fixExpr(e: Fix[ExprOp], rs: List[Reshape[ExprOp]]):
        Option[Fix[ExprOp]] =
      e.cataM[Option, Fix[ExprOp]] {
        case $varF(ref) => get0(ref.path, rs).flatMap(_.toOption)
        case x          => Fix(x).some
      }

    private def inlineProject0(r: Reshape[ExprOp], rs: List[Reshape[ExprOp]]): Option[Reshape[ExprOp]] =
      inlineProject($ProjectF((), r, IgnoreId), rs)

    def inlineProject[A](p: $ProjectF[A], rs: List[Reshape[ExprOp]]): Option[Reshape[ExprOp]] = {
      val map = p.getAll.map { case (k, v) =>
        k -> (v match {
          case $include() =>
            get0(k.flatten.toList, rs).map(_.right).orElse(
              if (k == IdName) ().left.some
              else none)
          case $var(d)    => get0(d.path, rs).map(_.right)
          case _          => fixExpr(v, rs).map(_.right.right)
        })
      }.foldLeftM[Option, ListMap[BsonField, Reshape.Shape[ExprOp]]](ListMap()) {
        case (acc, (k, Some(\/-(v)))) => (acc + (k -> v)).some
        case (acc, (_, Some(-\/(_)))) => acc.some
        case (_,   (_, None))         => none
      }

      map.map(p.empty.setAll(_).shape)
    }

    /** Map from old grouped names to new names and mapping of expressions. */
    def renameProjectGroup(r: Reshape[ExprOp], g: Grouped[ExprOp]): Option[ListMap[BsonField.Name, List[BsonField.Name]]] = {
      val s = r.value.toList.traverse {
        case (newName, \/-($var(v))) =>
          v.path match {
            case List(oldHead @ BsonField.Name(_)) =>
              g.value.get(oldHead).map { κ(oldHead -> newName) }
            case _ => None
          }
        case _ => None
      }

      def multiListMap[A, B](ts: List[(A, B)]): ListMap[A, List[B]] =
        ts.foldLeft(ListMap[A,List[B]]()) { case (map, (a, b)) => map + (a -> (map.get(a).getOrElse(List[B]()) :+ b)) }

      s.map(multiListMap)
    }

    def inlineProjectGroup(r: Reshape[ExprOp], g: Grouped[ExprOp]): Option[Grouped[ExprOp]] = {
      for {
        names   <- renameProjectGroup(r, g)
        values1 = names.flatMap {
          case (oldName, ts) => ts.map((_: BsonField.Name) -> g.value(oldName))
        }
      } yield Grouped(values1)
    }

    def inlineProjectUnwindGroup(r: Reshape[ExprOp], unwound: DocVar, g: Grouped[ExprOp]): Option[(DocVar, Grouped[ExprOp])] = {
      for {
        names    <- renameProjectGroup(r, g)
        unwound1 <- unwound.path match {
          case (name @ BsonField.Name(_)) :: Nil => names.get(name) match {
            case Some(n :: Nil) => Some(DocField(n))
            case _ => None
          }
          case _ => None
        }
        values1 = names.flatMap {
          case (oldName, ts) => ts.map((_: BsonField.Name) -> g.value(oldName))
        }
      } yield unwound1 -> Grouped(values1)
    }

    def inlineGroupProjects[F[_]: Functor](g: $GroupF[Fix[F]])
      (implicit I: WorkflowOpCoreF :<: F)
      : Option[(Fix[F], Grouped[ExprOp], Reshape.Shape[ExprOp])] = {
      def collectShapes: GAlgebra[(Fix[F], ?), F, (List[Reshape[ExprOp]], Fix[F])] =
        {
          case I($ProjectF(src, shape, _)) => ((x: List[Reshape[ExprOp]]) => shape :: x).first(src._2)
          case x                           => (Nil, Fix(x.map(_._1)))
        }

      val (rs, src) = g.src.para(collectShapes)

      if (src == g.src) None
      else {
        val grouped = ListMap(g.getAll: _*).traverse(_.traverse(fixExpr(_, rs)))

        val by = g.by.fold(
          inlineProject0(_, rs).map(_.left),
          fixExpr(_, rs).map(\/-(_)))
        (grouped |@| by)((grouped, by) => (src, Grouped(grouped), by))
      }
    }
  }
}
