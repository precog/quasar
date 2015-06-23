package slamdata.engine

import slamdata.engine.analysis.fixplate._

import scalaz._

sealed trait Func {
  def name: String

  def help: String

  def domain: List[Type]

  def simplify0: Func.Simplifier

  def simplify(args: List[Term[LogicalPlan]]) = simplify0(args)(this)

  def apply(args: Term[LogicalPlan]*): Term[LogicalPlan] = LogicalPlan.Invoke(this, args.toList)

  def unapply[A](node: LogicalPlan[A]): Option[List[A]] = {
    node match {
      case LogicalPlan.InvokeF(f, a) if f == this => Some(a)
      case _                                      => None
    }
  }

  def apply: Func.Typer

  val unapply: Func.Untyper

  final def apply(arg1: Type, rest: Type*): ValidationNel[SemanticError, Type] = apply(arg1 :: rest.toList)

  def mappingType: MappingType

  final def arity: Int = domain.length

  override def toString: String = name
}
trait FuncInstances {
  implicit val FuncRenderTree = new RenderTree[Func] {
    override def render(v: Func) = Terminal(v.mappingType.toString :: "Func" :: Nil, Some(v.name))
  }
}
object Func extends FuncInstances {
  type Simplifier = List[Term[LogicalPlan]] => Func => Term[LogicalPlan]
  type Typer      = List[Type] => ValidationNel[SemanticError, Type]
  type Untyper    = Type => ValidationNel[SemanticError, List[Type]]
}

trait VirtualFunc {
  def apply(args: Term[LogicalPlan]*): Term[LogicalPlan]

  def unapply(t: Term[LogicalPlan]): Option[List[Term[LogicalPlan]]] = Attr.unapply(attrK(t, ())).map(l => l.map(forget(_)))

  def Attr: VirtualFuncAttrExtractor
  trait VirtualFuncAttrExtractor {
    def unapply[A](t: Cofree[LogicalPlan, A]): Option[List[Cofree[LogicalPlan, A]]]
  }
}

final case class Reduction(name: String, help: String, domain: List[Type], simplify0: Func.Simplifier, apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.ManyToOne
}

final case class Expansion(name: String, help: String, domain: List[Type], simplify0: Func.Simplifier, apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToMany
}

final case class ExpansionFlat(name: String, help: String, domain: List[Type], simplify0: Func.Simplifier, apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToManyFlat
}

final case class Mapping(name: String, help: String, domain: List[Type], simplify0: Func.Simplifier, apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToOne
}

final case class Squashing(name: String, help: String, domain: List[Type], simplify0: Func.Simplifier, apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.Squashing
}

final case class Transformation(name: String, help: String, domain: List[Type], simplify0: Func.Simplifier, apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.ManyToMany
}

sealed trait MappingType

object MappingType {
  final case object OneToOne      extends MappingType
  final case object OneToMany     extends MappingType
  final case object OneToManyFlat extends MappingType
  final case object ManyToOne     extends MappingType
  final case object ManyToMany    extends MappingType
  final case object Squashing     extends MappingType
}
