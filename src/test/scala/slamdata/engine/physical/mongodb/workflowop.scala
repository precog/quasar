package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

class WorkflowOpSpec extends Specification {
  import WorkflowOp._
  import PipelineOp._

  val readFoo = ReadOp(Collection("foo"))
  
  "WorkflowOp.++" should {
    "merge trivial reads" in {
      readFoo merge readFoo must_==
        (ExprOp.DocVar.ROOT(), ExprOp.DocVar.ROOT()) -> readFoo
    }
    
    "merge group by constant with project" in {
      val left = chain(readFoo, 
                  groupOp(
                    Grouped(ListMap()),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
      val right = chain(readFoo,
                    projectOp(Reshape.Doc(ListMap(
                      BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city")))))))
          
      val ((lb, rb), op) = left merge right
      
      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocField(BsonField.Name("__sd_tmp_1"))
      op must_== 
          chain(readFoo,
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city")))))),
              BsonField.Name("rIght") -> -\/ (ExprOp.DocVar.ROOT())))), 
            groupOp(
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            unwindOp(
              ExprOp.DocField(BsonField.Name("__sd_tmp_1"))))
    }
  }
  
  "RenderTree[WorkflowOp]" should {
    val RW = RenderTree[WorkflowOp]

    "render read" in {
      RW.render(readFoo) must_==
        Terminal("foo", List("WorkflowOp", "ReadOp"))
    }

    "render simple project" in {
      val op = chain(readFoo,
        projectOp( 
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz")))))))
 
      RW.render(op) must_==
        NonTerminal("",
          Terminal("foo", List("WorkflowOp", "ReadOp")) ::
            NonTerminal("",
              Terminal("bar -> $baz", List("PipelineOp", "Project", "Name")) :: Nil,
              List("PipelineOp", "Project", "Shape")) ::
            Nil,
          List("WorkflowOp", "ProjectOp"))
    }

    "render array project" in {
      val op = chain(readFoo,
        projectOp(
          Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz")))))))
 
      RW.render(op) must_==
        NonTerminal("",
          Terminal("foo", List("WorkflowOp", "ReadOp")) ::
            NonTerminal("",
              Terminal("0 -> $baz", List("PipelineOp", "Project", "Index")) :: Nil,
              List("PipelineOp", "Project", "Shape")) ::
            Nil,
          List("WorkflowOp", "ProjectOp"))
    }
    
    "render nested project" in {
      val op = chain(readFoo,
        projectOp(
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))))))))
 
      RW.render(op) must_==
        NonTerminal("",
          Terminal("foo", List("WorkflowOp", "ReadOp")) ::
            NonTerminal("",
              NonTerminal("bar",
                Terminal("0 -> $baz", List("PipelineOp", "Project", "Index")) :: Nil,
                List("PipelineOp", "Project", "Shape")) ::
              Nil,
              List("PipelineOp", "Project", "Shape")) ::
            Nil,
          List("WorkflowOp", "ProjectOp"))
    }
  }
}