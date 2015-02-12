package slamdata.engine.admin

import scala.collection.immutable.ListMap
import scalaz._
import Scalaz._
import scalaz.stream.{async => _, _}
import scalaz.concurrent._

import argonaut._
import Argonaut._

import slamdata.engine.{ResultPath}
import slamdata.engine.fs._
import slamdata.engine.fp._

import SwingUtils._

class CollectionTableModel(fs: FileSystem, path: ResultPath) extends javax.swing.table.AbstractTableModel {
  val ChunkSize = 100

  async(fs.count(path.path))(_.fold(
    err => println(err),
    rows => {
      collectionSize = Some((rows min Int.MaxValue).toInt)
      fireTableStructureChanged
    }))

  var collectionSize: Option[Int] = None
  var results: PartialResultSet[Json] = PartialResultSet(ChunkSize)
  var columns: List[Values.Path] = Nil

  def getColumnCount: Int = columns.length max 1
  def getRowCount: Int = collectionSize.getOrElse(1)
  def getValueAt(row: Int, column: Int): Object =
    collectionSize match {
      case None => "loading"
      case _ =>
        results.get(row) match {
          case -\/(-\/(chunk)) => {
            load(chunk)
            "loading"
          }
          case -\/(\/-(_)) => "loading"
          case \/-(json) => (for {
              p <- columns.drop(column).headOption
              v <- Values.flatten(json).get(p)
            } yield v).getOrElse("")
        }
    }
  override def getColumnName(column: Int) =
    columns.drop(column).headOption.fold("value")(_.mkString("."))  // TODO: prune common prefix

  /**
    Get the value of every cell in the table as a sequence of rows, the first
    row of which is the column names (as currently seen in the table). The
    values are read directly from the source collection, and not cached for
    display.
    */
  def getAllValues: Process[Task, List[String]] = {
    // TODO: handle columns not discovered yet?
    val currentColumns = columns
    val header = currentColumns.map(_.mkString("."))
    Process.emit(header) ++ fs.scanAll(path.path).map(rj => Parse.parse(rj.value).fold(
      err => List("error: " + err),
      json => {
        val map = Values.flatten(json)
        currentColumns.map(p => map.get(p).getOrElse(""))
      }))
  }

  def cleanup: Task[Unit] = path match {
    case ResultPath.Temp(path) => for {
      _ <- fs.delete(path)
      _ = println("Deleted temp collection: " + path)
    } yield ()
    case ResultPath.User(_) => Task.now(())
  }

  private def load(chunk: Chunk) = {
    def fireUpdated = fireTableRowsUpdated(chunk.firstRow, chunk.firstRow + chunk.size - 1)

    results = results.withLoading(chunk)
    fireUpdated

    async(fs.scan(path.path, Some(chunk.firstRow), Some(chunk.size)).runLog)(_.fold(
      err  => println(err),
      rows => {
        val json = rows.toVector.map(rj => Parse.parse(rj.value).fold(err => Json("parse error" := err), identity))

        results = results.withRows(chunk, json)

        val newColumns = json.map(obj => Values.flatten(obj).keys.toList)
        val merged = newColumns.foldLeft[List[Values.Path]](columns) { case (cols, nc) => Values.mergePaths(cols, nc) }
        if (merged != columns) {
          columns = merged
          fireTableStructureChanged
        }
        else fireUpdated
      }))
  }
}

object Values {
  type Path = List[String]

  def mergePaths(as: List[Path], bs: List[Path]): List[Path] =
    (as ++ bs).distinct

  def flatten(json: Json): ListMap[Path, String] = {
    def loop(json: Json): String \/ List[(Path, String)] = {
      def prepend(name: String, json: Json): List[(Path, String)] =
        loop(json) match {
          case -\/ (value) => (List(name) -> value) :: Nil
          case  \/-(map)   => map.map(t => (name :: t._1) -> t._2)
        }
      json.fold(
        -\/(""),
        bool => -\/ (bool.toString),
        num  => -\/ (json.toString),
        str  => -\/ (str),
        arr  =>  \/-(arr.zipWithIndex.flatMap { case (c, i) => prepend(i.toString, c) }),
        obj  =>  obj.toList.flatMap { case (f, c) => prepend(f, c) } match {
          case (List("$date"), value: String) :: Nil => -\/(value)
          case (List("$oid"), value: String) :: Nil => -\/(value)
          case map => \/-(map)
        }
      )
    }

    loop(json) match {
      case -\/ (value) => ListMap(List("value") -> value)
      case  \/-(map)   => map.toListMap
    }
  }
}

case class Chunk(chunkIndex: Int, firstRow: Int, size: Int)
case object Loading
case class PartialResultSet[A](chunkSize: Int, chunks: Map[Int, Loading.type \/ Vector[A]] = Map[Int, Loading.type \/ Vector[A]]()) {
  import PartialResultSet._

  /**
   * One of:
   * - a chunk specifying rows that need to be loaded
   * - Loading, signifying the requested row is already being loaded
   * - the requested row
   */
  def get(row: Int): Chunk \/ Loading.type \/ A = {
    val (chunk, offset) = chunkIndex(row)
    chunks.get(chunk) match {
      case None => -\/(-\/(Chunk(chunk, chunk*chunkSize, chunkSize)))
      case Some(\/-(rows)) => \/-(rows(offset))
      case Some(_) => -\/(\/-(Loading))
    }
  }

  def withLoading(chunk: Chunk) = copy(chunks = chunks + (chunk.chunkIndex -> -\/(Loading)))

  def withRows(chunk: Chunk, rows: Vector[A]) = copy(chunks = chunks + (chunk.chunkIndex -> \/-(rows)))

  private def chunkIndex(row: Int): (Int, Int) = (row / chunkSize) -> (row % chunkSize)
}
