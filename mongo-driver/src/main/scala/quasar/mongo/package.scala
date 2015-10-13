package quasar

import com.mongodb.MongoClient
import com.mongodb.client.MongoCursor
import scala.collection.Iterator

package object mongo {

  implicit def toScalaIterator[A](cursor: MongoCursor[A]): Iterator[A] = {
    new Iterator[A] {
      def hasNext = cursor.hasNext
      def next = cursor.next()
    }
  }

  implicit def augmentMongoClient(a: MongoClient): MongoConnection = new MongoConnection(a)
}
