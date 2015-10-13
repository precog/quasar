package quasar.mongo

import quasar.Predef._

import com.mongodb.MongoClient

import scalaz.concurrent.Task

class MongoConnection(impl: MongoClient) {
  def exists(path: Collection): Task[Boolean] = Task.delay {
    val databaseExists = impl.listDatabaseNames().iterator().contains(path.databaseName)
    if (databaseExists)
      impl.getDatabase(path.databaseName).listCollectionNames().iterator().contains(path.collectionName)
    else false
  }
}
