package servicetools

import org.mongodb.scala.MongoClient
import org.scalatest._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

class MongoDBConnectorTest extends FunSuite with BeforeAndAfterAllConfigMap {

  val log: Logger = LoggerFactory.getLogger(classOf[MongoDBConnectorTest])
  var host: String = _

  override def beforeAll(configMap: ConfigMap): Unit = {
    host = configMap.get("host").getOrElse("localhost:27017").toString
  }

  test("MongoDB server connection"){
    val client: MongoClient = MongoClient(s"mongodb://$host")
    val databaseNames = MongoDBConnector.testMongoDBConnection(client)
    databaseNames match {
      case Success(dbs) =>
        log.info(s"Connection to mongodb://$host established successfully, found databases: ${dbs.mkString(",")}")
      case Failure(e) =>
        log.error(s"Connection to mongodb://$host failed: $e")
    }
    assert(databaseNames.isSuccess)
  }

}
