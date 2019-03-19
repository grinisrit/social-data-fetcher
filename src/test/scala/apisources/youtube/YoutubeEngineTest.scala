package apisources.youtube

import apisources.youtube.YoutubeStream.{CommentRepliesBundle, UpdateReplies}
import com.google.api.services.youtube.model.{Comment, CommentThread}
import org.bson.BsonInt64
import org.scalatest._
import org.slf4j.{Logger, LoggerFactory}
import servicetools.YoutubeConfiguration

import collection.JavaConverters._
import scala.util.Try

class YoutubeEngineTest extends FunSuite with BeforeAndAfterAllConfigMap {

  val log: Logger = LoggerFactory.getLogger(classOf[YoutubeEngineTest])
  var videoId: String = _
  var conf: YoutubeConfiguration = _

  override def beforeAll(configMap: ConfigMap): Unit = {
    videoId = configMap.get("videoId").getOrElse("qjUVOzdq0VE").toString
    val userName = configMap.get("ytUser").getOrElse("roland.grinis").toString
    val dataStoreDirectory = configMap.get("ytStoreDir").getOrElse("/youtube-credentials").toString
    val clientSecretJson = configMap.get("ytClientJson").getOrElse("/client_secret.json").toString
    conf = YoutubeConfiguration(userName, dataStoreDirectory, clientSecretJson)
  }

  def updateReplies(bundle: CommentRepliesBundle, state: UpdateReplies, youtubeEngine: YoutubeEngine)
  : (CommentRepliesBundle, Vector[Comment]) = {
    val replies: Vector[Comment] = if(state.status){
      Try{ youtubeEngine.queryComments("snippet").setParentId(state.threadId).execute().getItems.asScala.toVector }
        .getOrElse(Vector.empty[Comment])
    } else Vector.empty[Comment]
    (bundle, replies)
  }

  test("Youtube API connection"){
    val ytClient: YoutubeEngine = new YoutubeEngine(conf)
    val ids = ytClient.fetchThreadsId(videoId)
    val nids = ids.length
    log.info(s"Fetched $nids ids for comments threads on video: https://www.youtube.com/watch?v=$videoId")

    val streamSource = YoutubeStream.chunkStreaming(ids,50)
    val commentThreads =
      streamSource
        .map( ytClient.queryThreads("snippet,replies").setId(_).execute())
        .flatMap(_.getItems.asScala.toVector )

    val nThreads = commentThreads.length
    log.info(s"Fetched $nThreads comment+replies threads")
    assert(nids == nThreads)

    val fullQuery = commentThreads
      .map(YoutubeStream.checkUpdateReplies)
      .map{ case (bundle, state) => updateReplies(bundle, state ,ytClient) }
      .map{ case (bundle, replies) => YoutubeEngine.serializeCommentRepliesBundle(bundle, replies) }

    val totalCount = fullQuery.map(_.get[BsonInt64]("repliesCount").get.getValue + 1).sum
    log.info(s"Fetched $totalCount comments in total")

  }
}
