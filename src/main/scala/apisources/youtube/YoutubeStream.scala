package apisources.youtube

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.{ActorMaterializer, FlowShape, OverflowStrategy}
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition, Source}
import akka.util.Timeout
import akka.pattern.ask
import apisources.{StreamPersistenceMonitor, StreamThrottler}
import com.google.api.services.youtube.model.{Comment, CommentThread}
import org.bson.BsonInt64
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Projections
import servicetools.{SourceBroker, YoutubeSource}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._

object YoutubeStream {

  def props(mongoCollection: MongoCollection[Document], youtubeEngine: YoutubeEngine): Props =
    Props(classOf[YoutubeStream], mongoCollection, youtubeEngine)

  def chunkStreaming(ids: Vector[String], size: Int): Vector[String] =
    ids.sliding(size,size).toVector.map(_.mkString(","))

  case class StreamData[S](data: S, id: (Boolean, String))

  case class UpdateReplies(status: Boolean, threadId: String) {
    def toStreamId: (Boolean, String) = (status, threadId)
  }

  case class CommentRepliesBundle(commentThread: CommentThread, replies: Vector[Comment])

  def checkUpdateReplies(commentThread: CommentThread): (CommentRepliesBundle, UpdateReplies) = {
    val fetchedReplies = Try{commentThread.getReplies.getComments.asScala.toVector}.getOrElse(Vector.empty[Comment])
    val numReplies = commentThread.getSnippet.getTotalReplyCount.toLong
    val threadId = commentThread.getId
    val status = numReplies > fetchedReplies.length
    (CommentRepliesBundle(commentThread, fetchedReplies), UpdateReplies(status, threadId))
  }

  def toStreamData(bundleState: (CommentRepliesBundle, UpdateReplies)): StreamData[CommentRepliesBundle] = {
    StreamData(data = bundleState._1, id = bundleState._2.toStreamId )
  }

}

class YoutubeStream(mongoCollection: MongoCollection[Document], youtubeEngine: YoutubeEngine) extends Actor with ActorLogging {

  import YoutubeStream._
  import servicetools.MongoDBConnector._
  import context.dispatcher

  val streamThrottler: ActorRef = context.actorOf(StreamThrottler.props(100.seconds), "stream-throttler")

  def receive: Receive = {
    case src @ YoutubeSource(videoId) =>
      val sourceBroker = sender()
      log.info(s"Work for video $videoId started")
      val stream = launchStream(videoId)
      stream.onComplete{
        case Success(v) => sourceBroker ! SourceBroker.TaskCompleted(src)
        case Failure(e) => sourceBroker ! SourceBroker.TaskFailed(src,e)
      }
  }

  def loadExistingIds(videoId: String): Future[Vector[String]] = {
    val idField = "googleId"
    val query = mongoCollection.find(equal("videoId",videoId)).projection(Projections.include(idField)).toFuture()
    query.map(data => data.toVector.map( _.get[BsonString](idField).get.getValue ))
  }

  def launchStream(videoId: String): Future[Done] = {
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val ids = Future{youtubeEngine.fetchThreadsId(videoId)}
    val storedIds = loadExistingIds(videoId)

    val filteredIds = for{
      gotIds <- ids
      gotstoredIds <- storedIds
    } yield {
      log.info(s"$videoId: fetched ${gotIds.length} comments threads ids")
      log.info(s"$videoId: found ${gotstoredIds.length} comments threads already stored")
      gotIds.filterNot(gotstoredIds.contains(_))
    }

    filteredIds.flatMap( gotFilteredIds =>
      streamFromYoutubeAPI(chunkStreaming(gotFilteredIds,50), videoId)
    )
  }

  def streamFromYoutubeAPI(idSource: Vector[String], videoId: String): Future[Done] = {
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    log.info(s"$videoId: launching stream for ${idSource.length} chunks of comments threads")

    val source = Source(idSource map {ids => StreamData(ids, (true,ids))} )
      .via(safeAPIQuerier(4, queryThreadsForIds))
      .mapConcat{ case (str, fetchedData) => fetchedData.map(checkUpdateReplies).map(toStreamData) }
      .via(safeAPIQuerier(4, queryRepliesForThreadId))
      .map{ case (streamData, fetchedData) => YoutubeEngine.serializeCommentRepliesBundle(streamData.data,fetchedData)}
      .via(
        StreamPersistenceMonitor
          .monitor(100){count: Int => log.info(s"$videoId: persisted $count comments threads")}(context.system)
      )
    source.runWith(MongoDBSink(4,mongoCollection))
  }

  def controlledAPIQuerier[S, T](nParallel: Int, querier: String => Future[Vector[T]])
  : Flow[StreamData[S], (StreamData[S], Vector[T]), NotUsed] = {
    Flow[StreamData[S]].mapAsync(nParallel){ case str @ StreamData(data,id)  =>
      implicit val timeout: Timeout = Timeout(200.seconds)
      if(id._1) {
        for {
          _ <- streamThrottler ? StreamThrottler.PassRequest
          threads <- querier(id._2)
        } yield str -> threads
      } else {
        Future{ str -> Vector.empty[T] }
      }
    }
  }

  def streamThrottlerNotifier[S,T]: Flow[(StreamData[S], Vector[T]),(StreamData[S], Vector[T]), NotUsed] = {
    Flow[(StreamData[S], Vector[T])].map{ case (str @ StreamData(_,id), fetchedData) =>
      if(id._1 && fetchedData.isEmpty) streamThrottler ! StreamThrottler.RequestFailed
      (str, fetchedData)
    }
  }

  def safeAPIQuerier[S,T](nParallel: Int, querier: String => Future[Vector[T]])
  : Flow[StreamData[S], (StreamData[S], Vector[T]), NotUsed] =
    Flow[StreamData[S]]
      .via(controlledAPIQuerier[S,T](nParallel,querier))
      .via(streamThrottlerNotifier[S,T])
      .filterNot{ case (StreamData(_,id), fetchedData) => id._1 && fetchedData.isEmpty}

  def queryThreadsForIds(ids: String): Future[Vector[CommentThread]] =
    Future{
      Try[Vector[CommentThread]]{
        youtubeEngine.queryThreads("snippet,replies").setId(ids).execute().getItems.asScala.toVector
      }.getOrElse(Vector.empty[CommentThread])
    }

  def queryRepliesForThreadId(threadId: String): Future[Vector[Comment]] = {
    Future{
      Try[Vector[Comment]]{
        youtubeEngine.queryComments("snippet").setParentId(threadId).execute().getItems.asScala.toVector
      }.getOrElse(Vector.empty[Comment])
    }
  }

}


