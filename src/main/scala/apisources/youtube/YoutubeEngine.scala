package apisources.youtube

import java.io.{File, FileInputStream, InputStream, InputStreamReader}
import java.time.LocalDateTime
import java.util

import apisources.youtube.YoutubeStream.CommentRepliesBundle
import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.client.http.HttpTransport
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver
import com.google.api.services.youtube.model.{Comment, CommentThread}
import com.google.api.services.youtube.{YouTube, YouTubeScopes}
import org.mongodb.scala.bson.collection.immutable.Document
import servicetools.YoutubeConfiguration

import scala.collection.mutable
import collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Try

class YoutubeEngine(config: YoutubeConfiguration) {

  private val APPLICATION_NAME: String = "youtube-stream-source"
  private val DATA_STORE_DIR: File = new File(config.dataStoreDirectory)
  private val DATA_STORE_FACTORY: FileDataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR)
  private val JSON_FACTORY: JsonFactory = JacksonFactory.getDefaultInstance
  private val HTTP_TRANSPORT: HttpTransport = GoogleNetHttpTransport.newTrustedTransport()
  private val SCOPES: util.List[String] = util.Arrays.asList(YouTubeScopes.YOUTUBE_READONLY, YouTubeScopes.YOUTUBE_FORCE_SSL)

  private val in: InputStream = new FileInputStream(config.clientSecretJson)
  private val clientSecrets: GoogleClientSecrets =
    GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in))

  private val flow: GoogleAuthorizationCodeFlow =
    new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
      .setDataStoreFactory(DATA_STORE_FACTORY)
      .setAccessType("offline")
      .build()
  private val credential: Credential =
    new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize(config.userName)

  private val youtube: YouTube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
    .setApplicationName(APPLICATION_NAME)
    .build()


  def getApplicationName: String = APPLICATION_NAME

  def queryThreads(queryType: String): YouTube#CommentThreads#List =
    youtube.commentThreads().list(queryType).setOrder("time").setMaxResults(100.toLong).setTextFormat("plainText")


  def queryComments(queryType: String):  YouTube#Comments#List =
    youtube.comments().list(queryType).setMaxResults(100.toLong).setTextFormat("plainText")

  def fetchThreadsId(videoId: String): Vector[String] = {
    val ids = mutable.Queue.empty[String]
    val query = queryThreads("id").setVideoId(videoId).execute()
    val token = Option(query.getNextPageToken)
    ids ++= query.getItems.asScala.toVector.map(_.getId)

    def queryNextPage(token: Option[String]): Unit = token match {
      case Some(tkn) =>
        val query = queryThreads("id").setVideoId(videoId).setPageToken(tkn).execute()
        val newToken = Option(query.getNextPageToken)
        ids ++= query.getItems.asScala.toVector.map(_.getId)
        queryNextPage(newToken)
      case None => ()
    }

    queryNextPage(token)

    ids.toVector
  }

}

object YoutubeEngine {

  def serializeComment(comment: Comment): Document = {
    val snippet = comment.getSnippet
    val dateTime = snippet.getPublishedAt.toStringRfc3339.dropRight(5)
    Document(
      "googleId" -> comment.getId,
      "author" -> snippet.getAuthorDisplayName,
      "authorURL" -> snippet.getAuthorChannelUrl,
      "comment" -> snippet.getTextDisplay,
      "likesCount" -> snippet.getLikeCount.toLong,
      "publishedAt" -> dateTime,
      "date" -> dateToInt(dateTime)
    )
  }

  def serializeCommentRepliesBundle(bundle: CommentRepliesBundle, replies: Vector[Comment]): Document = {
    val comm = bundle.commentThread
    val thread = comm.getSnippet
    val topComment = serializeComment( thread.getTopLevelComment )
    val fetchedAt = LocalDateTime.now.toString
    val serializedReplies =(if(replies.isEmpty) bundle.replies else replies) map { serializeComment }
    val repliesCount = serializedReplies.length.toLong
    Document(
      "googleId" -> comm.getId,
      "videoId" -> thread.getVideoId,
      "repliesCount" -> repliesCount,
      "topLevelComment" -> topComment,
      "replies" -> serializedReplies,
      "fetchedAt" -> fetchedAt
    )
  }

  private def dateToInt(date: String): Int = {
    date.substring(0,10).split("-").mkString("").toInt
  }

}
