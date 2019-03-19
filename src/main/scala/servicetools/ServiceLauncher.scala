package servicetools

import akka.actor.{ActorRef, ActorSystem}

object ServiceLauncher extends App with ServiceLauncherCmdLine  {

  val arguments: Vector[String] = args.toVector

  val system = ActorSystem("social-data-fetcher")
  val serviceManager: ActorRef = system.actorOf(
    ServiceManager.props(
      mongoDBHost = mongoDBHost(arguments),
      workSeed = workSeed(arguments),
      sourceConfigurations = Vector(
        YoutubeConfiguration(userName = ytUser(arguments), dataStoreDirectory = ytStoreDir(arguments), clientSecretJson = ytClientJson(arguments))
      )
    ),
    "service-manager"
  )

}

trait ServiceLauncherCmdLine {

  def mongoDBHost(arguments: Vector[String]): String = {
    val i = arguments.indexOf("-mongodb") + 1
    if(i>0) arguments(i) else "localhost:27017"
  }

  def workSeed(arguments: Vector[String]): Option[String] = {
    val i = arguments.indexOf("-fetch") + 1
    if(i>0) Some(arguments(i)) else None
  }

  def ytUser(arguments: Vector[String]): String = {
    val i = arguments.indexOf("-ytUser") + 1
    if(i>0) arguments(i) else "roland.grinis"
  }

  def ytStoreDir(arguments: Vector[String]): String = {
    val i = arguments.indexOf("-ytStoreDir") + 1
    if(i>0) arguments(i) else "/youtube-credentials"
  }

  def ytClientJson(arguments: Vector[String]): String = {
    val i = arguments.indexOf("-ytClientJson") + 1
    if(i>0) arguments(i) else "/client_secret.json"
  }

}
