name := "social-data-fetcher"

version := "0.0.0"

scalaVersion := "2.12.3"

lazy val akkaVersion = "2.5.4"
lazy val googleVersion = "1.22.0"
lazy val youtubeVersion = s"v3-rev183-$googleVersion"

libraryDependencies ++= Seq(

  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0",

  "com.google.api-client" % "google-api-client" % googleVersion,
  "com.google.oauth-client" % "google-oauth-client" % googleVersion,
  "com.google.apis" % "google-api-services-youtube" % youtubeVersion,
  "com.google.oauth-client" % "google-oauth-client-java6" % googleVersion,
  "com.google.oauth-client" % "google-oauth-client-jetty" % googleVersion,

  "org.slf4j" % "slf4j-simple" % "1.7.25" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.25" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"

)

mainClass in Compile := Some("servicetools.ServiceLauncher")





