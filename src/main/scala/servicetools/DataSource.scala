package servicetools

sealed trait DataSource
sealed trait SourceConfiguration

case class UnknownDataSource(message: String) extends DataSource

case object YoutubeSource extends DataSource
case class YoutubeSource(videoId: String) extends DataSource
case class YoutubeConfiguration(userName: String, dataStoreDirectory: String, clientSecretJson: String) extends SourceConfiguration
