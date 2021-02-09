package zio.pulsar.client

case class ClientSettings(serviceUrls: List[String], properties: Map[String, AnyRef]) {
  def toMap: Map[String, AnyRef] =
    Map("serviceUrl" -> serviceUrls.mkString(",")) ++ properties

  def withProperty(key: String, value: AnyRef): ClientSettings =
    copy(properties = properties + (key -> value))

  def withProperties(kvs: (String, AnyRef)*): ClientSettings =
    withProperties(kvs.toMap)

  def withProperties(kvs: Map[String, AnyRef]): ClientSettings =
    copy(properties = properties ++ kvs)
}

object ClientSettings {
  def apply(serviceUrls: List[String]): ClientSettings =
    new ClientSettings(serviceUrls, Map.empty[String, AnyRef])
}
