package zio.pulsar.consumer

case class ConsumerSettings(topics: List[String], properties: Map[String, AnyRef]) {

  def toMap: Map[String, AnyRef] =
    Map("topicNames" -> topics.mkString(",")) ++ properties

  def withProperty(key: String, value: AnyRef): ConsumerSettings =
    copy(properties = properties + (key -> value))

  def withProperties(kvs: (String, AnyRef)*): ConsumerSettings =
    withProperties(kvs.toMap)

  def withProperties(kvs: Map[String, AnyRef]): ConsumerSettings =
    copy(properties = properties ++ kvs)
}

object ConsumerSettings {
  def apply(topics: List[String]): ConsumerSettings =
    new ConsumerSettings(topics, Map.empty[String, AnyRef])
}
