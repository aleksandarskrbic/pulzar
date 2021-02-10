package zio.pulsar.consumer

case class ConsumerSettings(topics: String, properties: Map[String, AnyRef]) {

  def withProperty(key: String, value: AnyRef): ConsumerSettings =
    copy(properties = properties + (key -> value))

  def withProperties(kvs: (String, AnyRef)*): ConsumerSettings =
    withProperties(kvs.toMap)

  def withProperties(kvs: Map[String, AnyRef]): ConsumerSettings =
    copy(properties = properties ++ kvs)
}

object ConsumerSettings {
  def apply(topic: String): ConsumerSettings =
    new ConsumerSettings(topic, Map.empty[String, AnyRef])
}
