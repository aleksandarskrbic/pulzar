package zio.pulsar.producer

case class ProducerSettings(topic: String, properties: Map[String, AnyRef]) {

  def toMap: Map[String, AnyRef] =
    Map("topicName" -> topic) ++ properties

  def withProperty(key: String, value: AnyRef): ProducerSettings =
    copy(properties = properties + (key -> value))

  def withProperties(kvs: (String, AnyRef)*): ProducerSettings =
    withProperties(kvs.toMap)

  def withProperties(kvs: Map[String, AnyRef]): ProducerSettings =
    copy(properties = properties ++ kvs)
}

object ProducerSettings {
  def apply(topic: String): ProducerSettings =
    new ProducerSettings(topic, Map.empty[String, AnyRef])
}
